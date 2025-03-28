package cldy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const usFrontdoorURL = "https://frontdoor.apptio.com"
const euFrontdoorURL = "https://frontdoor-eu.apptio.com"
const auFrontdoorURL = "https://frontdoor-au.apptio.com"
const meFrontdoorURL = "https://frontdoor-me.apptio.com"

const usCloudabilityURL = "https://api.cloudability.com"
const euCloudabilityURL = "https://api-eu.cloudability.com"
const auCloudabilityURL = "https://api-au.cloudability.com"
const meCloudabilityURL = "https://api-me.cloudability.com"

type customerRegion int

const (
	nativeUS customerRegion = iota
	hybridEU
	hybridAU
	hybridME
	nativeEU
	nativeAU
	nativeME
)

// StorageService is a generic uploader, could be apptio, custom s3 or custom azure blob
type StorageService interface {
	Upload(payload UploadPayload) error
}

type UploadPayload struct {
	ClusterUID   string `json:"clusterUID"`
	FileName     string `json:"fileName"`
	AgentVersion string `json:"agentVersion"`
	UploadHash   string `json:"uploadHash"`
	FilePath     string `json:"-"`
}

type ApptioServiceImpl struct {
	keyAccess       string
	keySecret       string
	envID           string
	openToken       string
	frontdoorURL    string
	cloudabilityURL string
	validTil        time.Time
}

type cloudabilityClustersUploadResponse struct {
	Result cloudabilityClustersUploadInfo `json:"result"`
}

type cloudabilityClustersUploadInfo struct {
	Location  string `json:"location"`
	RequestID string `json:"requestId"`
}

func NewApptioSerivce(config ApptioConfig) StorageService {
	return &ApptioServiceImpl{
		keyAccess: config.keyAccess,
		keySecret: config.keySecret,
		envID:     config.envID,
		openToken: config.openToken,
	}
}

type ApptioConfig struct {
	keyAccess    string
	keySecret    string
	envID        string
	openToken    string
	customerType string
}

func (s *ApptioServiceImpl) Upload(payload UploadPayload) error {
	var presignedURL string
	var err error
	// gather opentoken from Frontdoor
	s.openToken, err = s.login()
	if err != nil {
		return err
	}
	// using token from Frontdoor get upload URL from Cloudability
	presignedURL, err = s.getUploadURL(payload)
	if err != nil {
		return err
	}
	// upload data using presigned url

	return s.sendData()
}

// login gathers the opentoken required to make requests to Cloudability by hitting Frontdoor's apikeylogin endpoint
// using the keyAccess and keySecret credentials provided by the customer config
func (s *ApptioServiceImpl) login() (openToken string, rErr error) {
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("%s/service/apikeylogin", s.frontdoorURL)
	body, err := json.Marshal(map[string]interface{}{"keyAccess": s.keyAccess, "keySecret": s.keySecret})
	if err != nil {
		return "",
			fmt.Errorf("error in creating http request token string parameter for frontdoor service: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("error in creating http request for frontdoor service: %w", err)
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("error connecting to frontdoor service: %w", err)
	}
	defer safeClose(resp.Body.Close, &rErr)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("frontdoor service login call failed with status "+
			"code: %d", resp.StatusCode)
	}

	openToken = resp.Header.Get("apptio-opentoken")
	if openToken == "" {
		return "", fmt.Errorf("empty open token returned by frontdoor service")
	}
	return openToken, nil
}

// getUploadURL request to Cloudability to gather the presigned s3 URL that allows the agent to
// upload to Apptio's S3 bucket
func (s *ApptioServiceImpl) getUploadURL(payload UploadPayload) (uploadURL string, rErr error) {
	client := &http.Client{Timeout: 10 * time.Second}
	url := fmt.Sprintf("%s/v3/containers/clusters/upload", s.cloudabilityURL)
	body, err := json.Marshal(map[string]interface{}{
		"clusterUID":   payload.ClusterUID,
		"fileName":     payload.FileName,
		"agentVersion": payload.AgentVersion,
		"uploadHash":   payload.UploadHash,
	})
	if err != nil {
		return "",
			fmt.Errorf("error in marshaling http request parameters to cloudability: %w", err)
	}

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("error in creating http request to cloudability: %w", err)
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("error connecting to cloudability: %w", err)
	}
	defer safeClose(resp.Body.Close, &rErr)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("cloudability clusters/upload request call failed with status "+
			"code: %d", resp.StatusCode)
	}

	var result cloudabilityClustersUploadResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "", fmt.Errorf("error decoding clusters/upload response %s", err.Error())
	}
	uploadURL = result.Result.Location
	if uploadURL == "" {
		return "", fmt.Errorf("empty uploadURL returned by cloudability")
	}
	return uploadURL, nil
}

func (s *ApptioServiceImpl) sendData() error {

}
