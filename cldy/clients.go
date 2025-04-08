package cldy

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/opencost/opencost/core/pkg/log"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
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

const contentTypeHeader = "Content-Type"
const contentMD5 = "Content-MD5"
const defaultTimeout = time.Second * 10
const defaultRetries = 3
const proxyAuthHeader = "Proxy-Authorization"

const frontDoorLoginDescription = "performing login request to FrontDoor using KeyAccess and KeySecret"
const presignedURLDescription = "acquiring presigned URL from Cloudability with acquired Open-token"
const s3UploadDescription = "uploading sample to Cloudability S3 using presigned URL"

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

type ClientService interface {
	Do(r *http.Request, requestDescription string) (*http.Response, error)
}

func (ac ApptioClient) Do(r *http.Request, requestDescription string) (*http.Response, error) {
	return ac.doWithRetry(r, requestDescription)
}

type UploadPayload struct {
	ClusterUID   string `json:"clusterUID"`
	FileName     string `json:"fileName"`
	AgentVersion string `json:"agentVersion"`
	UploadHash   string `json:"uploadHash"`
	FilePath     string `json:"-"`
}

type ApptioServiceImpl struct {
	KeyAccess        string
	KeySecret        string
	EnvID            string
	OpenToken        string
	FrontdoorURL     string
	CloudabilityURL  string
	validTil         time.Time
	CldyUploadClient ClientService
}

type CloudabilityClustersUploadResponse struct {
	Result CloudabilityClustersUploadInfo `json:"result"`
}

type CloudabilityClustersUploadInfo struct {
	Location  string `json:"location"`
	RequestID string `json:"requestId"`
}

func NewApptioSerivce(config ApptioConfig) StorageService {
	return &ApptioServiceImpl{
		KeyAccess:        config.KeyAccess,
		KeySecret:        config.KeySecret,
		EnvID:            config.EnvID,
		OpenToken:        config.OpenToken,
		CldyUploadClient: NewApptioClient(config),
		FrontdoorURL:     config.FrontdoorURL,
		CloudabilityURL:  config.CloudabilityURL,
	}
}

// ApptioClient is the client used in the cloudability uploader
type ApptioClient struct {
	client     *http.Client
	maxRetries int
}

// NewApptioClient creates a client with support for various customer configurations
func NewApptioClient(config ApptioConfig) ApptioClient {
	if config.Timeout <= 0 {
		config.Timeout = defaultTimeout
	}
	if config.Retries <= 0 {
		config.Retries = defaultRetries
	}
	netTransport := &http.Transport{
		TLSHandshakeTimeout: config.Timeout,
	}

	// configure outbound proxy
	if len(config.ProxyURL.Host) > 0 {
		ConnectHeader := http.Header{}

		if config.ProxyAuth != "" {
			basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(config.ProxyAuth))
			ConnectHeader.Add(proxyAuthHeader, basicAuth)
		}

		netTransport = &http.Transport{
			Proxy:               http.ProxyURL(&config.ProxyURL),
			ProxyConnectHeader:  ConnectHeader,
			TLSHandshakeTimeout: config.Timeout,
			TLSClientConfig: &tls.Config{
				//nolint gas
				InsecureSkipVerify: config.ProxyInsecure,
			},
		}
	}

	httpClient := http.Client{
		Timeout:   config.Timeout,
		Transport: netTransport,
	}
	return ApptioClient{
		client:     &httpClient,
		maxRetries: 3,
	}
}

type ApptioConfig struct {
	KeyAccess       string
	KeySecret       string
	EnvID           string
	OpenToken       string
	CustomerType    string
	Timeout         time.Duration
	Retries         int
	ProxyURL        url.URL
	ProxyAuth       string
	ProxyInsecure   bool
	FrontdoorURL    string
	CloudabilityURL string
}

func (s *ApptioServiceImpl) Upload(payload UploadPayload) error {
	var presignedURL string
	var err error
	// gather opentoken from Frontdoor on first run or if token expired
	if s.OpenToken == "" || time.Now().UTC().After(s.validTil) {
		s.OpenToken, err = s.login()
		if err != nil {
			return err
		}
	}
	// using token from Frontdoor get upload URL from Cloudability
	presignedURL, err = s.getUploadURL(payload)
	if err != nil {
		return err
	}
	// upload data using presigned url
	return s.sendData(payload, presignedURL)
}

// login gathers the opentoken required to make requests to Cloudability by hitting Frontdoor's apikeylogin endpoint
// using the KeyAccess and KeySecret credentials provided by the customer config
func (s *ApptioServiceImpl) login() (openToken string, rErr error) {
	url := fmt.Sprintf("%s/service/apikeylogin", s.FrontdoorURL)
	body, err := json.Marshal(map[string]string{"KeyAccess": s.KeyAccess, "KeySecret": s.KeySecret})
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

	resp, err := s.CldyUploadClient.Do(request, frontDoorLoginDescription)
	if err != nil {
		return "", fmt.Errorf("error connecting to frontdoor service: %w", err)
	}
	defer safeClose(resp.Body.Close, &rErr)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("frontdoor service login call failed with status "+
			"code: %d", resp.StatusCode)
	}

	openToken = resp.Header.Get("Apptio-Opentoken")
	if openToken == "" {
		return "", fmt.Errorf("empty open token returned by frontdoor service")
	}
	validTill, err := strconv.ParseInt(resp.Header.Get("valid_till"), 10, 64)
	if err != nil {
		return "", fmt.Errorf("error in parsing valid_till returned by frontdoor service: %w", err)
	}
	// add some buffer to prevent a failure during upload window
	s.validTil = time.UnixMilli(validTill).Add(-10 * time.Minute)
	return openToken, nil
}

// getUploadURL request to Cloudability to gather the presigned s3 URL that allows the agent to
// upload to Apptio's S3 bucket
func (s *ApptioServiceImpl) getUploadURL(payload UploadPayload) (uploadURL string, rErr error) {
	url := fmt.Sprintf("%s/v3/internal/containers/clusters/upload", s.CloudabilityURL)
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
	request.Header.Add("apptio-opentoken", s.OpenToken)
	request.Header.Add("apptio-environmentid", s.EnvID)

	resp, err := s.CldyUploadClient.Do(request, presignedURLDescription)
	if err != nil {
		return "", fmt.Errorf("error connecting to cloudability: %w", err)
	}
	defer safeClose(resp.Body.Close, &rErr)

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("cloudability clusters/upload request call failed with status "+
			"code: %d", resp.StatusCode)
	}

	var result CloudabilityClustersUploadResponse
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

func (s *ApptioServiceImpl) sendData(payload UploadPayload, uploadURL string) (rErr error) {
	fileToUpload, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("error in opening file to upload: %w", err)
	}
	defer safeClose(fileToUpload.Close, &rErr)

	fi, err := fileToUpload.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()

	request, err := http.NewRequest(http.MethodPut, uploadURL, fileToUpload)
	if err != nil {
		return err
	}

	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, payload.UploadHash)
	request.ContentLength = size

	resp, err := s.CldyUploadClient.Do(request, s3UploadDescription)
	if err != nil || resp == nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		log.Infof("successfully uploaded metric sample %s to cloudability", payload.FileName)
	}
	return nil
}

func (ac ApptioClient) doWithRetry(req *http.Request, requestDescription string) (*http.Response, error) {
	for i := 1; i < 4; i++ {
		log.Infof("Attempt %d: %s", i, requestDescription)
		resp, err := ac.client.Do(req)
		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		if err != nil {
			log.Errorf("HTTPS request failed with error %s", err.Error())
		}
		if resp != nil {
			log.Errorf("Request failed with status code %s", resp.Status)
		}
		time.Sleep(time.Duration(math.Pow(float64(2), float64(i))))
	}
	return nil, fmt.Errorf("failed to complete request after maximum retries")
}
