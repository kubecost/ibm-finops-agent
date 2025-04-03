package cldy

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/opencost/opencost/core/pkg/log"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
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

const frontDoorLoginDescription = "performing login request to FrontDoor using keyAccess and keySecret"
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

type UploadPayload struct {
	ClusterUID   string `json:"clusterUID"`
	FileName     string `json:"fileName"`
	AgentVersion string `json:"agentVersion"`
	UploadHash   string `json:"uploadHash"`
	FilePath     string `json:"-"`
}

type ApptioServiceImpl struct {
	keyAccess        string
	keySecret        string
	envID            string
	openToken        string
	frontdoorURL     string
	cloudabilityURL  string
	validTil         time.Time
	cldyUploadClient ApptioClient
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
		keyAccess:        config.KeyAccess,
		keySecret:        config.KeySecret,
		envID:            config.EnvID,
		openToken:        config.OpenToken,
		cldyUploadClient: NewApptioClient(config),
		frontdoorURL:     config.FrontdoorURL,
		cloudabilityURL:  config.CloudabilityURL,
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
	if s.openToken == "" || time.Now().After(s.validTil) {
		s.openToken, err = s.login()
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
// using the keyAccess and keySecret credentials provided by the customer config
func (s *ApptioServiceImpl) login() (openToken string, rErr error) {
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

	resp, err := doWithRetry(s.cldyUploadClient.client, request, frontDoorLoginDescription)
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
	url := fmt.Sprintf("%s/v3/internal/containers/clusters/upload", s.cloudabilityURL)
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

	request, err := http.NewRequest(http.MethodPost, url, io.NopCloser(bytes.NewBuffer(body)))
	if err != nil {
		return "", fmt.Errorf("error in creating http request to cloudability: %w", err)
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	request.Header.Add("apptio-opentoken", s.openToken)
	request.Header.Add("apptio-environmentid", s.envID)

	resp, err := doWithRetry(s.cldyUploadClient.client, request, presignedURLDescription)
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

	request, err := http.NewRequest(http.MethodPut, uploadURL, io.NopCloser(fileToUpload))
	if err != nil {
		return err
	}

	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, payload.UploadHash)
	request.ContentLength = size

	resp, err := doWithRetry(s.cldyUploadClient.client, request, s3UploadDescription)
	if err != nil || resp == nil {
		return err
	}
	if resp.StatusCode == http.StatusOK {
		log.Infof("successfully uploaded metric sample %s to cloudability", payload.FileName)
	}
	return nil
}

func doWithRetry(client *http.Client, req *http.Request, requestDescription string) (*http.Response, error) {

	for i := 1; i < 4; i++ {
		log.Infof("Attempt %d: %s", i, requestDescription)
		resp, err := client.Do(req)
		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		if err != nil {
			log.Errorf("HTTPS request failed with error %s", err.Error())
		}
		if resp != nil {
			log.Errorf("Request failed with status code %s", resp.Status)
		}
		// retry with backoff
		time.Sleep(time.Duration(math.Pow(float64(2), float64(i))))
	}
	return nil, fmt.Errorf("failed to complete request after maximum retries")
}
