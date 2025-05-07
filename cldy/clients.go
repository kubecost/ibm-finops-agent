package cldy

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/opencost/opencost/core/pkg/log"
)

const usFrontdoorURL = "https://frontdoor.apptio.com"
const usFrontdoorStgURL = "https://frontdoor-stage.apptio.com"
const euFrontdoorURL = "https://frontdoor-eu.apptio.com"
const auFrontdoorURL = "https://frontdoor-au.apptio.com"
const meFrontdoorURL = "https://frontdoor-me.apptio.com"

const usCloudabilityURL = "https://api.cloudability.com"
const usCloudabilityStgURL = "https://api-s.cloudability.com"
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

// StorageService contains the upload paths for cloudability s3, custom s3, and custom azure blob
type StorageService interface {
	Upload(payload UploadPayload) error
}

type ClientService interface {
	Do(r *http.Request, requestDescription string) (*http.Response, error)
	CustomS3Session(s3Region string) (*s3manager.Uploader, error)
	CustomS3Upload(s3Bucket string, key string, fileReader *os.File, uploader *s3manager.Uploader) error
}

func (ac ApptioClient) Do(r *http.Request, requestDescription string) (*http.Response, error) {
	return ac.doWithRetry(r, requestDescription)
}

func (ac ApptioClient) CustomS3Session(s3Region string) (*s3manager.Uploader, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:     aws.String(s3Region),
		MaxRetries: aws.Int(3)},
	)
	if err != nil {
		return nil, fmt.Errorf("Could not establish AWS Session, " +
			"ensure AWS environment variables are set correctly: %s", err)
	}
	svc := s3.New(sess)
	return s3manager.NewUploaderWithClient(svc), nil
}

func (ac ApptioClient) CustomS3Upload(s3Bucket string, key string, fileReader *os.File, uploader *s3manager.Uploader) error {
	sampleToUpload := &s3manager.UploadInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(key),
		Body:   fileReader,
	}

	_, err := uploader.Upload(sampleToUpload)
	if err != nil {
		return fmt.Errorf("failed to put Object to custom S3 with error: %s", err)
	}
	return nil
}

type UploadPayload struct {
	ClusterUID   string `json:"clusterUID"`
	FileName     string `json:"fileName"`
	AgentVersion string `json:"agentVersion"`
	UploadHash   string `json:"uploadHash"`
	FilePath     string `json:"-"`
}

type ApptioServiceImpl struct {
	SecretManager    SecretManager
	EnvID            string
	OpenToken        string
	FrontdoorURL     string
	CloudabilityURL  string
	validTil         time.Time
	CldyUploadClient ClientService
	CustomS3Bucket   string
	CustomS3Region   string
	S3Uploader       *s3manager.Uploader
}

type CloudabilityClustersUploadResponse struct {
	Result CloudabilityClustersUploadInfo `json:"result"`
}

type CloudabilityClustersUploadInfo struct {
	Location  string `json:"location"`
	RequestID string `json:"requestId"`
}

func NewApptioSerivce(config ApptioConfig) StorageService {
	frontdoorURL, cloudabilityURL := getURLsFromRegion(config.Region)

	return &ApptioServiceImpl{
		SecretManager:    config.SecretManager,
		EnvID:            config.EnvID,
		OpenToken:        config.OpenToken,
		CldyUploadClient: NewApptioClient(config),
		FrontdoorURL:     frontdoorURL,
		CloudabilityURL:  cloudabilityURL,
		CustomS3Bucket:   config.CustomS3UploadBucket,
		CustomS3Region:   config.CustomS3UploadRegion,
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
	if config.ProxyURL != nil {
		ConnectHeader := http.Header{}

		if config.ProxyAuth != "" {
			basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(config.ProxyAuth))
			ConnectHeader.Add(proxyAuthHeader, basicAuth)
		}

		netTransport = &http.Transport{
			Proxy:               http.ProxyURL(config.ProxyURL),
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
	SecretManager        SecretManager
	EnvID                string
	OpenToken            string
	CustomerType         string
	Timeout              time.Duration
	Retries              int
	ProxyURL             *url.URL
	ProxyAuth            string
	ProxyInsecure        bool
	Region               string
	CustomS3UploadBucket string
	CustomS3UploadRegion string
}

func (s *ApptioServiceImpl) Upload(payload UploadPayload) error {
	// Cloudability upload path
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
	err = s.sendData(payload, presignedURL)
	if err != nil {
		return err
	}

	// Custom S3 upload path
	if s.CustomS3Bucket != "" || s.CustomS3Region != "" {
		if s.CustomS3Bucket != "" && s.CustomS3Region != "" {
			err = s.UploadToCustomS3(payload)
			if err != nil {
				return err
			}
		} else {
			log.Warnf("both custom bucket and custom region must be set for custom s3 configuration. skipping s3 upload.")
		}
	}
	return nil
}

func (s *ApptioServiceImpl) UploadToCustomS3(payload UploadPayload) error {
	var err error

	// Create s3 session on first custom s3 upload
	if s.S3Uploader == nil {
		s.S3Uploader, err = s.CldyUploadClient.CustomS3Session(s.CustomS3Region)
		if err != nil {
			return err
		}
	}

	fileReader, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("Unable to open metric sample file %v", err)
	}
	defer fileReader.Close()

	key, err := generateSampleKey(payload.FileName, payload.ClusterUID)
	if err != nil {
		return err
	} 

	err = s.CldyUploadClient.CustomS3Upload(s.CustomS3Bucket, key, fileReader, s.S3Uploader)
	if err != nil {
		return err
	} 

	log.Infof("successfully uploaded metric sample %s to custom S3 bucket: %s",
		payload.FileName, s.CustomS3Bucket)
	return nil
}

// login gathers the opentoken required to make requests to Cloudability by hitting Frontdoor's apikeylogin endpoint
// using the KeyAccess and KeySecret credentials provided by the customer config
func (s *ApptioServiceImpl) login() (openToken string, rErr error) {
	url := fmt.Sprintf("%s/service/apikeylogin", s.FrontdoorURL)
	body, err := s.SecretManager.GetSecret()
	// remove secret from memory
	defer func() {
		for i := range body {
			body[i] = 0
		}
	}()
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

// Converts region to urls in (FrontdoorURL, CloudabilityURL) format.
// All hybrid regions return that region's FrontdoorURL and the US CloudabilitiyURL.
func getURLsFromRegion(region string) (string, string) {
	switch region {
	case "us", "us-west-2": // us-west-2 is for old agent migrations
		return usFrontdoorURL, usCloudabilityURL
	case "staging": // staging account
		return usFrontdoorStgURL, usCloudabilityStgURL
	case "eu", "eu-central-1": // eu-central-1 is for old agent migrations
		return euFrontdoorURL, euCloudabilityURL
	case "au", "ap-southeast-2":
		return auFrontdoorURL, auCloudabilityURL
	case "me", "me-central-1": // me-central-1 is for old agent migrations
		return meFrontdoorURL, meCloudabilityURL
	case "hybrid-eu":
		return euFrontdoorURL, usCloudabilityURL
	case "hybrid-au":
		return auFrontdoorURL, usCloudabilityURL
	case "hybrid-me":
		return meFrontdoorURL, usCloudabilityURL
	default:
		log.Warnf("customer region is invalid. Defaulting to US region.")
		return usFrontdoorURL, usCloudabilityURL
	}
}

// generateSampleKey creates a key (location) for s3 to upload the sample to. Example of s3 location format
// /production/data/metrics-agent/<YYYY>/<MM>/<DD>/<CLUSTER_UID>/<CLUSTER_UID>-<YYYYMMDD>-<HH>-<MM>.tgz
func generateSampleKey(fileName string, clusterUID string) (string, error) {
	withoutID := strings.Split(fileName, "_")
	if len(withoutID) < 2 {
		return "", fmt.Errorf("error parsing name from sample filename")
	}

	segments := strings.Split(withoutID[1], "-")
    numSegments := len(segments)

	// Filename should be comprised of at least 6 segments
	if numSegments < 5 {
		return "", fmt.Errorf("error parsing timestamp from sample filename")
	}
    minute := segments[numSegments - 2]
    hour := segments[numSegments - 3]
	day := segments[numSegments - 4]
	month := segments[numSegments - 5]
	year := segments[numSegments - 6]

	return fmt.Sprintf("/production/data/metrics-agent/%s/%s/%s/%s/%s-%s%s%s-%s-%s.tgz", year,
		month, day, clusterUID, clusterUID, year, month, day, hour, minute), nil
}

// SecretManager is an abstraction that allows for an api key to not be held in memory
type SecretManager interface {
	GetSecret() ([]byte, error)
}

// keyValueSecretManager is a simple implementation of SecretManager which likely triggers CWE-244
type keyValueSecretManager struct {
	keyAccess string
	keySecret string
}

func NewKeyValueSecretManager(keyAccess string, keySecret string) SecretManager {
	return &keyValueSecretManager{
		keyAccess: keyAccess,
		keySecret: keySecret,
	}
}

func (s *keyValueSecretManager) GetSecret() ([]byte, error) {
	return json.Marshal(map[string]string{"keyAccess": s.keyAccess, "keySecret": s.keySecret})
}
