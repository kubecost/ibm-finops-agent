package cldy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/ibm/finops-agent/pkg/version"
	"github.com/opencost/opencost/core/pkg/log"
)

const metricsSampleEndpoint = "/metricsample"

const metricsCollectorDefaultBaseURL = "https://metrics-collector.cloudability.com/metricsample"
const metricsCollectorEUBaseURL = "https://metrics-collector-eu.cloudability.com/metricsample"
const metricsCollectorAUBaseURL = "https://metrics-collector-au.cloudability.com/metricsample"
const metricsCollectorMEBaseURL = "https://metrics-collector-me.cloudability.com/metricsample"
const metricsCollectorINBaseURL = "https://metrics-collector-in.cloudability.com/metricsample"
const metricsCollectorJPBaseURL = "https://metrics-collector-jp.cloudability.com/metricsample"
const metricsCollectorSGBaseURL = "https://metrics-collector-sg.cloudability.com/metricsample"
const metricsCollectorCABaseURL = "https://metrics-collector-ca.cloudability.com/metricsample"
const metricsCollectorGovBaseURL = "https://metrics-collector-production-gov.cloudability.com/metricsample"
const metricsCollectorStagingBaseURL = "https://metrics-collector-staging.cloudability.com/metricsample"

const metricsCollectorAuthHeader = "token"
const metricsCollectorAPIKeyHeader = "x-api-key"
const metricsCollectorClusterUIDHeader = "x-cluster-uid"
const metricsCollectorAgentVersionHeader = "x-agent-version"
const metricsCollectorUserAgentHeader = "User-Agent"
const metricsCollectorUploadFileHashHeader = "x-upload-file"

const metricsCollectorPresignDescription = "acquiring presigned URL from metrics-collector using API key"

// semverRegexp matches a plain semver like "2.11.17" (no leading v).
var semverRegexp = regexp.MustCompile(`^\d+\.\d+\.\d+`)

// MetricsCollectorServiceImpl uploads samples via the legacy metrics-collector API Gateway endpoint.
type MetricsCollectorServiceImpl struct {
	APIKey           string
	BaseURL          string
	UserAgent        string
	CldyUploadClient ClientService
	// sleepFunc allows tests to observe retry backoff without incurring real delays.
	// When nil, time.Sleep is used.
	sleepFunc func(d time.Duration)
}

type metricsCollectorUploadResponse struct {
	Location string `json:"location"`
}

// NewMetricsCollectorService configures the API-key upload path used by legacy metrics-agent customers.
func NewMetricsCollectorService(config ApptioConfig) (StorageService, error) {
	apiKey, err := readAPIKey(config.APIKeySecretManager)
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, fmt.Errorf("cloudability api key must be set to upload via metrics-collector")
	}

	service := &MetricsCollectorServiceImpl{
		APIKey:           apiKey,
		BaseURL:          getMetricsCollectorURLByRegion(config.Region),
		UserAgent:        fmt.Sprintf("cldy-client/%s", version.Version),
		CldyUploadClient: NewApptioClient(config),
	}

	log.Infof("Testing Cloudability metrics-collector upload connection.")
	if err := service.testUpload(); err != nil {
		return nil, fmt.Errorf("cloudability metrics-collector test connection failed: %s", err)
	}
	log.Infof("Cloudability metrics-collector upload test succeeded.")
	return service, nil
}

func readAPIKey(secretManager SecretManager) (string, error) {
	if secretManager == nil {
		return "", nil
	}
	body, err := secretManager.GetSecret()
	if err != nil {
		return "", err
	}
	defer func() {
		for i := range body {
			body[i] = 0
		}
	}()
	return strings.TrimSpace(string(body)), nil
}

func hasAPIKeyConfigured(secretManager SecretManager) bool {
	apiKey, err := readAPIKey(secretManager)
	return err == nil && apiKey != ""
}

func (s *MetricsCollectorServiceImpl) Upload(payload UploadPayload) error {
	presignedURL, err := s.getUploadURL(payload)
	if err != nil {
		return err
	}
	return uploadPayloadToPresignedURL(s.CldyUploadClient, payload, presignedURL)
}

func (s *MetricsCollectorServiceImpl) getUploadURL(payload UploadPayload) (string, error) {
	request, err := http.NewRequest(http.MethodPost, s.BaseURL, nil)
	if err != nil {
		return "", fmt.Errorf("error creating metrics-collector upload request: %w", err)
	}

	// Lambda function requires "1.0.9" as minimum version, but current versioning of test images does not
	// follow this rule. Should probably update the lambda function in the future once the metrics-agent
	// is compeltely deprecated to avoid this case.
	agentVersion := strings.TrimPrefix(payload.AgentVersion, "v")
	if !semverRegexp.MatchString(agentVersion) {
		agentVersion = "1.0.9"
	}

	request.Header.Set(contentTypeHeader, "application/json")
	request.Header.Set(metricsCollectorAuthHeader, s.APIKey)
	request.Header.Set(metricsCollectorAPIKeyHeader, s.APIKey)
	request.Header.Set(metricsCollectorUserAgentHeader, s.UserAgent)
	request.Header.Set(metricsCollectorAgentVersionHeader, agentVersion)
	request.Header.Set(metricsCollectorClusterUIDHeader, payload.ClusterUID)
	request.Header.Set(metricsCollectorUploadFileHashHeader, payload.UploadHash)

	resp, err := s.CldyUploadClient.Do(request, metricsCollectorPresignDescription)
	if err != nil {
		return "", fmt.Errorf("error connecting to metrics-collector: %w. Please ensure agent "+
			"is configured to have access to external resources", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warnf("error closing metrics-collector response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics-collector presign request failed with status code: %d", resp.StatusCode)
	}

	var result metricsCollectorUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("error decoding metrics-collector response: %w", err)
	}
	if result.Location == "" {
		return "", fmt.Errorf("empty upload URL returned by metrics-collector")
	}
	return result.Location, nil
}

func (s *MetricsCollectorServiceImpl) testUpload() error {
	testUpload := UploadPayload{
		ClusterUID:   "9f89af4e-5353-41a9-a7ca-42dce367006f",
		FileName:     "9f89af4e-5353-41a9-a7ca-42dce367006f_2006-01-02-15-04-05.tgz",
		FilePath:     "9f89af4e-5353-41a9-a7ca-42dce367006f_2006-01-02-15-04-05.tgz",
		AgentVersion: version.Version,
		UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
	}

	presignedURL, err := s.getUploadURL(testUpload)
	if err != nil {
		return err
	}

	presignedURL += "testUpload"
	request, err := http.NewRequest(http.MethodPut, presignedURL, new(bytes.Buffer))
	if err != nil {
		return err
	}
	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, testUpload.UploadHash)

	// the shared probe owns the retry loop, the seconds-scale backoff and closing every response
	return uploadProbe{
		client:           s.CldyUploadClient.(ApptioClient).client,
		request:          request,
		sleepFunc:        s.sleepFunc,
		requestErrFormat: "Cloudability metrics-collector test HTTPS request failed with error: %s",
		statusFormat:     "Cloudability metrics-collector test upload %d failed with status code: %s",
		exhaustedErr:     "metrics-collector test upload exceeded max amount of failures",
	}.run()
}

// MetricsCollectorURLForRegion exposes region mapping for tests and documentation consumers.
func MetricsCollectorURLForRegion(region string) string {
	return getMetricsCollectorURLByRegion(region)
}

func getMetricsCollectorURLByRegion(region string) string {
	switch region {
	case "eu", "eu-central-1":
		return metricsCollectorEUBaseURL
	case "au", "ap-southeast-2":
		return metricsCollectorAUBaseURL
	case "me", "me-central-1":
		return metricsCollectorMEBaseURL
	case "us", "us-west-2":
		return metricsCollectorDefaultBaseURL
	case "jp", "ap-northeast-1":
		return metricsCollectorJPBaseURL
	case "in", "ap-south-1":
		return metricsCollectorINBaseURL
	case "sg", "ap-southeast-1":
		return metricsCollectorSGBaseURL
	case "ca", "ca-central-1":
		return metricsCollectorCABaseURL
	case "gov", "us-gov-west-1":
		return metricsCollectorGovBaseURL
	case "staging", "us-west-2-staging":
		return metricsCollectorStagingBaseURL
	default:
		log.Warnf("Region %s is not supported for metrics-collector uploads. Defaulting to us-west-2.", region)
		return metricsCollectorDefaultBaseURL
	}
}

func uploadPayloadToPresignedURL(client ClientService, payload UploadPayload, uploadURL string) error {
	fileToUpload, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("error in opening file to upload: %w", err)
	}

	// The HTTP transport closes whatever body it is handed, so ownership of the descriptor moves
	// to the request the moment client.Do is called. Until then this function owns it and has to
	// close it on every early return, otherwise the descriptor leaks once per failed upload.
	fileOwnedHere := true
	defer func() {
		if fileOwnedHere {
			if closeErr := fileToUpload.Close(); closeErr != nil {
				log.Warnf("error closing file to upload: %v", closeErr)
			}
		}
	}()

	fi, err := fileToUpload.Stat()
	if err != nil {
		return fmt.Errorf("error in reading size of file to upload: %w", err)
	}

	request, err := http.NewRequest(http.MethodPut, uploadURL, fileToUpload)
	if err != nil {
		return err
	}

	// Make the body replayable. doWithRetry re-issues this same *http.Request, and the transport
	// consumes and closes the *os.File on the first attempt; without GetBody every later attempt
	// would put a zero-byte body on the wire against a live presigned S3 URL. GetBody must hand
	// back a genuinely fresh descriptor (not a re-seek of the closed one) for that reason, so it
	// reopens the same path by name.
	//
	// The invariant is therefore "every attempt sends the file that is on disk at that moment",
	// NOT "ContentLength always matches the body": ContentLength is measured once, below, from the
	// descriptor opened above and is never recomputed. A file that changed size between attempts
	// would be sent against the original declared length - the transport rejects a body longer
	// than ContentLength outright, and a shorter one is sent as an under-length request. That is
	// acceptable because a sample tar is written once and then only read or deleted, never
	// rewritten in place, and because the mismatch fails closed rather than silently: S3 does not
	// store an object whose declared length is unmet, and the Content-MD5 set below would not
	// match the altered bytes either. Recomputing the size per attempt is therefore not worth the
	// extra stat-per-retry machinery.
	request.GetBody = func() (io.ReadCloser, error) {
		return os.Open(payload.FilePath)
	}

	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, payload.UploadHash)
	// measured once, from the descriptor opened above; see the GetBody note about retries
	request.ContentLength = fi.Size()

	// From here on the request (and therefore the transport) owns the descriptor: closing it here
	// as well would be a double close.
	fileOwnedHere = false
	resp, err := client.Do(request, s3UploadDescription)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warnf("error closing upload response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("sample upload failed with status code: %d", resp.StatusCode)
		}
		return fmt.Errorf("sample upload failed with status code: %d and response: %s", resp.StatusCode, body)
	}

	log.Infof("Successfully uploaded metric sample %s to cloudability", removeQueryParameters(path.Base(uploadURL)))
	return nil
}
