package cldy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
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

	for i := 1; i < 4; i++ {
		resp, err := s.CldyUploadClient.(ApptioClient).client.Do(request)
		if err == nil && resp != nil && resp.StatusCode == http.StatusForbidden {
			return nil
		}
		if err != nil {
			log.Warnf("Cloudability metrics-collector test HTTPS request failed with error: %s", err.Error())
		}
		if resp != nil {
			log.Warnf("Cloudability metrics-collector test upload %d failed with status code: %s", i, resp.Status)
		}
		time.Sleep(time.Duration(math.Pow(float64(2), float64(i))))
	}

	return fmt.Errorf("metrics-collector test upload exceeded max amount of failures")
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

// allowedUploadHostSuffixes are the domains a presigned upload URL may point at.
//
// The upload URL is not chosen by the agent: it is read from the "location"
// field of a Cloudability API response and then used as the destination for a
// PUT containing the cluster's entire resource inventory. Without validation, a
// tampered or malicious response could redirect that payload to any host
// (CWE-918). Presigned URLs are S3, so they resolve under amazonaws.com; the
// Apptio and Cloudability domains are permitted because both fronting services
// may return a URL on their own domain.
//
// Matching is on a dot boundary so that a lookalike host such as
// "evil-amazonaws.com" does not satisfy an "amazonaws.com" suffix.
var allowedUploadHostSuffixes = []string{
	"amazonaws.com",
	"cloudability.com",
	"apptio.com",
}

// validateUploadURL rejects an upload destination that is not HTTPS or does not
// sit under one of allowedUploadHostSuffixes.
func validateUploadURL(rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("upload URL could not be parsed: %w", err)
	}

	if parsed.Scheme != "https" {
		return fmt.Errorf("refusing to upload to a non-HTTPS destination (scheme %q)", parsed.Scheme)
	}

	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("upload URL has no host")
	}

	for _, suffix := range allowedUploadHostSuffixes {
		if host == suffix || strings.HasSuffix(host, "."+suffix) {
			return nil
		}
	}

	return fmt.Errorf("refusing to upload to unexpected host %q; expected one of %v",
		host, allowedUploadHostSuffixes)
}

func uploadPayloadToPresignedURL(client ClientService, payload UploadPayload, uploadURL string) error {
	// The destination comes from an API response rather than from our own
	// configuration, so check it before sending the cluster inventory to it.
	if err := validateUploadURL(uploadURL); err != nil {
		return fmt.Errorf("invalid presigned upload URL: %w", err)
	}

	fileToUpload, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("error in opening file to upload: %w", err)
	}

	fi, err := fileToUpload.Stat()
	if err != nil {
		return err
	}

	request, err := http.NewRequest(http.MethodPut, uploadURL, fileToUpload)
	if err != nil {
		return err
	}

	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, payload.UploadHash)
	request.ContentLength = fi.Size()

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
