package cldy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"regexp"
	"strings"

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

	// An unknown region falls back to the US; the uploader raises region_fallback for it (D3).
	endpoints, _ := resolveRegion(config.Region)
	if endpoints.metricsCollector == "" {
		return nil, fmt.Errorf("CLOUDABILITY_UPLOAD_REGION %q is not served by the metrics-collector (API key) upload path", config.Region)
	}

	service := &MetricsCollectorServiceImpl{
		APIKey:           apiKey,
		BaseURL:          endpoints.metricsCollector,
		UserAgent:        fmt.Sprintf("cldy-client/%s", version.Version),
		CldyUploadClient: NewApptioClient(config),
	}

	log.Infof("Testing Cloudability metrics-collector upload connection.")
	ctx, cancel := context.WithTimeout(context.Background(), uploadDeadline(config, 0))
	defer cancel()
	if err := service.testUpload(ctx); err != nil {
		// Advisory: the service is still returned, and every upload cycle retries it.
		return service, fmt.Errorf("cloudability metrics-collector %w: %v", errConnectivityTest, err)
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

func (s *MetricsCollectorServiceImpl) Upload(ctx context.Context, payload UploadPayload) error {
	return putWithPresign(payload, func() (string, error) {
		return s.getUploadURL(ctx, payload)
	}, func(presignedURL string) error {
		return uploadPayloadToPresignedURL(ctx, s.CldyUploadClient, payload, presignedURL)
	})
}

func (s *MetricsCollectorServiceImpl) getUploadURL(ctx context.Context, payload UploadPayload) (uploadURL string, rErr error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.BaseURL, nil)
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
	defer safeClose(func() error { return drainAndClose(resp.Body) }, &rErr)

	if resp.StatusCode != http.StatusOK {
		return "", statusErrorf(resp.StatusCode, "metrics-collector presign request failed with status code: %d", resp.StatusCode)
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

func (s *MetricsCollectorServiceImpl) testUpload(ctx context.Context) error {
	testUpload := UploadPayload{
		ClusterUID:   "9f89af4e-5353-41a9-a7ca-42dce367006f",
		FileName:     "9f89af4e-5353-41a9-a7ca-42dce367006f_2006-01-02-15-04-05.tgz",
		FilePath:     "9f89af4e-5353-41a9-a7ca-42dce367006f_2006-01-02-15-04-05.tgz",
		AgentVersion: version.Version,
		UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
	}

	presignedURL, err := s.getUploadURL(ctx, testUpload)
	if err != nil {
		return err
	}
	return probePresignedURL(ctx, s.CldyUploadClient, http.MethodPut, presignedURL, testUpload.UploadHash)
}

// MetricsCollectorURLForRegion is the metrics-collector URL for region: "" if no
// metrics-collector serves it, and the US one if the region is unknown (D3).
func MetricsCollectorURLForRegion(region string) string {
	e, _ := resolveRegion(region)
	return e.metricsCollector
}

func uploadPayloadToPresignedURL(ctx context.Context, client ClientService, payload UploadPayload, uploadURL string) error {
	fileToUpload, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("error in opening file to upload: %w", err)
	}

	fi, err := fileToUpload.Stat()
	if err != nil {
		_ = fileToUpload.Close()
		return err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, fileToUpload)
	if err != nil {
		_ = fileToUpload.Close()
		return err
	}
	// The transport closes the body after each attempt; reopen the file for retries.
	request.GetBody = func() (io.ReadCloser, error) {
		return os.Open(payload.FilePath)
	}

	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, payload.UploadHash)
	request.ContentLength = fi.Size()

	resp, err := client.Do(request, s3UploadDescription)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := drainAndClose(resp.Body); closeErr != nil {
			log.Warnf("error closing upload response body: %v", closeErr)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return statusErrorf(resp.StatusCode, "sample upload failed with status code: %d", resp.StatusCode)
		}
		return statusErrorf(resp.StatusCode, "sample upload failed with status code: %d and response: %s", resp.StatusCode, body)
	}

	log.Infof("Successfully uploaded metric sample %s to cloudability", removeQueryParameters(path.Base(uploadURL)))
	return nil
}
