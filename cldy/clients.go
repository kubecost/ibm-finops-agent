package cldy

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws"                  //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/aws/aws-sdk-go/aws/session"          //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/aws/aws-sdk-go/service/s3"           //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/aws/aws-sdk-go/service/s3/s3manager" //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/opencost/opencost/core/pkg/log"
)

const frontdoorBaseURL = "https://frontdoor%s.apptio.com"
const cloudabilityBaseURL = "https://api%s.cloudability.com"

const contentTypeHeader = "Content-Type"
const contentMD5 = "Content-MD5"
const defaultTimeout = time.Second * 10
const defaultRetries = 3
const proxyAuthHeader = "Proxy-Authorization"

// defaultMinThroughput is the slowest upload, in bytes per second, allowed to finish when
// ApptioConfig.MinThroughput is not set.
const defaultMinThroughput = 256 << 10

// maxUploadDeadline caps the deadline of any one request, and of any one upload.
const maxUploadDeadline = time.Hour

// maxRetryBackoff caps the wait between two attempts of a request.
const maxRetryBackoff = 30 * time.Second

const frontDoorLoginDescription = "performing login request to FrontDoor using KeyAccess and KeySecret"
const presignedURLDescription = "acquiring presigned URL from Cloudability with acquired Open-token"
const testUploadDescription = "testing the upload connection with a broken presigned URL"
const s3UploadDescription = "uploading sample to Cloudability S3 using presigned URL"

const clustersUploadEndpoint = "/v3/internal/containers/clusters/upload"
const apikeyloginEndpoint = "/service/apikeylogin"

// StorageService is a generic uploader, could be apptio, custom s3 or custom azure blob. Upload
// returns an *UploadError naming the stage that failed, so the uploader can tell a refused
// credential from a refused payload (classifyUpload).
type StorageService interface {
	Upload(ctx context.Context, payload UploadPayload) error
}

// Upload stages, for UploadError.
const (
	// UploadStageLogin is the Frontdoor login.
	UploadStageLogin = "login"
	// UploadStagePresign is the request for a presigned URL, to Cloudability or the
	// metrics-collector.
	UploadStagePresign = "presign"
	// UploadStagePresignedPut is the PUT of the payload to a presigned URL.
	UploadStagePresignedPut = "presigned_put"
	// UploadStageStore is an authenticated write to the customer's own S3 bucket or Azure
	// container.
	UploadStageStore = "store"
)

// UploadError is a failed upload and the stage it failed at. The HTTP status, when the server
// answered, is in the wrapped error (see uploadStatusCode).
type UploadError struct {
	Stage string
	Err   error
}

func (e *UploadError) Error() string { return fmt.Sprintf("upload failed at %s: %v", e.Stage, e.Err) }

func (e *UploadError) Unwrap() error { return e.Err }

// HTTPStatusError is a request the server answered with a status other than 200.
type HTTPStatusError struct {
	StatusCode int
	Message    string
}

func (e *HTTPStatusError) Error() string { return e.Message }

// statusErrorf returns an *HTTPStatusError for code with a formatted message.
func statusErrorf(code int, format string, args ...any) error {
	return &HTTPStatusError{StatusCode: code, Message: fmt.Sprintf(format, args...)}
}

// uploadStatusCode returns the HTTP status err carries, if the server answered: from an
// *HTTPStatusError, an aws-sdk-go request failure or an Azure response error.
func uploadStatusCode(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode, true
	}
	var awsErr interface{ StatusCode() int } // awserr.RequestFailure
	if errors.As(err, &awsErr) && awsErr.StatusCode() != 0 {
		return awsErr.StatusCode(), true
	}
	var azureErr *azcore.ResponseError
	if errors.As(err, &azureErr) && azureErr.StatusCode != 0 {
		return azureErr.StatusCode, true
	}
	return 0, false
}

// s3ErrorCode returns " (<Code>...</Code>)" from an S3 XML error body, or "". Only the code is
// kept: the rest of an S3 error can echo the request, credential scope included.
func s3ErrorCode(body []byte) string {
	_, rest, ok := strings.Cut(string(body), "<Code>")
	if !ok {
		return ""
	}
	code, _, ok := strings.Cut(rest, "</Code>")
	if !ok || len(code) > 64 {
		return ""
	}
	return " (<Code>" + code + "</Code>)"
}

// errConnectivityTest marks a failed startup connectivity test. The service is still usable:
// the test is advisory.
var errConnectivityTest = errors.New("connectivity test failed")

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
	SecretManager    SecretManager
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

func NewApptioService(config ApptioConfig) (StorageService, error) {
	body, err := config.SecretManager.GetSecret()
	if err != nil {
		return nil, err
	}
	// remove secret from memory
	defer func() {
		for i := range body {
			body[i] = 0
		}
	}()

	if len(body) == 0 || config.EnvID == "" {
		return nil, fmt.Errorf("key access, key secret, and env id must all be set to upload to cloudability")
	}

	// An unknown region falls back to the US; the uploader raises region_fallback for it (D3).
	endpoints, _ := resolveRegion(config.Region)
	if endpoints.frontdoor == "" || endpoints.cloudability == "" {
		return nil, fmt.Errorf("CLOUDABILITY_UPLOAD_REGION %q is not served by the Cloudability (Frontdoor) upload path", config.Region)
	}

	apptioService := &ApptioServiceImpl{
		SecretManager:    config.SecretManager,
		EnvID:            config.EnvID,
		OpenToken:        config.OpenToken,
		CldyUploadClient: NewApptioClient(config),
		FrontdoorURL:     endpoints.frontdoor,
		CloudabilityURL:  endpoints.cloudability,
	}

	log.Infof("Testing Cloudability upload connection.")
	ctx, cancel := context.WithTimeout(context.Background(), uploadDeadline(config, 0))
	defer cancel()
	err = apptioService.testUpload(ctx)
	if err != nil {
		// Advisory: the service is still returned, and every upload cycle retries it.
		return apptioService, fmt.Errorf("cloudability %w: %v", errConnectivityTest, err)
	}
	log.Infof("Cloudability upload test succeeded.")
	return apptioService, nil
}

// ApptioClient is the client used in the cloudability uploader
type ApptioClient struct {
	client *http.Client
	// attempts is how many times a request is sent before giving up (UPLOAD_RETRY_COUNT).
	attempts int
	// timeout and minThroughput size each attempt's deadline (requestTimeout).
	timeout       time.Duration
	minThroughput int64
}

// withDefaults fills in the timeout, attempts and minimum throughput when they are not set.
func (config ApptioConfig) withDefaults() ApptioConfig {
	if config.Timeout <= 0 {
		config.Timeout = defaultTimeout
	}
	if config.Retries <= 0 {
		config.Retries = defaultRetries
	}
	if config.MinThroughput <= 0 {
		config.MinThroughput = defaultMinThroughput
	}
	return config
}

// NewApptioClient creates a client with support for various customer configurations. It has no
// http.Client.Timeout: each attempt gets a deadline sized to what it sends (requestTimeout),
// inside the deadline of the request's context.
func NewApptioClient(config ApptioConfig) ApptioClient {
	config = config.withDefaults()
	return ApptioClient{
		client:        &http.Client{Transport: newTransport(config)},
		attempts:      config.Retries,
		timeout:       config.Timeout,
		minThroughput: config.MinThroughput,
	}
}

// newTransport clones http.DefaultTransport, which keeps the proxy from HTTPS_PROXY and NO_PROXY,
// the dial and idle timeouts and HTTP/2. CLOUDABILITY_OUTBOUND_PROXY, when set, takes precedence
// over the environment. Certificates are always verified, except the HTTPS proxy's own with
// CLOUDABILITY_OUTBOUND_PROXY_INSECURE.
func newTransport(config ApptioConfig) *http.Transport {
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment}
	if def, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = def.Clone()
	}
	transport.TLSHandshakeTimeout = config.Timeout

	if config.ProxyURL == nil {
		if config.UseProxyForGettingUploadURLOnly {
			log.Warnf("UseProxyForGettingUploadURLOnly is set, but ProxyUrl is not. Skipping proxy setup.")
		}
		if config.ProxyInsecure {
			log.Warnf("CLOUDABILITY_OUTBOUND_PROXY_INSECURE has no effect without CLOUDABILITY_OUTBOUND_PROXY; certificates are verified")
		}
		return transport
	}

	connectHeader := http.Header{}
	if config.ProxyAuth != "" {
		basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(config.ProxyAuth))
		connectHeader.Add(proxyAuthHeader, basicAuth)
	}
	transport.Proxy = BuildProxyFunc(config)
	transport.ProxyConnectHeader = connectHeader

	if config.ProxyInsecure {
		if strings.EqualFold(config.ProxyURL.Scheme, "https") {
			transport.DialTLSContext = dialTLSSkippingVerifyFor(proxyAddr(config.ProxyURL), transport, config.Timeout)
		} else {
			log.Warnf("CLOUDABILITY_OUTBOUND_PROXY_INSECURE has no effect: the proxy %s is not an https:// proxy, and the "+
				"certificates of the destinations behind it are always verified", config.ProxyURL.Host)
		}
	}
	return transport
}

// proxyAddr is the host:port the transport dials for the proxy u.
func proxyAddr(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = "443"
		if strings.EqualFold(u.Scheme, "http") {
			port = "80"
		}
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// dialTLSSkippingVerifyFor is a DialTLSContext that skips verifying the certificate of the
// proxy at proxyAddr and verifies every other. The transport uses DialTLSContext to reach an
// https:// proxy and any destination it connects to directly; the TLS to a destination through
// the proxy's tunnel is its own, with TLSClientConfig, so that is verified too.
func dialTLSSkippingVerifyFor(proxyAddr string, transport *http.Transport, handshakeTimeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		config := &tls.Config{}
		if transport.TLSClientConfig != nil {
			config = transport.TLSClientConfig.Clone()
		}
		if config.ServerName == "" {
			config.ServerName = host
		}
		if strings.EqualFold(addr, proxyAddr) {
			config.InsecureSkipVerify = true //nolint:gosec // only the configured proxy (CLOUDABILITY_OUTBOUND_PROXY_INSECURE)
			config.NextProtos = nil          // HTTP/1.1 to the proxy, for CONNECT
		}
		if handshakeTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, handshakeTimeout)
			defer cancel()
		}
		return (&tls.Dialer{NetDialer: dialer, Config: config}).DialContext(ctx, network, addr)
	}
}

type ApptioConfig struct {
	ClusterName                     string
	SecretManager                   SecretManager
	APIKeySecretManager             SecretManager
	EnvID                           string
	OpenToken                       string
	CustomerType                    string
	Timeout                         time.Duration
	Retries                         int
	ProxyURL                        *url.URL
	ProxyAuth                       string
	ProxyInsecure                   bool
	Region                          string
	CustomS3UploadBucket            string
	CustomS3UploadRegion            string
	CustomAzureBlobContainerName    string
	CustomAzureBlobUrl              string
	CustomAzureTenantID             string
	CustomAzureClientID             string
	CustomAzureClientSecret         SecretManager
	UseProxyForGettingUploadURLOnly bool
	// MinThroughput is the slowest upload, in bytes per second, that is allowed to finish
	// (CLOUDABILITY_UPLOAD_MIN_THROUGHPUT_KBPS). Zero means defaultMinThroughput.
	MinThroughput int64
}

func BuildProxyFunc(config ApptioConfig) func(*http.Request) (*url.URL, error) {
	if config.ProxyURL == nil {
		log.Warnf("cannot build proxy without a ProxyURL set. Skipping.")
		return nil
	}
	return func(request *http.Request) (*url.URL, error) {
		if config.UseProxyForGettingUploadURLOnly {
			// agent configured to only use proxy for presign and frontdoor login requests
			if request.URL.Path == clustersUploadEndpoint ||
				request.URL.Path == metricsSampleEndpoint ||
				strings.Contains(request.URL.Path, apikeyloginEndpoint) {
				return config.ProxyURL, nil
			}
			return nil, nil
		}

		// proxy enabled for all requests
		return config.ProxyURL, nil
	}
}

func (s *ApptioServiceImpl) Upload(ctx context.Context, payload UploadPayload) error {
	var err error
	// gather opentoken from Frontdoor on first run or if token expired
	if s.OpenToken == "" || time.Now().UTC().After(s.validTil) {
		s.OpenToken, err = s.login(ctx)
		if err != nil {
			return &UploadError{Stage: UploadStageLogin, Err: err}
		}
	}
	return putWithPresign(payload, func() (string, error) {
		// using token from Frontdoor get upload URL from Cloudability
		presignedURL, err := s.getUploadURL(ctx, payload)
		if code, ok := uploadStatusCode(err); ok && (code == http.StatusUnauthorized || code == http.StatusForbidden) {
			// The token was refused: log in again on the next attempt.
			s.OpenToken = ""
		}
		return presignedURL, err
	}, func(presignedURL string) error {
		return s.sendData(ctx, payload, presignedURL)
	})
}

// putWithPresign gets a presigned URL and PUTs the payload to it. A 403 on the PUT means the URL
// expired, so it presigns once more and retries once. Errors are *UploadError.
func putWithPresign(payload UploadPayload, presign func() (string, error), put func(url string) error) error {
	for attempt := 1; ; attempt++ {
		presignedURL, err := presign()
		if err != nil {
			return &UploadError{Stage: UploadStagePresign, Err: err}
		}
		err = put(presignedURL)
		if err == nil {
			return nil
		}
		if code, _ := uploadStatusCode(err); code != http.StatusForbidden || attempt > 1 {
			return &UploadError{Stage: UploadStagePresignedPut, Err: err}
		}
		log.Warnf("the presigned URL for %s was refused (403), probably expired; requesting a new one", payload.FileName)
	}
}

// login gathers the opentoken required to make requests to Cloudability by hitting Frontdoor's apikeylogin endpoint
// using the KeyAccess and KeySecret credentials provided by the customer config
func (s *ApptioServiceImpl) login(ctx context.Context) (openToken string, rErr error) {
	url := fmt.Sprintf("%s%s", s.FrontdoorURL, apikeyloginEndpoint)
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

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("error in creating http request for frontdoor service: %w", err)
	}
	// Make body reusable for retries
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := s.CldyUploadClient.Do(request, frontDoorLoginDescription)
	if err != nil {
		return "", fmt.Errorf("error connecting to frontdoor service: %w. Please ensure agent "+
			"is able to connect to %s", err, url)
	}
	defer safeClose(func() error { return drainAndClose(resp.Body) }, &rErr)

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("error reading response body: %w", err)
		}

		return "", statusErrorf(resp.StatusCode, "frontdoor service login call failed with status code: %d and "+
			"response: %s", resp.StatusCode, body)
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

// testUpload tries to fetch the uploadURL for the cloudabilty upload path
func (s *ApptioServiceImpl) testUpload(ctx context.Context) error {
	var err error
	s.OpenToken, err = s.login(ctx)
	if err != nil {
		return err
	}

	testUpload := UploadPayload{
		ClusterUID:   "9f89af4e-5353-41a9-a7ca-42dce367006f",
		FileName:     "9f89af4e-5353-41a9-a7ca-42dce367006f_2006-01-02-15-04-05.tgz",
		FilePath:     "9f89af4e-5353-41a9-a7ca-42dce367006f_2006-01-02-15-04-05.tgz",
		AgentVersion: "1.0.0",
		UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
	}

	presignedURL, err := s.getUploadURL(ctx, testUpload)
	if err != nil {
		return err
	}
	return probePresignedURL(ctx, s.CldyUploadClient, http.MethodPost, presignedURL, testUpload.UploadHash)
}

// probePresignedURL checks that the storage behind a presigned URL answers, without writing
// anything: an empty request to a deliberately broken copy of the URL must be refused with 403.
func probePresignedURL(ctx context.Context, client ClientService, method, presignedURL, hash string) error {
	request, err := http.NewRequestWithContext(ctx, method, presignedURL+"testUpload", http.NoBody)
	if err != nil {
		return fmt.Errorf("error creating the test upload request for %s", removeQueryParameters(presignedURL))
	}
	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, hash)

	resp, err := client.Do(request, testUploadDescription)
	if err == nil {
		_ = drainAndClose(resp.Body)
		return errors.New("the test upload to a broken presigned URL was accepted; want 403")
	}
	if code, ok := uploadStatusCode(err); ok && code == http.StatusForbidden {
		return nil
	}
	return fmt.Errorf("test upload failed: %w. Please ensure agent is configured to have access to external resources", err)
}

// getUploadURL request to Cloudability to gather the presigned s3 URL that allows the agent to
// upload to Apptio's S3 bucket
func (s *ApptioServiceImpl) getUploadURL(ctx context.Context, payload UploadPayload) (uploadURL string, rErr error) {
	url := fmt.Sprintf("%s%s", s.CloudabilityURL, clustersUploadEndpoint)

	// The Frontdoor API requires a plain semver without a leading "v".
	// Fall back to "0.0.0" for non-semver strings (e.g. "dev" in local builds).
	agentVersion := strings.TrimPrefix(payload.AgentVersion, "v")
	if !semverRegexp.MatchString(agentVersion) {
		agentVersion = "0.0.0"
	}

	body, err := json.Marshal(map[string]any{
		"clusterUID":   payload.ClusterUID,
		"fileName":     payload.FileName,
		"agentVersion": agentVersion,
		"uploadHash":   payload.UploadHash,
	})
	if err != nil {
		return "",
			fmt.Errorf("error in marshaling http request parameters to cloudability: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("error in creating http request to cloudability: %w", err)
	}
	// Make body reusable for retries
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}

	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")
	request.Header.Add("apptio-opentoken", s.OpenToken)
	request.Header.Add("apptio-environmentid", s.EnvID)

	resp, err := s.CldyUploadClient.Do(request, presignedURLDescription)
	if err != nil {
		return "", fmt.Errorf("error connecting to cloudability: %w. Please ensure agent is "+
			"configured to have access to external resources", err)
	}
	defer safeClose(func() error { return drainAndClose(resp.Body) }, &rErr)

	if resp.StatusCode != http.StatusOK {
		return "", statusErrorf(resp.StatusCode, "cloudability clusters/upload request call failed with status "+
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

func (s *ApptioServiceImpl) sendData(ctx context.Context, payload UploadPayload, uploadURL string) error {
	return uploadPayloadToPresignedURL(ctx, s.CldyUploadClient, payload, uploadURL)
}

// requestTimeout is the deadline for one attempt of a request that sends n bytes: timeout, plus
// the time n bytes take at minThroughput bytes per second, capped at maxUploadDeadline.
func requestTimeout(timeout time.Duration, minThroughput, n int64) time.Duration {
	d := timeout
	if n > 0 && minThroughput > 0 {
		d += time.Duration(float64(n) / float64(minThroughput) * float64(time.Second))
	}
	return min(d, maxUploadDeadline)
}

// uploadDeadline is the deadline for one Upload of a payload of size bytes: a request timeout
// each for the login and the presign, and one sized to the payload for the PUT, capped at
// maxUploadDeadline. Retries fit inside it only when attempts fail fast; the next upload cycle
// retries the rest.
func uploadDeadline(config ApptioConfig, size int64) time.Duration {
	config = config.withDefaults()
	return min(2*config.Timeout+requestTimeout(config.Timeout, config.MinThroughput, size), maxUploadDeadline)
}

// retryableStatus reports whether a request answered with code may succeed if sent again.
func retryableStatus(code int) bool {
	return code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= 500
}

// doWithRetry sends req until it gets a 200, at most ac.attempts times (UPLOAD_RETRY_COUNT). A
// transport error, 408, 429 or 5xx is retried, after a backoff; any other status is returned at
// once. Each attempt has its own deadline (requestTimeout), inside the deadline of req's
// context, which also cuts the backoff short. When every attempt fails the error wraps the last
// one: an *HTTPStatusError if the server answered, or the transport error.
func (ac ApptioClient) doWithRetry(req *http.Request, requestDescription string) (*http.Response, error) {
	ctx := req.Context()
	attempts := max(ac.attempts, 1)
	var lastErr error
	for i := 1; i <= attempts; i++ {
		// http.Client.Do always closes the request body, so every retry needs a fresh one.
		if i > 1 && req.Body != nil && req.Body != http.NoBody {
			if req.GetBody == nil {
				return nil, fmt.Errorf("cannot retry request with non-rewindable body: %s", requestDescription)
			}
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("failed to reset request body for retry: %w", err)
			}
			req.Body = body
		}
		log.Debugf("Attempt %d: %s", i, requestDescription)
		attemptCtx, cancel := context.WithTimeout(ctx, requestTimeout(ac.timeout, ac.minThroughput, req.ContentLength))
		resp, err := ac.client.Do(req.WithContext(attemptCtx))
		if err == nil && resp.StatusCode == http.StatusOK {
			// The attempt's deadline covers reading the body too; closing it releases the timer.
			resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
			return resp, nil
		}
		retry := true
		if err != nil {
			// A presigned URL's query string is a credential: keep it out of the error, which
			// is logged here and by the uploader.
			var urlErr *url.Error
			if errors.As(err, &urlErr) {
				urlErr.URL = removeQueryParameters(urlErr.URL)
			}
			log.Warnf("HTTPS request failed with error: %s", err.Error())
			lastErr = err
		} else {
			head, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
			_ = drainAndClose(resp.Body)
			lastErr = statusErrorf(resp.StatusCode, "status %s%s", resp.Status, s3ErrorCode(head))
			log.Warnf("Request failed with status code: %s", lastErr)
			retry = retryableStatus(resp.StatusCode)
		}
		cancel()
		if !retry {
			return nil, fmt.Errorf("request failed: %w", lastErr)
		}
		if i < attempts {
			if err := sleepContext(ctx, retryBackoff(i)); err != nil {
				return nil, fmt.Errorf("gave up after %d of %d attempts: %w; last error: %w", i, attempts, err, lastErr)
			}
		}
	}
	return nil, fmt.Errorf("failed to complete request after maximum retries: %w", lastErr)
}

// cancelOnClose cancels a request's context when its response body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// drainAndClose reads what is left of a response body, up to 64 KiB, and closes it, so that
// the connection can be reused.
func drainAndClose(body io.ReadCloser) error {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 64<<10))
	return body.Close()
}

// sleepContext waits for d, or until ctx is done, and then returns ctx's error.
func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// regionEndpoints are one upload region's endpoints on each upload path. "" means the path
// doesn't serve the region, which is a configuration error on that path.
type regionEndpoints struct {
	frontdoor        string
	cloudability     string
	metricsCollector string
}

// endpoints formats a region's Frontdoor and Cloudability URLs from their suffixes.
func endpoints(frontdoorSuffix, cloudabilitySuffix, metricsCollectorURL string) regionEndpoints {
	return regionEndpoints{
		frontdoor:        fmt.Sprintf(frontdoorBaseURL, frontdoorSuffix),
		cloudability:     fmt.Sprintf(cloudabilityBaseURL, cloudabilitySuffix),
		metricsCollector: metricsCollectorURL,
	}
}

// usEndpoints are the US endpoints, which an unknown region falls back to (D3).
var usEndpoints = endpoints("", "", metricsCollectorDefaultBaseURL)

// regions maps every CLOUDABILITY_UPLOAD_REGION the agent accepts, short and AWS names, to its
// endpoints. They are listed by hand because there is no formula from AWS region to suffix.
// Note the staging suffixes: -stage for Frontdoor, -s for Cloudability.
var regions = func() map[string]regionEndpoints {
	m := map[string]regionEndpoints{}
	add := func(e regionEndpoints, names ...string) {
		for _, name := range names {
			m[name] = e
		}
	}
	add(usEndpoints, "us", "us-west-2")
	add(endpoints("-stage", "-s", metricsCollectorStagingBaseURL), "staging", "us-west-2-staging")
	add(endpoints("-eu", "-eu", metricsCollectorEUBaseURL), "eu", "eu-central-1")
	add(endpoints("-au", "-au", metricsCollectorAUBaseURL), "au", "ap-southeast-2")
	add(endpoints("-me", "-me", metricsCollectorMEBaseURL), "me", "me-central-1")
	add(endpoints("-sg", "-sg", metricsCollectorSGBaseURL), "sg", "ap-southeast-1")
	add(endpoints("-jp", "-jp", metricsCollectorJPBaseURL), "jp", "ap-northeast-1")
	add(endpoints("-in", "-in", metricsCollectorINBaseURL), "in", "ap-south-1")
	add(endpoints("-ca", "-ca", metricsCollectorCABaseURL), "ca", "ca-central-1")
	add(endpoints("-usgov", ".usgov", metricsCollectorGovBaseURL), "gov", "us-gov-west-1")
	// No metrics-collector serves gov2.
	add(endpoints("-usgov2", ".usgov2", ""), "gov2", "us-gov-east-1")
	// Deliberate: a hybrid region logs in to its own Frontdoor and uploads to the US Cloudability,
	// on either path.
	add(endpoints("-eu", "", metricsCollectorDefaultBaseURL), "hybrid-eu")
	add(endpoints("-au", "", metricsCollectorDefaultBaseURL), "hybrid-au")
	add(endpoints("-me", "", metricsCollectorDefaultBaseURL), "hybrid-me")
	add(endpoints("-sg", "", metricsCollectorDefaultBaseURL), "hybrid-sg")
	add(endpoints("-jp", "", metricsCollectorDefaultBaseURL), "hybrid-jp")
	add(endpoints("-in", "", metricsCollectorDefaultBaseURL), "hybrid-in")
	add(endpoints("-ca", "", metricsCollectorDefaultBaseURL), "hybrid-ca")
	return m
}()

// resolveRegion returns the endpoints for region, ignoring case and surrounding space. An
// unknown region gets the US endpoints and fallback is true (D3).
func resolveRegion(region string) (e regionEndpoints, fallback bool) {
	e, ok := regions[strings.ToLower(strings.TrimSpace(region))]
	if !ok {
		return usEndpoints, true
	}
	return e, false
}

// regionFallback reports whether uploads go to the US because the region is unknown (D3). Only
// the Cloudability paths, metrics-collector and Frontdoor, use the region; they are selected as
// newStorageServices selects them.
func regionFallback(config ApptioConfig) bool {
	if !hasAPIKeyConfigured(config.APIKeySecretManager) && config.EnvID == "" {
		return false
	}
	_, fallback := resolveRegion(config.Region)
	return fallback
}

type CustomS3Client struct {
	S3Bucket     string
	S3Region     string
	UploadClient CustomS3UploadService
}

func NewCustomS3Client(customS3Bucket string, customS3Region string) (StorageService, error) {
	if customS3Bucket == "" || customS3Region == "" {
		return nil, fmt.Errorf("CLOUDABILITY_CUSTOM_S3_UPLOAD_BUCKET and CLOUDABILITY_CUSTOM_S3_UPLOAD_REGION " +
			"must be set for custom S3 configuration")
	}

	uploadClient, err := newUploadClient(customS3Region)
	if err != nil {
		return nil, err
	}

	return CustomS3Client{
		S3Bucket:     customS3Bucket,
		S3Region:     customS3Region,
		UploadClient: uploadClient,
	}, nil
}

type CustomS3UploadService interface {
	Do(ctx context.Context, sampleToUpload *s3manager.UploadInput) error
}

type CustomS3Uploader struct {
	Uploader *s3manager.Uploader
}

func newUploadClient(s3Region string) (*CustomS3Uploader, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:     new(s3Region),
		MaxRetries: new(3)},
	)
	if err != nil {
		return nil, fmt.Errorf("could not establish AWS Session, "+
			"ensure Cloudability AWS environment variables are set correctly: %s", err)
	}
	svc := s3.New(sess)

	return &CustomS3Uploader{
		Uploader: s3manager.NewUploaderWithClient(svc),
	}, nil
}

func (cs3c CustomS3Client) Upload(ctx context.Context, payload UploadPayload) (err error) {
	fileReader, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("unable to open metric sample file: %w", err)
	}
	defer safeClose(fileReader.Close, &err)

	key, err := generateSampleKey(payload.FileName, payload.ClusterUID)
	if err != nil {
		return err
	}

	sampleToUpload := &s3manager.UploadInput{
		Bucket: new(cs3c.S3Bucket),
		Key:    new(key),
		Body:   fileReader,
	}

	err = cs3c.UploadClient.Do(ctx, sampleToUpload)
	if err != nil {
		return &UploadError{Stage: UploadStageStore, Err: fmt.Errorf("failed to put sample to custom S3 with error: %w. Please ensure agent "+
			"is configured to have access to external resources", err)}
	}

	log.Infof("Successfully uploaded metric sample %s to custom S3 bucket: %s", path.Base(key), cs3c.S3Bucket)
	return nil
}

func (cs3u CustomS3Uploader) Do(ctx context.Context, sampleToUpload *s3manager.UploadInput) error {
	_, err := cs3u.Uploader.UploadWithContext(ctx, sampleToUpload)
	return err
}

type CustomBlobClient struct {
	BlobContainerName string
	UploadClient      CustomBlobUploadService
}

func NewCustomBlobClient(blobContainerName string, customBlobUrl string, azureTenantID string, azureClientID string,
	azureClientSecret SecretManager) (StorageService, error) {
	if blobContainerName == "" || customBlobUrl == "" {
		return nil, fmt.Errorf("CLOUDABILITY_CUSTOM_AZURE_BLOB_CONTAINER_NAME and CLOUDABILITY_CUSTOM_AZURE_BLOB_URL " +
			"must be set for all custom azure blob configurations")
	}

	body, err := azureClientSecret.GetSecret()
	if err != nil {
		return nil, err
	}
	// remove secret from memory
	defer func() {
		for i := range body {
			body[i] = 0
		}
	}()

	// Use managed identity if secondary env variables aren't set
	if azureTenantID == "" && azureClientID == "" && len(body) == 0 {
		uploadClient, err := newBlobManagedIdentityClient(customBlobUrl)
		if err != nil {
			return nil, fmt.Errorf("could not establish Azure client with managed identity, "+
				"ensure Azure environment variables are set correctly: %w", err)
		}
		if uploadClient != nil {
			return CustomBlobClient{
				BlobContainerName: blobContainerName,
				UploadClient:      uploadClient,
			}, nil
		}
	} else {
		if azureTenantID == "" || azureClientID == "" || len(body) == 0 {
			return nil, fmt.Errorf("CLOUDABILITY_CUSTOM_AZURE_BLOB_TENANT_ID, CLOUDABILITY_CUSTOM_AZURE_BLOB_CLIENT_ID, " +
				"and CLOUDABILITY_CUSTOM_AZURE_BLOB_CLIENT_SECRET_FILEPATH must be set for Azure client creation through environment")
		}

		uploadClient, err := newBlobServicePrincipalClient(customBlobUrl, azureTenantID, azureClientID, azureClientSecret)
		if err != nil {
			return nil, fmt.Errorf("could not establish Azure client through environment, "+
				"ensure all Azure environment variables are set correctly: %w", err)
		}
		if uploadClient != nil {
			return CustomBlobClient{
				BlobContainerName: blobContainerName,
				UploadClient:      uploadClient,
			}, nil
		}
	}

	return nil, fmt.Errorf("unspecified error generating azure client")
}

type CustomBlobUploadService interface {
	Do(ctx context.Context, sampleToUpload *BlobUploadInput) error
}

type CustomBlobUploader struct {
	Uploader *azblob.Client
}

func newBlobManagedIdentityClient(customBlobUrl string) (*CustomBlobUploader, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	azureClient, err := azblob.NewClient(customBlobUrl, cred, nil)
	if err != nil {
		return nil, err
	}

	return &CustomBlobUploader{
		Uploader: azureClient,
	}, nil
}

func newBlobServicePrincipalClient(customBlobUrl string, azureTentantID string, azureClientID string,
	azureClientSecret SecretManager) (*CustomBlobUploader, error) {
	body, err := azureClientSecret.GetSecret()
	if err != nil {
		return nil, err
	}
	// remove secret from memory
	defer func() {
		for i := range body {
			body[i] = 0
		}
	}()

	cred, err := azidentity.NewClientSecretCredential(azureTentantID, azureClientID, string(body),
		nil)
	if err != nil {
		return nil, err
	}

	retryConfig := azblob.ClientOptions{
		Retry: policy.RetryOptions{
			MaxRetries: 3,
		},
	}
	azureClient, err := azblob.NewClient(customBlobUrl, cred, &retryConfig)
	if err != nil {
		return nil, err
	}

	return &CustomBlobUploader{
		Uploader: azureClient,
	}, nil
}

type BlobUploadInput struct {
	ContainerName string
	BlobName      string
	Body          *os.File
}

func (cbc CustomBlobClient) Upload(ctx context.Context, payload UploadPayload) (err error) {
	fileReader, err := os.Open(payload.FilePath)
	if err != nil {
		return fmt.Errorf("unable to open metric sample file: %w", err)
	}
	defer safeClose(fileReader.Close, &err)

	key, err := generateSampleKey(payload.FileName, payload.ClusterUID)
	if err != nil {
		return err
	}

	sampleToUpload := &BlobUploadInput{
		ContainerName: cbc.BlobContainerName,
		BlobName:      key,
		Body:          fileReader,
	}

	err = cbc.UploadClient.Do(ctx, sampleToUpload)
	if err != nil {
		return &UploadError{Stage: UploadStageStore, Err: fmt.Errorf("failed to put sample to custom azure blob with error: %w. Please ensure agent "+
			"is configured to have access to external resources", err)}
	}

	log.Infof("Successfully uploaded metric sample %s to custom azure blob: %s", path.Base(key), cbc.BlobContainerName)
	return nil
}

func (cbu CustomBlobUploader) Do(ctx context.Context, sampleToUpload *BlobUploadInput) error {
	_, err := cbu.Uploader.UploadFile(ctx, sampleToUpload.ContainerName, sampleToUpload.BlobName, sampleToUpload.Body, nil)
	return err
}

// generateSampleKey creates a key (location) for s3 to upload the sample to. Example of s3 location format
// production/data/metrics-agent/<YYYY>/<MM>/<DD>/<CLUSTER_UID>/<CLUSTER_UID>-<YYYYMMDD>-<HH>-<MM>.tgz
func generateSampleKey(fileName string, clusterUID string) (string, error) {
	withoutID := strings.Split(fileName, "_")
	if len(withoutID) < 2 {
		return "", fmt.Errorf("error parsing name from sample filename")
	}

	segments := strings.Split(withoutID[1], "-")
	numSegments := len(segments)

	// Filename should be comprised of at least 6 segments
	if numSegments < 6 {
		return "", fmt.Errorf("error parsing timestamp from sample filename")
	}
	minute := segments[numSegments-2]
	hour := segments[numSegments-3]
	day := segments[numSegments-4]
	month := segments[numSegments-5]
	year := segments[numSegments-6]

	return fmt.Sprintf("production/data/metrics-agent/%s/%s/%s/%s/%s-%s%s%s-%s-%s.tgz", year,
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

type valueSecretManager struct {
	keySecret string
}

func NewValueSecretManager(keySecret string) SecretManager {
	return &valueSecretManager{
		keySecret: keySecret,
	}
}

func (s *valueSecretManager) GetSecret() ([]byte, error) {
	if s.keySecret == "" {
		return nil, fmt.Errorf("no secret value provided")
	}
	return []byte(s.keySecret), nil
}

// Trims query parameters
func removeQueryParameters(url string) string {
	return strings.Split(url, "?")[0]
}

// regionURLs returns the Frontdoor, Cloudability and metrics-collector URLs for region, "" where
// the path doesn't serve it, and whether region is unknown and fell back to the US.
func regionURLs(region string) (frontdoor, cloudability, metricsCollector string, fallback bool) {
	e, fallback := resolveRegion(region)
	return e.frontdoor, e.cloudability, e.metricsCollector, fallback
}
