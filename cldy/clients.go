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
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/aws/aws-sdk-go/aws"                  //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/aws/aws-sdk-go/aws/session"          //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/aws/aws-sdk-go/service/s3"           //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/aws/aws-sdk-go/service/s3/s3manager" //nolint:staticcheck // AWS SDK v1 deprecation - will be addressed separately
	"github.com/opencost/opencost/core/pkg/log"
)

// frontdoorBaseURL and cloudabilityBaseURL are the upload endpoint templates, formatted with
// the region suffix by formatFrontdoorAndCloudabilityURLs. They are vars rather than consts so
// that the test suite can redirect every upload path at a local server: a unit test must not
// depend on outbound network access, and an unreachable host now costs seconds of retry backoff.
var frontdoorBaseURL = "https://frontdoor%s.apptio.com"
var cloudabilityBaseURL = "https://api%s.cloudability.com"

const contentTypeHeader = "Content-Type"
const contentMD5 = "Content-MD5"
const defaultTimeout = time.Second * 10
const defaultRetries = 3

// testUploadAttempts is the number of times testUpload probes the presigned URL before giving up
const testUploadAttempts = 3
const proxyAuthHeader = "Proxy-Authorization"

const frontDoorLoginDescription = "performing login request to FrontDoor using KeyAccess and KeySecret"
const presignedURLDescription = "acquiring presigned URL from Cloudability with acquired Open-token"
const s3UploadDescription = "uploading sample to Cloudability S3 using presigned URL"

const clustersUploadEndpoint = "/v3/internal/containers/clusters/upload"
const apikeyloginEndpoint = "/service/apikeylogin"

// StorageService is a generic uploader, could be apptio, custom s3 or custom azure blob
type StorageService interface {
	Upload(payload UploadPayload) error
}

// ClientService performs an HTTP request on behalf of one of the upload paths.
//
// Ownership contract, which every implementation - including test doubles - has to honour:
//   - The implementation owns r.Body and must close it exactly once, on success and on failure
//     alike. Callers hand over live descriptors: uploadPayloadToPresignedURL passes an *os.File
//     as the request body and deliberately stops closing it the moment Do is called, so an
//     implementation that does not close leaks a file descriptor per upload. net/http's Transport
//     already does this, so clients built on it get it for free; fakes must do it explicitly.
//     Implementations that retry must rebuild the body from r.GetBody rather than reuse the
//     consumed one, since each attempt closes what it was handed.
//   - The caller owns the returned response and closes resp.Body.
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
	// sleepFunc allows tests to observe retry backoff without incurring real delays.
	// When nil, time.Sleep is used.
	sleepFunc func(d time.Duration)
}

// sleepOrDefault waits for d using the injected sleepFunc when one is set, and time.Sleep
// otherwise. Tests inject a recorder so that retry backoff is asserted without real delays.
func sleepOrDefault(sleepFunc func(d time.Duration), d time.Duration) {
	if sleepFunc != nil {
		sleepFunc(d)
		return
	}
	time.Sleep(d)
}

// drainAndClose releases an HTTP response body so that the underlying connection is returned
// to the pool instead of leaking. There is no recovery action, so failures are logged only.
func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		log.Debugf("error draining response body: %s", err)
	}
	if err := body.Close(); err != nil {
		log.Debugf("error closing response body: %s", err)
	}
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

	frontdoorURL, cloudabilityURL := getURLsFromRegion(config.Region)

	apptioService := &ApptioServiceImpl{
		SecretManager:    config.SecretManager,
		EnvID:            config.EnvID,
		OpenToken:        config.OpenToken,
		CldyUploadClient: NewApptioClient(config),
		FrontdoorURL:     frontdoorURL,
		CloudabilityURL:  cloudabilityURL,
	}

	log.Infof("Testing Cloudability upload connection.")
	err = apptioService.testUpload()
	if err != nil {
		return nil, fmt.Errorf("cloudability test connection failed: %s", err)
	}
	log.Infof("Cloudability upload test succeeded.")
	return apptioService, nil
}

// ApptioClient is the client used in the cloudability uploader
type ApptioClient struct {
	client     *http.Client
	maxRetries int
	// sleepFunc allows tests to observe retry backoff without incurring real delays.
	// When nil, time.Sleep is used.
	sleepFunc func(d time.Duration)
}

// sleep waits for the given duration, using the injected sleepFunc when one is set
func (ac ApptioClient) sleep(d time.Duration) {
	sleepOrDefault(ac.sleepFunc, d)
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

	if config.ProxyURL == nil && config.UseProxyForGettingUploadURLOnly {
		log.Warnf("UseProxyForGettingUploadURLOnly is set, but ProxyUrl is not. Skipping proxy setup.")
	}

	// configure outbound proxy
	if config.ProxyURL != nil {
		ConnectHeader := http.Header{}

		if config.ProxyAuth != "" {
			basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(config.ProxyAuth))
			ConnectHeader.Add(proxyAuthHeader, basicAuth)
		}

		netTransport = &http.Transport{
			Proxy:               BuildProxyFunc(config),
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
		// Never follow a redirect. This client carries the API key to Frontdoor and PUTs the
		// whole sample tar to a presigned S3 URL, and since those requests set GetBody so the
		// retry loop can replay them, net/http would otherwise happily replay the body to
		// whatever host a 307 or 308 names - including over plain http, since the stdlib does
		// not block a scheme downgrade on redirect. A redirected upload that answered 200 would
		// also be recorded as a successful upload, and the local tar deleted. Handing the 3xx
		// back to the caller instead fails closed: it is not a 200, so the upload is retried
		// against the original host.
		CheckRedirect: func(req *http.Request, _ []*http.Request) error {
			if req.Body != nil {
				if err := req.Body.Close(); err != nil {
					log.Debugf("error closing the body of a refused redirect: %s", err)
				}
			}
			return http.ErrUseLastResponse
		},
	}
	return ApptioClient{
		client:     &httpClient,
		maxRetries: 3,
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

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
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
	defer safeClose(resp.Body.Close, &rErr)

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("error reading response body: %w", err)
		}

		return "", fmt.Errorf("frontdoor service login call failed with status code: %d and "+
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
func (s *ApptioServiceImpl) testUpload() error {
	var err error
	s.OpenToken, err = s.login()
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

	presignedURL, err := s.getUploadURL(testUpload)
	if err != nil {
		return err
	}

	// Break presigned URL
	presignedURL += "testUpload"

	request, err := http.NewRequest(http.MethodPost, presignedURL, new(bytes.Buffer))
	if err != nil {
		return err
	}
	request.Header.Set(contentTypeHeader, "multipart/form-data")
	request.Header.Set(contentMD5, testUpload.UploadHash)

	// Allow multiple attempts for test upload
	return uploadProbe{
		client:    s.CldyUploadClient.(ApptioClient).client,
		request:   request,
		sleepFunc: s.sleepFunc,
		requestErrFormat: "Cloudability test HTTPS request failed with error: %s. Please ensure " +
			"agent is configured to have access to external resources",
		statusFormat: "Cloudability test upload %d failed with status code: %s",
		exhaustedErr: "bucket upload exceeded max amount of failures",
	}.run()
}

// uploadProbe is the connectivity check both upload paths run at start-up: it sends the same
// request to a deliberately broken presigned URL a few times and treats the 403 that a live
// bucket returns for a malformed key as proof that the agent can reach the destination.
//
// It exists so that the Cloudability and metrics-collector paths cannot drift apart again - they
// were line-for-line copies, and a backoff fix applied to one of them left the other sleeping
// nanoseconds and leaking a connection per attempt. Only the operator-facing wording differs
// between the two, so that is all either call site supplies.
type uploadProbe struct {
	client  *http.Client
	request *http.Request
	// sleepFunc lets tests observe the backoff without incurring real delays; nil means time.Sleep
	sleepFunc func(d time.Duration)
	// requestErrFormat logs a transport-level failure and takes the error string
	requestErrFormat string
	// statusFormat logs an unexpected response and takes the attempt number and the status
	statusFormat string
	// exhaustedErr is returned once every attempt has failed
	exhaustedErr string
}

func (p uploadProbe) run() error {
	for i := 1; i <= testUploadAttempts; i++ {
		resp, err := p.client.Do(p.request)
		if err != nil {
			log.Warnf(p.requestErrFormat, err.Error())
		}
		if resp != nil {
			statusCode, status := resp.StatusCode, resp.Status
			// close the body on every attempt, otherwise connections leak across retries
			drainAndClose(resp.Body)
			// Should return 403 with improper url
			if err == nil && statusCode == http.StatusForbidden {
				return nil
			}
			log.Warnf(p.statusFormat, i, status)
		}
		// exponential backoff between attempts: 2s, 4s, ... a bare time.Duration is nanoseconds,
		// so the delay has to be scaled by time.Second. No point sleeping after the final attempt.
		if i < testUploadAttempts {
			sleepOrDefault(p.sleepFunc, time.Duration(1<<i)*time.Second)
		}
	}

	return errors.New(p.exhaustedErr)
}

// getUploadURL request to Cloudability to gather the presigned s3 URL that allows the agent to
// upload to Apptio's S3 bucket
func (s *ApptioServiceImpl) getUploadURL(payload UploadPayload) (uploadURL string, rErr error) {
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

	request, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
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

func (s *ApptioServiceImpl) sendData(payload UploadPayload, uploadURL string) error {
	return uploadPayloadToPresignedURL(s.CldyUploadClient, payload, uploadURL)
}

func (ac ApptioClient) doWithRetry(req *http.Request, requestDescription string) (*http.Response, error) {
	attempts := ac.maxRetries
	if attempts <= 0 {
		attempts = defaultRetries
	}

	// Retrying means handing the *same* *http.Request to the client again, and the transport has
	// already consumed and closed req.Body by then. A request whose body cannot be rebuilt must
	// therefore never be retried: attempt 2 would put an empty (or already-closed) body on the
	// wire, which for a presigned S3 PUT means silently storing a truncated object. Failing after
	// a single attempt is the safe choice - a caller that wants retries opts in by setting
	// req.GetBody (see login, getUploadURL and uploadPayloadToPresignedURL).
	replayable := req.Body == nil || req.Body == http.NoBody || req.GetBody != nil
	if !replayable && attempts > 1 {
		log.Warnf("%s: request body cannot be replayed because req.GetBody is nil, so this "+
			"request will be attempted once instead of %d times. Set req.GetBody on requests "+
			"that carry a body to make them retryable.", requestDescription, attempts)
		attempts = 1
	}

	for i := 1; i <= attempts; i++ {
		// Rebuild the body before every retry. GetBody has to return a fresh, unread reader:
		// the transport owns and closes whatever body it was handed on the previous attempt.
		if i > 1 && req.Body != nil && req.GetBody != nil {
			body, err := req.GetBody()
			if err != nil {
				return nil, fmt.Errorf("unable to rewind request body before attempt %d of %s: %w",
					i, requestDescription, err)
			}
			req.Body = body
		}
		log.Debugf("Attempt %d: %s", i, requestDescription)
		resp, err := ac.client.Do(req)
		// the caller owns a successful response and is responsible for closing its body
		if err == nil && resp != nil && resp.StatusCode == http.StatusOK {
			return resp, nil
		}
		if err != nil {
			log.Warnf("HTTPS request failed with error: %s", err.Error())
		}
		if resp != nil {
			log.Warnf("Request failed with status code: %s", resp.Status)
			// close the failed response, otherwise connections leak across retries
			drainAndClose(resp.Body)
		}
		// exponential backoff between attempts: 2s, 4s, ... a bare time.Duration is nanoseconds,
		// so the delay has to be scaled by time.Second. No point sleeping after the final attempt.
		if i < attempts {
			ac.sleep(time.Duration(1<<i) * time.Second)
		}
	}
	return nil, fmt.Errorf("failed to complete request after maximum retries")
}

// Note: All hybrid regions return that region's FrontdoorURL and the US CloudabilitiyURL.
// Regions are all hardcoded because there is no direct formula from availability zone -> region suffix,
// as well as this switch block acts as validation on the region field if changed by the user.
func getURLsFromRegion(region string) (string, string) {
	switch region {
	case "staging": // staging account. Note the difference for -stage and -s
		return formatFrontdoorAndCloudabilityURLs("-stage", "-s")
	case "us", "us-west-2":
		return formatFrontdoorAndCloudabilityURLs("", "")
	case "eu", "eu-central-1":
		return formatFrontdoorAndCloudabilityURLs("-eu", "-eu")
	case "au", "ap-southeast-2":
		return formatFrontdoorAndCloudabilityURLs("-au", "-au")
	case "me", "me-central-1":
		return formatFrontdoorAndCloudabilityURLs("-me", "-me")
	case "sg", "ap-southeast-1":
		return formatFrontdoorAndCloudabilityURLs("-sg", "-sg")
	case "jp", "ap-northeast-1":
		return formatFrontdoorAndCloudabilityURLs("-jp", "-jp")
	case "in", "ap-south-1":
		return formatFrontdoorAndCloudabilityURLs("-in", "-in")
	case "ca", "ca-central-1":
		return formatFrontdoorAndCloudabilityURLs("-ca", "-ca")
	case "gov", "us-gov-west-1":
		return formatFrontdoorAndCloudabilityURLs("-usgov", ".usgov")
	case "gov2", "us-gov-east-1":
		return formatFrontdoorAndCloudabilityURLs("-usgov2", ".usgov2")
	case "hybrid-eu":
		return formatFrontdoorAndCloudabilityURLs("-eu", "")
	case "hybrid-au":
		return formatFrontdoorAndCloudabilityURLs("-au", "")
	case "hybrid-me":
		return formatFrontdoorAndCloudabilityURLs("-me", "")
	case "hybrid-sg":
		return formatFrontdoorAndCloudabilityURLs("-sg", "")
	case "hybrid-jp":
		return formatFrontdoorAndCloudabilityURLs("-jp", "")
	case "hybrid-in":
		return formatFrontdoorAndCloudabilityURLs("-in", "")
	case "hybrid-ca":
		return formatFrontdoorAndCloudabilityURLs("-ca", "")
	default:
		log.Warnf("Invalid cloudability region: %s. Defaulting to 'us-west-2' region.", region)
		return formatFrontdoorAndCloudabilityURLs("", "")
	}
}

// Formats the region suffixes into the respective frontdoor and cloudability url
func formatFrontdoorAndCloudabilityURLs(frontdoorRegionSuffix string, cloudabilityRegionSuffix string) (string, string) {
	return fmt.Sprintf(frontdoorBaseURL, frontdoorRegionSuffix), fmt.Sprintf(cloudabilityBaseURL, cloudabilityRegionSuffix)
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
	Do(sampleToUpload *s3manager.UploadInput) error
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

func (cs3c CustomS3Client) Upload(payload UploadPayload) (err error) {
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

	err = cs3c.UploadClient.Do(sampleToUpload)
	if err != nil {
		return fmt.Errorf("failed to put sample to custom S3 with error: %w. Please ensure agent "+
			"is configured to have access to external resources", err)
	}

	log.Infof("Successfully uploaded metric sample %s to custom S3 bucket: %s", path.Base(key), cs3c.S3Bucket)
	return nil
}

func (cs3u CustomS3Uploader) Do(sampleToUpload *s3manager.UploadInput) error {
	_, err := cs3u.Uploader.Upload(sampleToUpload)
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
	Do(sampleToUpload *BlobUploadInput) error
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

func (cbc CustomBlobClient) Upload(payload UploadPayload) (err error) {
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

	err = cbc.UploadClient.Do(sampleToUpload)
	if err != nil {
		return fmt.Errorf("failed to put sample to custom azure blob with error: %s. Please ensure agent "+
			"is configured to have access to external resources", err)
	}

	log.Infof("Successfully uploaded metric sample %s to custom azure blob: %s", path.Base(key), cbc.BlobContainerName)
	return nil
}

func (cbu CustomBlobUploader) Do(sampleToUpload *BlobUploadInput) error {
	_, err := cbu.Uploader.UploadFile(context.TODO(), sampleToUpload.ContainerName, sampleToUpload.BlobName, sampleToUpload.Body, nil)
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
	if numSegments < 5 {
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
