package cldy

//nolint:errcheck
import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" // nolint:revive
	. "github.com/onsi/gomega"    // nolint:revive
)

func TestResourcesPackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "resources Package Suite")
}

// presignedUploadPath is the presigned location the fake Cloudability API hands back, and
// testUploadProbeSuffix is what testUpload appends to break it before probing for a 403.
const presignedUploadPath = "/presigned-upload"
const testUploadProbeSuffix = "testUpload"

// startFakeCloudability stands in for the Frontdoor and Cloudability upload endpoints, so specs
// that construct a real ApptioService neither dial frontdoor.apptio.com nor burn retry backoff.
func startFakeCloudability() *httptest.Server {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, apikeyloginEndpoint):
			w.Header().Set("Apptio-Opentoken", "suite-open-token")
			w.Header().Set("valid_till", strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10))
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, clustersUploadEndpoint):
			w.Header().Set(contentTypeHeader, "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{
					"location":  server.URL + presignedUploadPath,
					"requestId": "suite-request-id",
				},
			})
		case r.URL.Path == presignedUploadPath+testUploadProbeSuffix:
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	return server
}

var _ = BeforeSuite(func() {
	// retries in the suite must not sleep for real
	retryBackoff = func(int) time.Duration { return 0 }

	server := startFakeCloudability()
	DeferCleanup(server.Close)

	originalFrontdoor, originalCloudability := frontdoorBaseURL, cloudabilityBaseURL
	// "%.0s" swallows the region suffix so every region resolves to the local server
	frontdoorBaseURL = strings.ReplaceAll(server.URL, "%", "%%") + "%.0s"
	cloudabilityBaseURL = frontdoorBaseURL
	DeferCleanup(func() {
		frontdoorBaseURL, cloudabilityBaseURL = originalFrontdoor, originalCloudability
	})
})
