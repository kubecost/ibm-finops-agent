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

// presignedUploadPath is the path the fake Cloudability API hands back as the presigned
// upload location, and testUploadProbeSuffix is what testUpload appends to deliberately
// break it before probing for the expected 403.
const presignedUploadPath = "/presigned-upload"
const testUploadProbeSuffix = "testUpload"

// startFakeCloudability stands up a local stand-in for the Frontdoor and Cloudability upload
// endpoints. Specs that construct a real ApptioService would otherwise resolve and dial
// frontdoor.apptio.com; in a sandboxed or offline environment every one of those requests
// fails and burns the full exponential retry backoff (2s + 4s per request), which is both slow
// and a dependency a unit test should not have.
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
			// the connectivity probe expects a 403 back from the intentionally broken URL
			w.WriteHeader(http.StatusForbidden)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	return server
}

var _ = BeforeSuite(func() {
	server := startFakeCloudability()
	DeferCleanup(server.Close)

	originalFrontdoor, originalCloudability := frontdoorBaseURL, cloudabilityBaseURL
	// "%.0s" swallows the region suffix so that every region resolves to the local server
	// rather than producing an unroutable host such as "http://127.0.0.1:1234-eu".
	frontdoorBaseURL = strings.ReplaceAll(server.URL, "%", "%%") + "%.0s"
	cloudabilityBaseURL = frontdoorBaseURL
	DeferCleanup(func() {
		frontdoorBaseURL, cloudabilityBaseURL = originalFrontdoor, originalCloudability
	})
})
