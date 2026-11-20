package cldy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client Proxy", func() {
	Context("Clusters Upload", func() {
		proxyExample := "https://proxy.example.com"
		proxyURL, err := url.Parse(proxyExample)
		Expect(err).ToNot(HaveOccurred())

		requestURL := "https://api.cloudability.com/v3/internal/containers/clusters/upload"

		It("should not be used when Proxy URL is not set", func() {
			config := ApptioConfig{
				ProxyURL: &url.URL{},
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
		It("should be used when Proxy URL is set", func() {
			config := ApptioConfig{
				ProxyURL: proxyURL,
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
	})
	Context("Frontdoor login", func() {
		proxyExample := "https://proxy.example.com"
		proxyURL, err := url.Parse(proxyExample)
		Expect(err).ToNot(HaveOccurred())

		requestURL := "https://frontdoor.apptio.com/service/apikeylogin"

		It("should not be used when Proxy URL is not set", func() {
			config := ApptioConfig{
				ProxyURL: &url.URL{},
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
		It("should be used when Proxy URL is set", func() {
			config := ApptioConfig{
				ProxyURL: proxyURL,
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
		It("should be used when Proxy URL is set and region is EU", func() {
			config := ApptioConfig{
				ProxyURL: proxyURL,
			}
			requestURLEU := "https://frontdoor-eu.apptio.com/service/apikeylogin"

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURLEU, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
	})
	Context("Metrics Collector presign", func() {
		proxyExample := "https://proxy.example.com"
		proxyURL, err := url.Parse(proxyExample)
		Expect(err).ToNot(HaveOccurred())

		requestURL := "https://metrics-collector.cloudability.com/metricsample"

		It("should use proxy when UseProxyForGettingUploadURLOnly is true", func() {
			config := ApptioConfig{
				UseProxyForGettingUploadURLOnly: true,
				ProxyURL:                        proxyURL,
			}

			proxyFunc := BuildProxyFunc(config)
			request, err := http.NewRequest(http.MethodPost, requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			actualURL, err := proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())
			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
		})

		It("should not use proxy for S3 upload when UseProxyForGettingUploadURLOnly is true", func() {
			config := ApptioConfig{
				UseProxyForGettingUploadURLOnly: true,
				ProxyURL:                        proxyURL,
			}

			proxyFunc := BuildProxyFunc(config)
			request, err := http.NewRequest(http.MethodPut, "https://apptio-production.s3.amazonaws.com/sample", nil)
			Expect(err).ToNot(HaveOccurred())

			actualURL, err := proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())
			Expect(actualURL).To(BeNil())
		})
	})
	Context("All other endpoints", func() {
		proxyExample := "https://proxy.example.com"
		proxyURL, err := url.Parse(proxyExample)
		Expect(err).ToNot(HaveOccurred())

		requestURL := "https://this-could-be-any-url.com/test"

		It("should not be used when Proxy URL is not set", func() {
			config := ApptioConfig{
				ProxyURL: &url.URL{},
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
		It("should be used when Proxy URL is set", func() {
			config := ApptioConfig{
				ProxyURL: proxyURL,
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
			Expect(actualURL.Path).To(Equal(config.ProxyURL.Path))
		})
		It("should be nil when Proxy URL is set but UseProxyForGettingUploadURLOnly is true", func() {
			config := ApptioConfig{
				UseProxyForGettingUploadURLOnly: true,
				ProxyURL:                        proxyURL,
			}

			proxyFunc := BuildProxyFunc(config)
			var request *http.Request
			request, err := http.NewRequest("POST", requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			var actualURL *url.URL
			actualURL, err = proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())

			Expect(actualURL).To(BeNil())
		})
	})
})

var _ = Describe("ApptioService agent version sanitization", func() {
	DescribeTable("agentVersion sent to Frontdoor API",
		func(inputVersion, wantVersion string) {
			var capturedVersion string
			mock := &apptioMockClient{
				onDo: func(r *http.Request) (*http.Response, error) {
					switch {
					case strings.Contains(r.URL.Path, "apikeylogin"):
						// login: return a valid open token
						resp := &http.Response{
							StatusCode: http.StatusOK,
							Header:     http.Header{},
							Body:       io.NopCloser(bytes.NewReader(nil)),
						}
						resp.Header.Set("Apptio-Opentoken", "test-token")
						resp.Header.Set("valid_till", "9999999999999")
						return resp, nil
					case strings.Contains(r.URL.Path, "clusters/upload"):
						// getUploadURL: capture agentVersion from body
						var body map[string]any
						Expect(json.NewDecoder(r.Body).Decode(&body)).To(Succeed())
						capturedVersion = body["agentVersion"].(string)
						resp := map[string]any{
							"result": map[string]any{
								"location":  "https://s3.example.com/upload",
								"requestId": "req-123",
							},
						}
						b, _ := json.Marshal(resp)
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(b))}, nil
					default:
						// sendData (S3 PUT)
						return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
					}
				},
			}

			service := ApptioServiceImpl{
				SecretManager:    NewKeyValueSecretManager("access", "secret"),
				EnvID:            "env-123",
				FrontdoorURL:     "https://frontdoor.apptio.com",
				CloudabilityURL:  "https://api.cloudability.com",
				CldyUploadClient: mock,
			}

			payload := UploadPayload{
				ClusterUID:   "cluster-1",
				FileName:     "cluster-1_2025-01-01-00-00-00.tgz",
				AgentVersion: inputVersion,
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     "testdata/daemonsets.jsonl",
			}

			Expect(service.Upload(payload)).To(Succeed())
			Expect(capturedVersion).To(Equal(wantVersion))
		},
		Entry("strips leading v from a tagged release version", "v1.0.22", "1.0.22"),
		Entry("passes through a plain semver unchanged", "1.0.22", "1.0.22"),
		Entry("falls back to 0.0.0 for non-semver dev string", "dev", "0.0.0"),
	)
})

type apptioMockClient struct {
	onDo func(r *http.Request) (*http.Response, error)
}

func (m *apptioMockClient) Do(r *http.Request, _ string) (*http.Response, error) {
	return m.onDo(r)
}

// recordBackoff replaces retryBackoff for the spec and returns the attempt numbers it was
// asked to back off after.
func recordBackoff() *[]int {
	var attempts []int
	previous := retryBackoff
	retryBackoff = func(attempt int) time.Duration {
		attempts = append(attempts, attempt)
		return 0
	}
	DeferCleanup(func() { retryBackoff = previous })
	return &attempts
}

// stubRoundTripper serves canned statuses and records whether each response body was closed.
type stubRoundTripper struct {
	statuses []int
	calls    int
	bodies   []*recordingBody
}

func (rt *stubRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	status := rt.statuses[min(rt.calls, len(rt.statuses)-1)]
	rt.calls++
	body := &recordingBody{Reader: strings.NewReader("response body")}
	rt.bodies = append(rt.bodies, body)
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Body: body, Header: http.Header{}}, nil
}

type recordingBody struct {
	*strings.Reader
	closed bool
}

func (b *recordingBody) Close() error {
	b.closed = true
	return nil
}

func stubClient(statuses ...int) (ApptioClient, *stubRoundTripper) {
	transport := &stubRoundTripper{statuses: statuses}
	return ApptioClient{client: &http.Client{Transport: transport}, maxRetries: defaultRetries}, transport
}

var _ = Describe("retry backoff", func() {
	It("is seconds-scale and exponential", func() {
		Expect(exponentialBackoff(1)).To(Equal(2 * time.Second))
		Expect(exponentialBackoff(2)).To(Equal(4 * time.Second))
	})
})

var _ = Describe("uploadProbe", func() {
	probe := func(transport http.RoundTripper) uploadProbe {
		request, err := http.NewRequest(http.MethodPut, "https://s3.example.com/presignedtestUpload", nil)
		Expect(err).ToNot(HaveOccurred())
		return uploadProbe{
			client:           &http.Client{Transport: transport},
			request:          request,
			requestErrFormat: "request failed: %s",
			statusFormat:     "attempt %d failed: %s",
			exhaustedErr:     "probe exhausted",
		}
	}

	It("retries with backoff, closes every response and returns the exhausted error", func() {
		backoffs := recordBackoff()
		transport := &stubRoundTripper{statuses: []int{http.StatusInternalServerError}}

		Expect(probe(transport).run()).To(MatchError("probe exhausted"))

		Expect(transport.calls).To(Equal(testUploadAttempts))
		Expect(*backoffs).To(Equal([]int{1, 2}), "no backoff after the final attempt")
		for _, body := range transport.bodies {
			Expect(body.closed).To(BeTrue())
		}
	})

	It("succeeds on the first 403 without backing off", func() {
		backoffs := recordBackoff()
		transport := &stubRoundTripper{statuses: []int{http.StatusForbidden}}

		Expect(probe(transport).run()).To(Succeed())

		Expect(transport.calls).To(Equal(1))
		Expect(*backoffs).To(BeEmpty())
	})
})

var _ = Describe("ApptioClient doWithRetry", func() {
	newRequest := func(body io.Reader) *http.Request {
		request, err := http.NewRequest(http.MethodPost, "https://api.cloudability.com"+clustersUploadEndpoint, body)
		Expect(err).ToNot(HaveOccurred())
		return request
	}

	It("retries with backoff, closes every failed response and returns the terminal error", func() {
		backoffs := recordBackoff()
		client, transport := stubClient(http.StatusInternalServerError)

		resp, err := client.Do(newRequest(nil), "failing request")

		Expect(err).To(MatchError("failed to complete request after maximum retries"))
		Expect(resp).To(BeNil())
		Expect(transport.calls).To(Equal(defaultRetries))
		Expect(*backoffs).To(Equal([]int{1, 2}), "no backoff after the final attempt")
		for _, body := range transport.bodies {
			Expect(body.closed).To(BeTrue())
		}
	})

	It("stops retrying on success and leaves that body open for the caller", func() {
		backoffs := recordBackoff()
		client, transport := stubClient(http.StatusInternalServerError, http.StatusOK)

		resp, err := client.Do(newRequest(nil), "eventually successful request")

		Expect(err).ToNot(HaveOccurred())
		Expect(transport.calls).To(Equal(2))
		Expect(*backoffs).To(Equal([]int{1}))
		Expect(transport.bodies[0].closed).To(BeTrue())
		Expect(transport.bodies[1].closed).To(BeFalse())
		Expect(io.ReadAll(resp.Body)).To(Equal([]byte("response body")))
	})

	It("re-sends the whole file on every retry of the presigned PUT", func() {
		recordBackoff()
		filePath := filepath.Join(GinkgoT().TempDir(), "sample.tgz")
		Expect(os.WriteFile(filePath, bytes.Repeat([]byte("a"), 4096), 0o600)).To(Succeed())

		// recorded server side: a retry that re-sends a consumed body still looks well formed
		// on the client
		var mu sync.Mutex
		var received []int
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n, _ := io.Copy(io.Discard, r.Body)
			mu.Lock()
			received = append(received, int(n))
			mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
		}))
		DeferCleanup(server.Close)

		client := ApptioClient{client: server.Client(), maxRetries: defaultRetries}
		payload := UploadPayload{FilePath: filePath, UploadHash: "hash"}
		err := uploadPayloadToPresignedURL(client, payload, server.URL+"/presigned")

		Expect(err).To(MatchError("failed to complete request after maximum retries"))
		mu.Lock()
		defer mu.Unlock()
		// without GetBody this was [4096 0]: the transport closed the file after attempt 1
		Expect(received).To(Equal([]int{4096, 4096, 4096}))
	})

	It("attempts a request whose body cannot be replayed exactly once", func() {
		backoffs := recordBackoff()
		client, transport := stubClient(http.StatusInternalServerError)
		// an opaque ReadCloser: http.NewRequest cannot derive GetBody for it
		request := newRequest(io.NopCloser(strings.NewReader("unreplayable body")))
		Expect(request.GetBody).To(BeNil())

		_, err := client.Do(request, "unreplayable request")

		Expect(err).To(HaveOccurred())
		Expect(transport.calls).To(Equal(1))
		Expect(*backoffs).To(BeEmpty())
	})

	It("does not follow a redirect away from the upload host", func() {
		var hits atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			hits.Add(1)
		}))
		DeferCleanup(target.Close)
		upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+"/exfil", http.StatusTemporaryRedirect)
		}))
		DeferCleanup(upload.Close)

		// http.NewRequest sets GetBody for a strings.Reader, which is what lets net/http replay
		// a body through a 307
		request, err := http.NewRequest(http.MethodPut, upload.URL+"/presigned", strings.NewReader("sample"))
		Expect(err).ToNot(HaveOccurred())

		resp, err := NewApptioClient(ApptioConfig{}).client.Do(request)
		Expect(err).ToNot(HaveOccurred())
		drainAndClose(resp.Body)

		// the 3xx is handed back, so the upload fails closed instead of being replayed elsewhere
		Expect(resp.StatusCode).To(Equal(http.StatusTemporaryRedirect))
		Expect(hits.Load()).To(BeZero())
	})
})
