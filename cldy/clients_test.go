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

// testUploadFixture stands up a local server that plays the frontdoor login, the
// clusters/upload presign and the presigned upload itself, and records the backoff
// durations testUpload asks for instead of actually sleeping.
type testUploadFixture struct {
	service       *ApptioServiceImpl
	server        *httptest.Server
	sleeps        []time.Duration
	uploadCalls   int
	uploadStatus  int
	presignedPath string
}

func newTestUploadFixture(uploadStatus int) *testUploadFixture {
	fixture := &testUploadFixture{uploadStatus: uploadStatus, presignedPath: "/upload"}

	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, apikeyloginEndpoint):
			w.Header().Set("Apptio-Opentoken", "test-token")
			w.Header().Set("valid_till", "9999999999999")
			w.WriteHeader(http.StatusOK)
		case strings.Contains(r.URL.Path, clustersUploadEndpoint):
			w.Header().Set(contentTypeHeader, "application/json")
			Expect(json.NewEncoder(w).Encode(map[string]any{
				"result": map[string]any{
					"location":  fixture.server.URL + fixture.presignedPath,
					"requestId": "req-123",
				},
			})).To(Succeed())
		default:
			// the deliberately broken presigned URL testUpload probes
			fixture.uploadCalls++
			w.WriteHeader(fixture.uploadStatus)
		}
	}))

	fixture.service = &ApptioServiceImpl{
		SecretManager:    NewKeyValueSecretManager("access", "secret"),
		EnvID:            "env-123",
		FrontdoorURL:     fixture.server.URL,
		CloudabilityURL:  fixture.server.URL,
		CldyUploadClient: ApptioClient{client: fixture.server.Client(), maxRetries: defaultRetries},
		sleepFunc: func(d time.Duration) {
			fixture.sleeps = append(fixture.sleeps, d)
		},
	}

	return fixture
}

var _ = Describe("ApptioService testUpload retries", func() {
	It("backs off in seconds rather than nanoseconds between failed attempts", func() {
		fixture := newTestUploadFixture(http.StatusInternalServerError)
		DeferCleanup(fixture.server.Close)

		Expect(fixture.service.testUpload()).To(HaveOccurred())

		Expect(fixture.uploadCalls).To(Equal(testUploadAttempts))
		// backoff only happens between attempts, so the final failure returns immediately
		Expect(fixture.sleeps).To(Equal([]time.Duration{2 * time.Second, 4 * time.Second}))
	})

	It("returns nil without any backoff when the presigned URL responds 403", func() {
		fixture := newTestUploadFixture(http.StatusForbidden)
		DeferCleanup(fixture.server.Close)

		Expect(fixture.service.testUpload()).To(Succeed())

		Expect(fixture.uploadCalls).To(Equal(1))
		Expect(fixture.sleeps).To(BeEmpty())
	})

	It("returns the max failures error once all attempts are exhausted", func() {
		fixture := newTestUploadFixture(http.StatusBadGateway)
		DeferCleanup(fixture.server.Close)

		err := fixture.service.testUpload()

		Expect(err).To(MatchError("bucket upload exceeded max amount of failures"))
		Expect(fixture.uploadCalls).To(Equal(testUploadAttempts))
	})
})

// stubRoundTripper serves canned responses so that doWithRetry specs can observe how the
// response body of each failed attempt is handled.
type stubRoundTripper struct {
	statuses []int
	calls    int
	bodies   []*recordingBody
}

func (rt *stubRoundTripper) RoundTrip(_ *http.Request) (*http.Response, error) {
	status := rt.statuses[len(rt.statuses)-1]
	if rt.calls < len(rt.statuses) {
		status = rt.statuses[rt.calls]
	}
	rt.calls++
	body := &recordingBody{Reader: strings.NewReader("response body")}
	rt.bodies = append(rt.bodies, body)
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Body:       body,
		Header:     http.Header{},
	}, nil
}

// recordingBody notes whether the response body was closed
type recordingBody struct {
	*strings.Reader
	closed bool
}

func (b *recordingBody) Close() error {
	b.closed = true
	return nil
}

var _ = Describe("ApptioClient doWithRetry", func() {
	var (
		transport *stubRoundTripper
		sleeps    []time.Duration
		client    ApptioClient
	)

	newClient := func(statuses ...int) {
		transport = &stubRoundTripper{statuses: statuses}
		sleeps = nil
		client = ApptioClient{
			client:     &http.Client{Transport: transport},
			maxRetries: defaultRetries,
			sleepFunc: func(d time.Duration) {
				sleeps = append(sleeps, d)
			},
		}
	}

	newRequest := func() *http.Request {
		request, err := http.NewRequest(http.MethodPost, "https://api.cloudability.com"+clustersUploadEndpoint, nil)
		Expect(err).ToNot(HaveOccurred())
		return request
	}

	It("backs off in seconds rather than nanoseconds between failed attempts", func() {
		newClient(http.StatusInternalServerError)

		resp, err := client.Do(newRequest(), "failing request")

		Expect(err).To(HaveOccurred())
		Expect(resp).To(BeNil())
		Expect(transport.calls).To(Equal(defaultRetries))
		// backoff only happens between attempts, so the final failure returns immediately
		Expect(sleeps).To(Equal([]time.Duration{2 * time.Second, 4 * time.Second}))
	})

	It("returns the max retries error once all attempts are exhausted", func() {
		newClient(http.StatusBadGateway)

		_, err := client.Do(newRequest(), "failing request")

		Expect(err).To(MatchError("failed to complete request after maximum retries"))
		Expect(transport.calls).To(Equal(defaultRetries))
	})

	It("closes the response body of every failed attempt", func() {
		newClient(http.StatusInternalServerError)

		_, err := client.Do(newRequest(), "failing request")
		Expect(err).To(HaveOccurred())

		Expect(transport.bodies).To(HaveLen(defaultRetries))
		for i, body := range transport.bodies {
			Expect(body.closed).To(BeTrue(), "body of attempt %d was not closed", i+1)
		}
	})

	It("returns a successful response without sleeping and leaves its body open for the caller", func() {
		newClient(http.StatusOK)

		resp, err := client.Do(newRequest(), "successful request")

		Expect(err).ToNot(HaveOccurred())
		Expect(transport.calls).To(Equal(1))
		Expect(sleeps).To(BeEmpty())
		Expect(transport.bodies[0].closed).To(BeFalse())
		Expect(io.ReadAll(resp.Body)).To(Equal([]byte("response body")))
	})

	It("stops backing off as soon as an attempt succeeds", func() {
		newClient(http.StatusInternalServerError, http.StatusInternalServerError, http.StatusOK)

		_, err := client.Do(newRequest(), "eventually successful request")

		Expect(err).ToNot(HaveOccurred())
		Expect(transport.calls).To(Equal(3))
		Expect(sleeps).To(Equal([]time.Duration{2 * time.Second, 4 * time.Second}))
	})
})

// bodyLengthRecorder is a real (loopback-only) HTTP server that records how many body bytes it
// actually received on each request. Recording on the server side is the only way to catch a
// retry that re-sends a consumed body: the client-side request object still looks well formed.
type bodyLengthRecorder struct {
	server *httptest.Server
	mu     sync.Mutex
	// lengths holds one entry per request that reached the handler
	lengths []int
}

func newBodyLengthRecorder(status int) *bodyLengthRecorder {
	recorder := &bodyLengthRecorder{}
	recorder.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Record what actually arrived even if the read fails: a retry that re-sends an
		// exhausted body shows up here as a short (or zero-length) request, and failing the
		// assertion on the recorded lengths is far more legible than an error from this
		// handler goroutine.
		received, _ := io.Copy(io.Discard, r.Body)
		recorder.mu.Lock()
		recorder.lengths = append(recorder.lengths, int(received))
		recorder.mu.Unlock()
		w.WriteHeader(status)
	}))
	return recorder
}

func (r *bodyLengthRecorder) received() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.lengths...)
}

var _ = Describe("ApptioClient doWithRetry request bodies", func() {
	// writeUploadFile lays down a payload big enough that a truncated retry is unmistakable.
	writeUploadFile := func(size int) (string, int) {
		dir, err := os.MkdirTemp("", "cldy-upload-retry")
		Expect(err).ToNot(HaveOccurred())
		DeferCleanup(func() {
			Expect(os.RemoveAll(dir)).To(Succeed())
		})
		filePath := filepath.Join(dir, "sample.tgz")
		Expect(os.WriteFile(filePath, bytes.Repeat([]byte("a"), size), 0o600)).To(Succeed())
		return filePath, size
	}

	It("re-sends the whole presigned PUT body on every retry attempt", func() {
		filePath, size := writeUploadFile(4096)
		recorder := newBodyLengthRecorder(http.StatusInternalServerError)
		DeferCleanup(recorder.server.Close)

		var sleeps []time.Duration
		client := ApptioClient{
			client:     recorder.server.Client(),
			maxRetries: defaultRetries,
			sleepFunc:  func(d time.Duration) { sleeps = append(sleeps, d) },
		}

		payload := UploadPayload{
			ClusterUID: "cluster-uid",
			FileName:   "sample.tgz",
			FilePath:   filePath,
			UploadHash: "aexCzQgBAnRYEZxKy71lAw==",
		}

		err := uploadPayloadToPresignedURL(client, payload, recorder.server.URL+"/presigned")
		Expect(err).To(MatchError("failed to complete request after maximum retries"))

		// Every attempt must reach the server carrying the complete file. Before GetBody was set
		// on the PUT this was [4096 0]: the transport closed the *os.File after attempt 1, so the
		// retry delivered a zero-byte object to the presigned URL and the third attempt never
		// left the client.
		Expect(recorder.received()).To(Equal([]int{size, size, size}))
		Expect(sleeps).To(Equal([]time.Duration{2 * time.Second, 4 * time.Second}))
	})

	It("attempts a request whose body cannot be replayed exactly once", func() {
		transport := &stubRoundTripper{statuses: []int{http.StatusInternalServerError}}
		var sleeps []time.Duration
		client := ApptioClient{
			client:     &http.Client{Transport: transport},
			maxRetries: defaultRetries,
			sleepFunc:  func(d time.Duration) { sleeps = append(sleeps, d) },
		}

		// An opaque ReadCloser: http.NewRequest cannot derive GetBody for it, so the body is
		// gone once the transport has read it.
		request, err := http.NewRequest(http.MethodPut, "https://s3.example.com/presigned",
			io.NopCloser(strings.NewReader("unreplayable body")))
		Expect(err).ToNot(HaveOccurred())
		Expect(request.GetBody).To(BeNil())

		resp, err := client.Do(request, "unreplayable request")

		Expect(err).To(MatchError("failed to complete request after maximum retries"))
		Expect(resp).To(BeNil())
		Expect(transport.calls).To(Equal(1))
		Expect(sleeps).To(BeEmpty())
	})

	It("does not leak the upload file descriptor when the request cannot be built", func() {
		filePath, _ := writeUploadFile(64)
		payload := UploadPayload{FilePath: filePath, UploadHash: "aexCzQgBAnRYEZxKy71lAw=="}

		// An unparseable URL makes http.NewRequest fail *after* the file has been opened, which
		// is exactly the early-return path that used to drop the descriptor on the floor.
		const unparseableURL = "://not-a-url"

		openDescriptors := func() int {
			entries, err := os.ReadDir("/dev/fd")
			Expect(err).ToNot(HaveOccurred())
			return len(entries)
		}

		// Warm up so lazily-created runtime descriptors are not counted as leaks.
		Expect(uploadPayloadToPresignedURL(nil, payload, unparseableURL)).To(HaveOccurred())

		before := openDescriptors()
		const iterations = 40
		for range iterations {
			Expect(uploadPayloadToPresignedURL(nil, payload, unparseableURL)).To(HaveOccurred())
		}
		after := openDescriptors()

		Expect(after-before).To(BeNumerically("<", iterations/4),
			"upload file descriptors leaked: %d open before, %d after %d failed uploads",
			before, after, iterations)
	})

	It("still retries a request that carries no body at all", func() {
		transport := &stubRoundTripper{statuses: []int{http.StatusInternalServerError}}
		var sleeps []time.Duration
		client := ApptioClient{
			client:     &http.Client{Transport: transport},
			maxRetries: defaultRetries,
			sleepFunc:  func(d time.Duration) { sleeps = append(sleeps, d) },
		}

		request, err := http.NewRequest(http.MethodGet, "https://api.cloudability.com/ping", nil)
		Expect(err).ToNot(HaveOccurred())

		_, err = client.Do(request, "bodyless request")

		Expect(err).To(HaveOccurred())
		Expect(transport.calls).To(Equal(defaultRetries))
		Expect(sleeps).To(Equal([]time.Duration{2 * time.Second, 4 * time.Second}))
	})
})

var _ = Describe("ApptioClient redirect policy", func() {
	// The upload client sets GetBody on the requests that carry a body, so that doWithRetry can
	// replay them. That is also exactly the condition net/http requires before it will follow a
	// 307/308 with a body, so without an explicit policy a redirect from the upload host would
	// replay the whole sample tar - and the API key login body - to whatever host it named.
	It("does not follow a redirect away from the upload host", func() {
		var redirectTarget struct {
			sync.Mutex
			hits int
			body int
		}
		attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received, _ := io.ReadAll(r.Body)
			redirectTarget.Lock()
			redirectTarget.hits++
			redirectTarget.body += len(received)
			redirectTarget.Unlock()
			w.WriteHeader(http.StatusOK)
		}))
		defer attacker.Close()

		upload := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, attacker.URL+"/exfil", http.StatusTemporaryRedirect)
		}))
		defer upload.Close()

		payload := []byte("cluster sample contents")
		request, err := http.NewRequest(http.MethodPut, upload.URL+"/presigned", bytes.NewReader(payload))
		Expect(err).ToNot(HaveOccurred())
		var replayed []*recordingBody
		request.GetBody = func() (io.ReadCloser, error) {
			body := &recordingBody{Reader: strings.NewReader(string(payload))}
			replayed = append(replayed, body)
			return body, nil
		}

		client := NewApptioClient(ApptioConfig{Timeout: 5 * time.Second})
		resp, err := client.client.Do(request)
		Expect(err).ToNot(HaveOccurred())
		defer drainAndClose(resp.Body)

		// the 3xx is handed back rather than followed, so the upload fails closed and is retried
		// against the original host instead of being recorded as a success
		Expect(resp.StatusCode).To(Equal(http.StatusTemporaryRedirect))

		Expect(replayed).To(HaveLen(1), "net/http opens one replay body for the redirect hop")
		Expect(replayed[0].closed).To(BeTrue(), "the body opened for the refused redirect hop must be closed")

		redirectTarget.Lock()
		defer redirectTarget.Unlock()
		Expect(redirectTarget.hits).To(Equal(0), "the redirect target must never be contacted")
		Expect(redirectTarget.body).To(Equal(0), "no payload bytes may reach the redirect target")
	})
})
