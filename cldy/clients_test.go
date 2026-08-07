package cldy_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const clustersUploadEndpoint = "/v3/internal/containers/clusters/upload"

var _ = Describe("Client Proxy", func() {
	Context("Clusters Upload", func() {
		proxyExample := "https://proxy.example.com"
		proxyURL, err := url.Parse(proxyExample)
		Expect(err).ToNot(HaveOccurred())

		requestURL := "https://api.cloudability.com/v3/internal/containers/clusters/upload"

		It("should not be used when Proxy URL is not set", func() {
			config := cldy.ApptioConfig{
				ProxyURL: &url.URL{},
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				ProxyURL: proxyURL,
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				ProxyURL: &url.URL{},
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				ProxyURL: proxyURL,
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				ProxyURL: proxyURL,
			}
			requestURLEU := "https://frontdoor-eu.apptio.com/service/apikeylogin"

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				UseProxyForGettingUploadURLOnly: true,
				ProxyURL:                        proxyURL,
			}

			proxyFunc := cldy.BuildProxyFunc(config)
			request, err := http.NewRequest(http.MethodPost, requestURL, nil)
			Expect(err).ToNot(HaveOccurred())

			actualURL, err := proxyFunc(request)
			Expect(err).ToNot(HaveOccurred())
			Expect(actualURL.Host).To(Equal(config.ProxyURL.Host))
		})

		It("should not use proxy for S3 upload when UseProxyForGettingUploadURLOnly is true", func() {
			config := cldy.ApptioConfig{
				UseProxyForGettingUploadURLOnly: true,
				ProxyURL:                        proxyURL,
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				ProxyURL: &url.URL{},
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				ProxyURL: proxyURL,
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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
			config := cldy.ApptioConfig{
				UseProxyForGettingUploadURLOnly: true,
				ProxyURL:                        proxyURL,
			}

			proxyFunc := cldy.BuildProxyFunc(config)
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

			service := cldy.ApptioServiceImpl{
				SecretManager:    cldy.NewKeyValueSecretManager("access", "secret"),
				EnvID:            "env-123",
				FrontdoorURL:     "https://frontdoor.apptio.com",
				CloudabilityURL:  "https://api.cloudability.com",
				CldyUploadClient: mock,
			}

			payload := cldy.UploadPayload{
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
