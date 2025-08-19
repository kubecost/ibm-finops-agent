package cldy_test

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const clustersUploadEndpoint = "/v3/internal/containers/clusters/upload"

var _ = Describe("Client", func() {
	Context("GetUploadURL", func() {
		It("should force use proxy when proxyClient is set", func() {
			proxyResponseBody := "Proxy Success!"

			// Create request with clusters endpoint
			req, err := http.NewRequest(http.MethodGet, clustersUploadEndpoint, strings.NewReader(""))
			Expect(err).ToNot(HaveOccurred())

			// Set client with mock round tripper to simulate http call
			client := cldy.ApptioClient{
				ProxyClient: &http.Client{
					Transport: MockRoundTripper(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString(proxyResponseBody)),
						}, nil
					}),
				},
			}

			resp, err := client.Do(req, "test")
			Expect(err).ToNot(HaveOccurred())

			// Validate response has unique body set by round tripper
			body, err := io.ReadAll(resp.Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(body)).To(Equal(proxyResponseBody))
		})
	})
	Context("Non-Clusters Upload Endpoint", func() {
		It("should not force use proxy when proxyClient is set but endpoint is not clusters upload endpoint", func() {
			normalClientResponseBody := "Normal client used!"
			proxyClientResponseBody := "Proxy client used!"

			// Create request with anything _but_ clusters upload endpoint
			req, err := http.NewRequest(http.MethodGet, "test/endpoint", strings.NewReader(""))
			Expect(err).ToNot(HaveOccurred())

			// Set client with mock round tripper to simulate http call
			client := cldy.ApptioClient{
				Client: &http.Client{
					Transport: MockRoundTripper(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString(normalClientResponseBody)),
						}, nil
					}),
				},
				ProxyClient: &http.Client{
					Transport: MockRoundTripper(func(req *http.Request) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(bytes.NewBufferString(proxyClientResponseBody)),
						}, nil
					}),
				},
			}

			resp, err := client.Do(req, "test")
			Expect(err).ToNot(HaveOccurred())

			// Validate response has normal client response body set by round tripper
			// It should _not_ be using the proxy even though the proxy client is set
			body, err := io.ReadAll(resp.Body)
			Expect(err).ToNot(HaveOccurred())
			Expect(string(body)).To(Equal(normalClientResponseBody))
		})
	})
})

type MockRoundTripper func(req *http.Request) (*http.Response, error)

func (m MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m(req)
}
