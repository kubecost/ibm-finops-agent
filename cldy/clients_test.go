package cldy_test

import (
	"net/http"
	"net/url"

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
				ProxyURL:                        &url.URL{},
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
				ProxyURL:                        proxyURL,
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
