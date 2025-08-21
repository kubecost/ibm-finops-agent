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
				UseProxyForGettingUploadURLOnly: false,
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
				UseProxyForGettingUploadURLOnly: false,
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
