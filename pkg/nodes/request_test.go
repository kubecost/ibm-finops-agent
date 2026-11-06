package nodes

import (
	"net/url"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("directNode formatEndpoint", func() {
	Context("IPv4 addresses", func() {
		It("should format IPv4 address correctly", func() {
			node := directNode{
				ip:   "10.128.15.239",
				port: 10250,
			}
			endpoint := node.formatEndpoint("stats/summary")
			Expect(endpoint).To(Equal("https://10.128.15.239:10250/stats/summary"))

			// Verify URL is parseable
			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Scheme).To(Equal("https"))
			Expect(parsedURL.Host).To(Equal("10.128.15.239:10250"))
			Expect(parsedURL.Path).To(Equal("/stats/summary"))
		})

		It("should handle different ports", func() {
			node := directNode{
				ip:   "192.168.1.1",
				port: 443,
			}
			endpoint := node.formatEndpoint("metrics")
			Expect(endpoint).To(Equal("https://192.168.1.1:443/metrics"))

			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Host).To(Equal("192.168.1.1:443"))
		})
	})

	Context("IPv6 addresses", func() {
		It("should wrap IPv6 address in brackets", func() {
			node := directNode{
				ip:   "2a05:d014:1314:704::a282",
				port: 10250,
			}
			endpoint := node.formatEndpoint("stats/summary")
			Expect(endpoint).To(Equal("https://[2a05:d014:1314:704::a282]:10250/stats/summary"))

			// Verify URL is parseable
			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Scheme).To(Equal("https"))
			Expect(parsedURL.Host).To(Equal("[2a05:d014:1314:704::a282]:10250"))
			Expect(parsedURL.Path).To(Equal("/stats/summary"))
		})

		It("should handle IPv6 loopback address", func() {
			node := directNode{
				ip:   "::1",
				port: 10250,
			}
			endpoint := node.formatEndpoint("stats/summary")
			Expect(endpoint).To(Equal("https://[::1]:10250/stats/summary"))

			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Host).To(Equal("[::1]:10250"))
		})

		It("should handle private IPv6 address", func() {
			node := directNode{
				ip:   "fd00::1",
				port: 443,
			}
			endpoint := node.formatEndpoint("metrics")
			Expect(endpoint).To(Equal("https://[fd00::1]:443/metrics"))

			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Host).To(Equal("[fd00::1]:443"))
		})

		It("should handle full IPv6 address", func() {
			node := directNode{
				ip:   "2001:db8:85a3:0:0:8a2e:370:7334",
				port: 8080,
			}
			endpoint := node.formatEndpoint("api/v1/health")
			Expect(endpoint).To(Equal("https://[2001:db8:85a3:0:0:8a2e:370:7334]:8080/api/v1/health"))

			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Scheme).To(Equal("https"))
			Expect(parsedURL.Host).To(Equal("[2001:db8:85a3:0:0:8a2e:370:7334]:8080"))
			Expect(parsedURL.Path).To(Equal("/api/v1/health"))
		})

		It("should handle compressed IPv6 address", func() {
			node := directNode{
				ip:   "2001:db8::1",
				port: 10250,
			}
			endpoint := node.formatEndpoint("stats/summary")
			Expect(endpoint).To(Equal("https://[2001:db8::1]:10250/stats/summary"))

			parsedURL, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
			Expect(parsedURL.Host).To(Equal("[2001:db8::1]:10250"))
		})
	})

	Context("Edge cases", func() {
		It("should handle empty path", func() {
			node := directNode{
				ip:   "10.0.0.1",
				port: 443,
			}
			endpoint := node.formatEndpoint("")
			Expect(endpoint).To(Equal("https://10.0.0.1:443/"))

			_, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should handle path with leading slash", func() {
			node := directNode{
				ip:   "192.168.1.1",
				port: 8080,
			}
			endpoint := node.formatEndpoint("/api/v1/nodes")
			Expect(endpoint).To(Equal("https://192.168.1.1:8080//api/v1/nodes"))

			_, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should handle different port values", func() {
			node := directNode{
				ip:   "10.0.0.1",
				port: 1,
			}
			endpoint := node.formatEndpoint("test")
			Expect(endpoint).To(Equal("https://10.0.0.1:1/test"))

			_, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should handle large port numbers", func() {
			node := directNode{
				ip:   "10.0.0.1",
				port: 65535,
			}
			endpoint := node.formatEndpoint("test")
			Expect(endpoint).To(Equal("https://10.0.0.1:65535/test"))

			_, err := url.Parse(endpoint)
			Expect(err).ToNot(HaveOccurred())
		})
	})
})

// Made with Bob
