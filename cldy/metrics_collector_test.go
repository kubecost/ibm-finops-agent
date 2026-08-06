package cldy_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metrics Collector", func() {
	Context("Region URL mapping", func() {
		It("maps supported upload regions to metrics-collector endpoints", func() {
			Expect(cldy.MetricsCollectorURLForRegion("us-west-2")).
				To(Equal("https://metrics-collector.cloudability.com/metricsample"))
			Expect(cldy.MetricsCollectorURLForRegion("eu-central-1")).
				To(Equal("https://metrics-collector-eu.cloudability.com/metricsample"))
			Expect(cldy.MetricsCollectorURLForRegion("staging")).
				To(Equal("https://metrics-collector-staging.cloudability.com/metricsample"))
		})
	})

	Context("Upload", func() {
		It("uploads using the metrics-collector API key flow", func() {
			service := cldy.MetricsCollectorServiceImpl{
				APIKey:    "goodkey123",
				BaseURL:   "https://metrics-collector.example.com/metricsample",
				UserAgent: "cldy-client/test",
				CldyUploadClient: &metricsCollectorMockClient{
					countByPath: map[string]int{},
				},
			}

			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     "good-cluster_2025-05-05-18-05-17.tgz",
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     "testdata/daemonsets.jsonl",
			}

			err := service.Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})

		// The upload destination is taken from the API response rather than from
		// our own configuration, so a tampered response must not be able to
		// redirect the cluster inventory somewhere else (CWE-918).
		DescribeTable("refuses to upload to a destination outside the allowed hosts",
			func(location string, expected string) {
				service := cldy.MetricsCollectorServiceImpl{
					APIKey:    "goodkey123",
					BaseURL:   "https://metrics-collector.example.com/metricsample",
					UserAgent: "cldy-client/test",
					CldyUploadClient: &metricsCollectorMockClient{
						countByPath:      map[string]int{},
						locationOverride: location,
					},
				}

				payload := cldy.UploadPayload{
					ClusterUID:   "good-cluster",
					FileName:     "good-cluster_2025-05-05-18-05-17.tgz",
					AgentVersion: "1.0.0",
					UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
					FilePath:     "testdata/daemonsets.jsonl",
				}

				err := service.Upload(payload)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring(expected))
			},
			Entry("attacker-controlled host",
				"https://evil.example.com/steal", "unexpected host"),
			Entry("lookalike host does not satisfy the suffix",
				"https://evil-amazonaws.com/steal", "unexpected host"),
			Entry("suffix must match on a dot boundary",
				"https://notamazonaws.com/steal", "unexpected host"),
			Entry("plaintext downgrade is refused",
				"http://apptio-production.s3.us-west-2.amazonaws.com/x", "non-HTTPS"),
			Entry("relative location has no host",
				"somewhere/valid-location", "non-HTTPS"),
		)
	})
})

type metricsCollectorMockClient struct {
	countByPath map[string]int
	// locationOverride, when set, replaces the presigned upload URL returned by
	// the mocked metricsample response.
	locationOverride string
}

func (m *metricsCollectorMockClient) Do(r *http.Request, _ string) (*http.Response, error) {
	if m.countByPath == nil {
		m.countByPath = map[string]int{}
	}
	m.countByPath[r.URL.Path]++

	if strings.Contains(r.URL.Path, "metricsample") {
		Expect(r.Header.Get("x-api-key")).To(Equal("goodkey123"))
		Expect(r.Header.Get("token")).To(Equal("goodkey123"))
		Expect(r.Header.Get("x-cluster-uid")).To(Equal("good-cluster"))
		Expect(r.Header.Get("x-upload-file")).To(Equal("aexCzQgBAnRYEZxKy71lAw=="))

		location := "https://apptio-production.s3.us-west-2.amazonaws.com/somewhere/valid-location"
		if m.locationOverride != "" {
			location = m.locationOverride
		}
		responseBody, _ := json.Marshal(map[string]string{
			"location": location,
		})
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(responseBody)),
		}, nil
	}

	if strings.Contains(r.URL.Path, "valid-location") {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}, nil
	}

	return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader(""))}, nil
}
