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

	})
})

type metricsCollectorMockClient struct {
	countByPath map[string]int
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

		responseBody, _ := json.Marshal(map[string]string{
			"location": "https://metrics-collector.example.com/somewhere/valid-location",
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
