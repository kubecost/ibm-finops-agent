// This file is part of package cldy rather than cldy_test because the metrics-collector
// connectivity probe (testUpload) is unexported, and its retry behaviour is exactly what the
// specs below pin.
package cldy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metrics Collector", func() {
	Context("Region URL mapping", func() {
		It("maps supported upload regions to metrics-collector endpoints", func() {
			Expect(MetricsCollectorURLForRegion("us-west-2")).
				To(Equal("https://metrics-collector.cloudability.com/metricsample"))
			Expect(MetricsCollectorURLForRegion("eu-central-1")).
				To(Equal("https://metrics-collector-eu.cloudability.com/metricsample"))
			Expect(MetricsCollectorURLForRegion("staging")).
				To(Equal("https://metrics-collector-staging.cloudability.com/metricsample"))
		})
	})

	Context("Upload", func() {
		var (
			mock    *metricsCollectorMockClient
			service MetricsCollectorServiceImpl
			payload UploadPayload
		)

		BeforeEach(func() {
			mock = &metricsCollectorMockClient{countByPath: map[string]int{}}
			service = MetricsCollectorServiceImpl{
				APIKey:           "goodkey123",
				BaseURL:          "https://metrics-collector.example.com/metricsample",
				UserAgent:        "cldy-client/test",
				CldyUploadClient: mock,
			}
			payload = UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     "good-cluster_2025-05-05-18-05-17.tgz",
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     "testdata/daemonsets.jsonl",
			}
		})

		It("uploads using the metrics-collector API key flow", func() {
			err := service.Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})

		It("hands the request body to the client, which closes it per the ClientService contract", func() {
			Expect(service.Upload(payload)).To(Succeed())

			// uploadPayloadToPresignedURL passes the sample as a live *os.File and gives up
			// ownership before calling Do, so only the client closing it keeps the descriptor
			// from leaking once per upload.
			Expect(mock.closedRequestBodies).To(Equal(1))
			uploadedFile, ok := mock.lastRequestBody.(*os.File)
			Expect(ok).To(BeTrue(), "expected the sample to be sent as the *os.File itself")
			_, err := uploadedFile.Read(make([]byte, 1))
			Expect(err).To(MatchError(os.ErrClosed))
		})
	})
})

// metricsCollectorProbeTransport answers the metrics-collector presign request and then plays a
// canned status for every probe request testUpload makes, recording each probe response body so
// that a spec can prove it was closed. It replaces the transport entirely, so no spec using it
// touches the network.
type metricsCollectorProbeTransport struct {
	probeStatus int
	presignURL  string
	probeCalls  int
	bodies      []*recordingBody
}

func (t *metricsCollectorProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, metricsSampleEndpoint) {
		presignResponse, err := json.Marshal(map[string]string{"location": t.presignURL})
		Expect(err).ToNot(HaveOccurred())
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     http.StatusText(http.StatusOK),
			Body:       io.NopCloser(bytes.NewReader(presignResponse)),
			Header:     http.Header{},
		}, nil
	}

	t.probeCalls++
	body := &recordingBody{Reader: strings.NewReader("probe response")}
	t.bodies = append(t.bodies, body)
	return &http.Response{
		StatusCode: t.probeStatus,
		Status:     http.StatusText(t.probeStatus),
		Body:       body,
		Header:     http.Header{},
	}, nil
}

// metricsCollectorTestUploadFixture wires a MetricsCollectorServiceImpl to the canned transport
// above and records the backoff durations testUpload asks for instead of actually sleeping.
type metricsCollectorTestUploadFixture struct {
	service   *MetricsCollectorServiceImpl
	transport *metricsCollectorProbeTransport
	sleeps    []time.Duration
}

func newMetricsCollectorTestUploadFixture(probeStatus int) *metricsCollectorTestUploadFixture {
	fixture := &metricsCollectorTestUploadFixture{
		transport: &metricsCollectorProbeTransport{
			probeStatus: probeStatus,
			presignURL:  "https://metrics-collector.example.com/upload",
		},
	}

	fixture.service = &MetricsCollectorServiceImpl{
		APIKey:    "test-key",
		BaseURL:   "https://metrics-collector.example.com" + metricsSampleEndpoint,
		UserAgent: "cldy-client/test",
		CldyUploadClient: ApptioClient{
			client:     &http.Client{Transport: fixture.transport},
			maxRetries: defaultRetries,
		},
		sleepFunc: func(d time.Duration) {
			fixture.sleeps = append(fixture.sleeps, d)
		},
	}

	return fixture
}

var _ = Describe("MetricsCollectorService testUpload retries", func() {
	It("backs off in seconds rather than nanoseconds between failed attempts", func() {
		fixture := newMetricsCollectorTestUploadFixture(http.StatusInternalServerError)

		Expect(fixture.service.testUpload()).To(HaveOccurred())

		Expect(fixture.transport.probeCalls).To(Equal(testUploadAttempts))
		// backoff only happens between attempts, so the final failure returns immediately
		Expect(fixture.sleeps).To(Equal([]time.Duration{2 * time.Second, 4 * time.Second}))
	})

	It("returns nil without any backoff when the presigned URL responds 403", func() {
		fixture := newMetricsCollectorTestUploadFixture(http.StatusForbidden)

		Expect(fixture.service.testUpload()).To(Succeed())

		Expect(fixture.transport.probeCalls).To(Equal(1))
		Expect(fixture.sleeps).To(BeEmpty())
	})

	It("returns the metrics-collector max failures error once all attempts are exhausted", func() {
		fixture := newMetricsCollectorTestUploadFixture(http.StatusBadGateway)

		err := fixture.service.testUpload()

		Expect(err).To(MatchError("metrics-collector test upload exceeded max amount of failures"))
		Expect(fixture.transport.probeCalls).To(Equal(testUploadAttempts))
	})

	It("closes the response body of every probe attempt", func() {
		fixture := newMetricsCollectorTestUploadFixture(http.StatusInternalServerError)

		Expect(fixture.service.testUpload()).To(HaveOccurred())

		Expect(fixture.transport.bodies).To(HaveLen(testUploadAttempts))
		for i, body := range fixture.transport.bodies {
			Expect(body.closed).To(BeTrue(), "body of probe attempt %d was not closed", i+1)
		}
	})
})

type metricsCollectorMockClient struct {
	countByPath map[string]int
	// closedRequestBodies counts the request bodies this double closed, and lastRequestBody keeps
	// the most recent one so a spec can check the descriptor really is closed
	closedRequestBodies int
	lastRequestBody     io.ReadCloser
}

func (m *metricsCollectorMockClient) Do(r *http.Request, _ string) (*http.Response, error) {
	if m.countByPath == nil {
		m.countByPath = map[string]int{}
	}
	m.countByPath[r.URL.Path]++

	// A ClientService owns the request body: net/http's Transport closes whatever it is handed,
	// and uploadPayloadToPresignedURL relies on that, handing over a live *os.File and no longer
	// closing it itself. A double that skips this leaks a descriptor per upload in the suite.
	if r.Body != nil {
		m.lastRequestBody = r.Body
		defer func() {
			m.closedRequestBodies++
			Expect(r.Body.Close()).To(Succeed())
		}()
	}

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
