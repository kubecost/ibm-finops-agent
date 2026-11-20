package cldy_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Upload retry", func() {
	It("re-sends the full sample file after a timed out presigned upload", func() {
		const samplePath = "testdata/daemonsets.jsonl"
		expected, err := os.ReadFile(samplePath)
		Expect(err).ToNot(HaveOccurred())

		var uploadAttempts atomic.Int32
		var uploadedBody atomic.Value
		release := make(chan struct{})
		defer close(release)

		mux := http.NewServeMux()
		server := httptest.NewServer(mux)
		defer server.Close()

		mux.HandleFunc("/metricsample", func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]string{"location": server.URL + "/upload"})
		})
		mux.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if uploadAttempts.Add(1) == 1 {
				// Stall past the client timeout to mimic "awaiting headers" failures.
				select {
				case <-release:
				case <-r.Context().Done():
				}
				return
			}
			uploadedBody.Store(body)
			w.WriteHeader(http.StatusOK)
		})

		service := cldy.MetricsCollectorServiceImpl{
			APIKey:           "key",
			BaseURL:          server.URL + "/metricsample",
			UserAgent:        "cldy-client/test",
			CldyUploadClient: cldy.NewApptioClient(cldy.ApptioConfig{Timeout: 200 * time.Millisecond}),
		}

		err = service.Upload(cldy.UploadPayload{
			ClusterUID:   "cluster",
			AgentVersion: "1.0.0",
			UploadHash:   "hash",
			FilePath:     samplePath,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(uploadAttempts.Load()).To(Equal(int32(2)))
		Expect(uploadedBody.Load()).To(Equal(expected))
	})
})
