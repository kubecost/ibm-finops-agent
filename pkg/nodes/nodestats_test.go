package nodes

import (
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// mockStatSummaryClient is a minimal StatSummaryClient for nodestats tests.
type mockStatSummaryClient struct {
	data []*stats.Summary
	err  error
}

func (m *mockStatSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	return m.data, m.err
}

var _ = Describe("NodeStatsSummaryProvider", func() {
	var (
		client   *mockStatSummaryClient
		provider *NodeStatsSummaryProvider
	)

	BeforeEach(func() {
		client = &mockStatSummaryClient{
			data: []*stats.Summary{{Node: stats.NodeStats{NodeName: "node1"}}},
		}
	})

	Describe("GetNodeData", func() {
		Context("when no collection has taken place", func() {
			It("returns an error", func() {
				provider = NewNodeStatsSummaryProvider(client, 0)
				_, err := provider.GetNodeData()
				Expect(err).To(MatchError(ContainSubstring("no node stats summary data has been recorded")))
			})
		})

		Context("when cache is fresh", func() {
			It("returns cached data without error", func() {
				provider = NewNodeStatsSummaryProvider(client, time.Hour)
				provider.statsLock.Lock()
				provider.stats = client.data
				provider.lastRecordedSummary = time.Now().UTC()
				provider.statsLock.Unlock()

				data, err := provider.GetNodeData()
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(HaveLen(1))
				Expect(data[0].Node.NodeName).To(Equal("node1"))
			})
		})

		Context("when cache has exceeded staleTTL", func() {
			It("returns an error", func() {
				provider = NewNodeStatsSummaryProvider(client, time.Millisecond)
				provider.statsLock.Lock()
				provider.stats = client.data
				provider.lastRecordedSummary = time.Now().UTC().Add(-time.Second)
				provider.statsLock.Unlock()

				_, err := provider.GetNodeData()
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(ContainSubstring("node stats summary cache is stale")))
			})
		})

		Context("when staleTTL is zero", func() {
			It("never returns a stale error regardless of cache age", func() {
				provider = NewNodeStatsSummaryProvider(client, 0)
				provider.statsLock.Lock()
				provider.stats = client.data
				provider.lastRecordedSummary = time.Now().UTC().Add(-24 * time.Hour)
				provider.statsLock.Unlock()

				data, err := provider.GetNodeData()
				Expect(err).ToNot(HaveOccurred())
				Expect(data).To(HaveLen(1))
			})
		})
	})
})

// ensure mockStatSummaryClient satisfies the interface at compile time
var _ StatSummaryClient = (*mockStatSummaryClient)(nil)
