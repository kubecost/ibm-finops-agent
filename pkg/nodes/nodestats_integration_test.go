package nodes

import (
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

var _ = Describe("NodeStatsSummaryProvider integration", func() {
	It("collects and caches node stats on an interval", func() {
		callCount := int32(0)
		mockClient := &mockStatsSummaryClient{
			fn: func() ([]*stats.Summary, error) {
				count := atomic.AddInt32(&callCount, 1)
				return []*stats.Summary{
					{Node: stats.NodeStats{NodeName: fmt.Sprintf("node-call-%d", count)}},
				}, nil
			},
		}

		provider := NewNodeStatsSummaryProvider(mockClient)
		started := provider.Start(100 * time.Millisecond)
		Expect(started).To(BeTrue())

		// Initial synchronous call happens immediately
		data, err := provider.GetNodeData()
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(HaveLen(1))
		Expect(data[0].Node.NodeName).To(Equal("node-call-1"))

		// Poll until background refresh has fired at least once more
		Eventually(func() int32 {
			return atomic.LoadInt32(&callCount)
		}, "2s", "50ms").Should(BeNumerically(">=", 2))

		data, err = provider.GetNodeData()
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(HaveLen(1))

		provider.Stop()

		// After stop, data should still be available (cached)
		data, err = provider.GetNodeData()
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(HaveLen(1))
	})

	It("does not overwrite data on total failure", func() {
		callCount := int32(0)
		mockClient := &mockStatsSummaryClient{
			fn: func() ([]*stats.Summary, error) {
				count := atomic.AddInt32(&callCount, 1)
				if count == 1 {
					return []*stats.Summary{
						{Node: stats.NodeStats{NodeName: "good-node"}},
					}, nil
				}
				return nil, fmt.Errorf("all nodes unreachable")
			},
		}

		provider := NewNodeStatsSummaryProvider(mockClient)
		started := provider.Start(50 * time.Millisecond)
		Expect(started).To(BeTrue())

		// Poll until the failing call has been attempted, then verify cached data survived
		Eventually(func() int32 {
			return atomic.LoadInt32(&callCount)
		}, "2s", "50ms").Should(BeNumerically(">=", 2))

		data, err := provider.GetNodeData()
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(HaveLen(1))
		Expect(data[0].Node.NodeName).To(Equal("good-node"))

		provider.Stop()
	})

	It("returns error when no data has been recorded", func() {
		mockClient := &mockStatsSummaryClient{
			fn: func() ([]*stats.Summary, error) {
				return nil, fmt.Errorf("cannot reach any nodes")
			},
		}

		provider := NewNodeStatsSummaryProvider(mockClient)
		started := provider.Start(time.Hour)
		Expect(started).To(BeTrue())

		data, err := provider.GetNodeData()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no node stats summary data has been recorded"))
		Expect(data).To(BeNil())

		provider.Stop()
	})

	It("prevents double-start", func() {
		mockClient := &mockStatsSummaryClient{
			fn: func() ([]*stats.Summary, error) {
				return []*stats.Summary{{Node: stats.NodeStats{NodeName: "n"}}}, nil
			},
		}

		provider := NewNodeStatsSummaryProvider(mockClient)
		Expect(provider.Start(time.Hour)).To(BeTrue())
		Expect(provider.Start(time.Hour)).To(BeFalse())
		provider.Stop()
	})
})
