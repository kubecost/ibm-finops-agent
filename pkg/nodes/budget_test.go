package nodes

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
)

// hangingClient blocks every request until its context is done, like an unreachable node.
type hangingClient struct{ requests atomic.Int32 }

func (h *hangingClient) Do(req *http.Request) (*http.Response, error) {
	h.requests.Add(1)
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func unreachableCluster(n int, client HTTPClient, config NodeClientConfig) *NodeStatsSummaryClient {
	var nodes []*v1.Node
	for i := range n {
		nodes = append(nodes, fakeNode(i, v1.ConditionTrue))
	}
	config.ConcurrentPollers = 10
	config.DirectNodeClient = Client{client: client}
	config.InClusterClient = Client{client: client}
	config.ProxyConfig = NodeClientProxyConfig{LocalProxy: "http://127.0.0.1:8001"}
	return &NodeStatsSummaryClient{config: config, cache: staticNodeCache{nodes: nodes}, endpoint: "stats/summary"}
}

// F-31: the fan-out stops at the caller's deadline, returns what it has, and reports the nodes
// it did not reach instead of running one per-node timeout per unreachable node.
func TestNodeStatsFanoutStopsAtDeadline(t *testing.T) {
	client := unreachableCluster(100, &hangingClient{}, NodeClientConfig{NodeTimeout: 50 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	data, err := client.GetNodeDataContext(ctx)
	elapsed := time.Since(start)

	// 100 unreachable nodes × 50ms / 10 pollers = 500ms without a budget.
	if elapsed > 300*time.Millisecond {
		t.Errorf("fan-out took %s; want it to return before the caller's 300ms deadline", elapsed)
	}
	if len(data) != 0 {
		t.Errorf("got %d summaries from unreachable nodes", len(data))
	}
	if err == nil || !strings.Contains(err.Error(), "not polled this cycle") {
		t.Errorf("error does not report the nodes skipped at the budget: %v", err)
	}
}

// The context-free GetNodeData (shared with OpenCost's collector) applies its own deadline.
func TestGetNodeDataAppliesCollectionTimeout(t *testing.T) {
	client := unreachableCluster(100, &hangingClient{}, NodeClientConfig{
		CollectionTimeout: 100 * time.Millisecond,
		NodeTimeout:       time.Hour,
	})

	start := time.Now()
	_, err := client.GetNodeData()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("GetNodeData took %s with a 100ms CollectionTimeout", elapsed)
	}
	if err == nil {
		t.Error("GetNodeData against unreachable nodes returned no error")
	}
}

// failingClient fails every request immediately.
type failingClient struct{ requests atomic.Int32 }

func (f *failingClient) Do(*http.Request) (*http.Response, error) {
	f.requests.Add(1)
	return nil, errors.New("connection refused")
}

// AttemptEndPoint stops retrying when its context is done, including during the backoff sleep.
func TestAttemptEndPointStopsOnContextDone(t *testing.T) {
	fc := &failingClient{}
	c := Client{client: fc, retries: 3} // 2s + 4s + 8s of backoff without a context

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := c.AttemptEndPoint(ctx, http.MethodGet, "https://10.0.0.1:10250/stats/summary", "")
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("AttemptEndPoint took %s after its context expired at 100ms", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v; want it to wrap context.DeadlineExceeded", err)
	}
	if got := fc.requests.Load(); got != 1 {
		t.Errorf("made %d requests; want 1 before the deadline", got)
	}
}
