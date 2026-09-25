package nodes

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/ibm/finops-agent/pkg/cluster"
	v1 "k8s.io/api/core/v1"
)

// staticNodeCache serves a fixed node list.
type staticNodeCache struct {
	cluster.ClusterCache
	nodes []*v1.Node
}

func (c staticNodeCache) GetAllNodes() []*v1.Node { return c.nodes }

func fakeNode(i int, ready v1.ConditionStatus) *v1.Node {
	return &v1.Node{
		Name: fmt.Sprintf("node-%03d", i),
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeMemoryPressure, Status: v1.ConditionFalse},
				{Type: v1.NodeReady, Status: ready},
			},
			Addresses:       []v1.NodeAddress{{Type: v1.NodeInternalIP, Address: fmt.Sprintf("10.0.%d.%d", i/256, i%256)}},
			DaemonEndpoints: v1.NodeDaemonEndpoints{KubeletEndpoint: v1.DaemonEndpoint{Port: 10250}},
		},
	}
}

// requestRecorder answers every stats/summary request with a minimal summary and records the URL.
type requestRecorder struct {
	mu   sync.Mutex
	urls []string
}

func (r *requestRecorder) Do(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	r.urls = append(r.urls, req.URL.String())
	r.mu.Unlock()
	body := `{"node":{"nodeName":"x"}}`
	return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString(body)), Header: http.Header{}}, nil
}

func TestGetReadyNodesRequiresReadyTrue(t *testing.T) {
	cache := staticNodeCache{nodes: []*v1.Node{
		fakeNode(0, v1.ConditionTrue),
		fakeNode(1, v1.ConditionFalse),
		fakeNode(2, v1.ConditionUnknown),
		fakeNode(3, v1.ConditionTrue),
	}}
	var names []string
	for _, n := range getReadyNodes(cache) {
		names = append(names, n.Name)
	}
	if got := strings.Join(names, ","); got != "node-000,node-003" {
		t.Errorf("F-43: getReadyNodes returned %q; want only the Ready=True nodes node-000,node-003", got)
	}
}

// F-43: in a 500-node cluster with 50 NotReady/Unknown nodes, those nodes are not polled.
func TestNotReadyNodesAreNotPolled(t *testing.T) {
	var nodes []*v1.Node
	notReady := map[string]bool{}
	for i := range 500 {
		status := v1.ConditionTrue
		switch {
		case i%10 == 0 && i%20 == 0:
			status = v1.ConditionFalse
		case i%10 == 0:
			status = v1.ConditionUnknown
		}
		n := fakeNode(i, status)
		if status != v1.ConditionTrue {
			notReady[n.Name] = true
			notReady[n.Status.Addresses[0].Address+":"] = true
		}
		nodes = append(nodes, n)
	}

	rec := &requestRecorder{}
	client := &NodeStatsSummaryClient{
		config: NodeClientConfig{
			ConcurrentPollers: 10,
			DirectNodeClient:  Client{client: rec},
			InClusterClient:   Client{client: rec},
			ProxyConfig:       NodeClientProxyConfig{LocalProxy: "http://127.0.0.1:8001"},
		},
		cache:    staticNodeCache{nodes: nodes},
		endpoint: "stats/summary",
	}
	data, err := client.GetNodeData()
	if err != nil {
		t.Fatalf("GetNodeData: %v", err)
	}
	if len(data) != 450 {
		t.Errorf("got stats for %d nodes; want the 450 Ready nodes", len(data))
	}

	var polled []string
	for _, u := range rec.urls {
		for key := range notReady {
			if strings.Contains(u, "/"+key) || strings.Contains(u, "/nodes/"+key+"/") {
				polled = append(polled, u)
			}
		}
	}
	if len(polled) > 0 {
		t.Errorf("F-43: %d requests went to NotReady/Unknown nodes, e.g. %s", len(polled), polled[0])
	}
}
