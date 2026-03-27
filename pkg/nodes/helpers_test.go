package nodes

import (
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/ibm/finops-agent/pkg/cluster"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// singleNodeCache is a minimal ClusterCache returning a fixed list of nodes.
type singleNodeCache struct {
	cluster.ClusterCache
	nodes []*v1.Node
}

func (s *singleNodeCache) GetAllNodes() []*v1.Node {
	return s.nodes
}

// failingHTTPClient always returns an error.
type failingHTTPClient struct{}

func (f *failingHTTPClient) Do(_ *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("connection refused")
}

// mockStatsSummaryClient allows injecting a custom function for GetNodeData.
type mockStatsSummaryClient struct {
	fn func() ([]*stats.Summary, error)
}

func (m *mockStatsSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	return m.fn()
}

func newTestNode(name, ip string, port int32) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{},
		},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
			Addresses: []v1.NodeAddress{
				{Type: v1.NodeInternalIP, Address: ip},
			},
			DaemonEndpoints: v1.NodeDaemonEndpoints{
				KubeletEndpoint: v1.DaemonEndpoint{Port: port},
			},
		},
	}
}

// hostPort extracts host and port from a URL like "https://127.0.0.1:12345".
func hostPort(url string) (string, int32) {
	addr := strings.TrimPrefix(url, "https://")
	addr = strings.TrimPrefix(addr, "http://")
	host, portStr, _ := net.SplitHostPort(addr)
	var port int32
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}
