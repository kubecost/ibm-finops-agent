package nodes

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/ibm/finops-agent/pkg/cluster"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

type mockClusterCache struct {
	cluster.ClusterCache
}

func NewMockClusterCache() (cluster.ClusterCache) {
	return mockClusterCache{}
}

func (m mockClusterCache) GetAllNodes() []*v1.Node {
	return nil
}

// labels found on an amazon EKS fargate node
var fargateLabels = map[string]string{
	"eks.amazonaws.com/compute-type": "fargate",
	"beta.kubernetes.io/os":          "linux",
}

// labels found on a generic node
var nodeSampleLabels = map[string]string{
	"beta.kubernetes.io/os":          "linux",
	"kubernetes.io/arch":             "amd64",
	"eks.amazonaws.com/compute-type": "not-fargate",
}

func TestDownloadNodeData(t *testing.T) {
	returnCodes := []int{200, 200, 200, 400, 400, 400, 200, 200, 200, 400}
	ts := launchTLSTestServer(returnCodes)
	// cs := NewTestClient(ts, nodeSampleLabels)
	defer ts.Close()

	kac := KubeAgentConfig{
		ClusterHostURL:		"https://" + ts.Listener.Addr().String(),
		ForceKubeProxy:     false,
		ConcurrentPollers:  10,
		DirectNodeClient:   NewClient(http.Client{}, 0),
		InClusterClient:    NewClient(http.Client{}, 0),
	}
	mockCache := NewMockClusterCache()
	nodeStatsSummaryClient := NewNodeStatsSummaryClient(mockCache, kac)



	t.Run("Ensure node added to fail list when providerID doesn't exist", func(t *testing.T) {
		// ed, ns, ka := setupTestNodeDownloaderClients(ts, cs, 1)
		
		stats, err := nodeStatsSummaryClient.GetNodeData()
		if err != nil {
			t.Error("Threw error")
		}

		if stats == nil {
			t.Error("Good error!")
		}
	})

	// t.Run("Ensure error is returned when GetReadyNodes returns error", func(t *testing.T) {
	// 	ed, _, ka := setupTestNodeDownloaderClients(ts, cs, 1)
	// 	ns := testNodeSource{}

	// 	_, err := downloadNodeData(
	// 		context.TODO(),
	// 		"baseline",
	// 		ka,
	// 		ed,
	// 		ns,
	// 	)

	// 	if err == nil {
	// 		t.Error("Expected no nodes found error")
	// 	}

	// 	if err.Error() != "cloudability metric agent is unable to get a list of nodes: 0 nodes were ready" {
	// 		t.Error("unexpected error")
	// 	}
	// })
}

// launchTLSTestServer takes a slice of http status codes (int) to return
func launchTLSTestServer(responseCodes []int) *httptest.Server {
	callCount := 0
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount < len(responseCodes) {
			w.WriteHeader(responseCodes[callCount])
			callCount++
		}
	}))

	return ts
}

func NewTestClient(ts *httptest.Server, labels map[string]string) *fake.Clientset {
	return NewTestClientWithNodes(ts, labels, 1)
}

func NewTestClientWithNodes(ts *httptest.Server, labels map[string]string, numNodes int) *fake.Clientset {
	s := strings.Split(ts.Listener.Addr().String(), ":")
	ip := s[0]
	port, _ := strconv.Atoi(s[1])
	nodes := make([]runtime.Object, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("proxyNode.%d", i),
				Namespace: v1.NamespaceDefault,
				Labels:    labels,
			},
			Status: v1.NodeStatus{
				Addresses: []v1.NodeAddress{
					{
						Type:    "InternalIP",
						Address: ip,
					},
				},
				Conditions: []v1.NodeCondition{{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				}},
				DaemonEndpoints: v1.NodeDaemonEndpoints{
					KubeletEndpoint: v1.DaemonEndpoint{
						Port: int32(port),
					},
				},
			},
		}
	}
	return fake.NewSimpleClientset(nodes...)
}

// setupTestNodeDownloaderClients returns commonly-needed configs and clients
// for testing node downloads
// func setupTestNodeDownloaderClients(ts *httptest.Server,
// 	cs *fake.Clientset,
// 	retries uint) (*os.File, testNodeSource, KubeAgentConfig) {
// 	c := http.Client{
// 		Transport: &http.Transport{
// 			TLSClientConfig: &tls.Config{
// 				// nolint gosec
// 				InsecureSkipVerify: true,
// 			},
// 		}}
// 	rc := NewClient(
// 		c,
// 		true,
// 		"",
// 		"",
// 		retries,
// 		false,
// 	)
// 	ka := KubeAgentConfig{
// 		Clientset:            cs,
// 		HTTPClient:           c,
// 		InClusterClient:      rc,
// 		ClusterHostURL:       "https://" + ts.Listener.Addr().String(),
// 		ConcurrentPollers:    10,
// 		CollectionRetryLimit: retries,
// 	}
// 	ka.NodeMetrics = EndpointMask{}
// 	ka.NodeMetrics.SetAvailability(NodeStatsSummaryEndpoint, Proxy, true)

// 	wd, _ := os.Getwd()
// 	ed, _ := os.Open(fmt.Sprintf("%s/testdata", wd))

// 	ns := testNodeSource{}

// 	s := strings.Split(ts.Listener.Addr().String(), ":")
// 	ip := s[0]
// 	port, _ := strconv.Atoi(s[1])
// 	ns.Nodes = []v1.Node{
// 		{
// 			ObjectMeta: metav1.ObjectMeta{Name: "proxyNode", Namespace: v1.NamespaceDefault},
// 			Status: v1.NodeStatus{
// 				Addresses: []v1.NodeAddress{
// 					{
// 						Type:    "InternalIP",
// 						Address: ip,
// 					},
// 				},
// 				Conditions: []v1.NodeCondition{{
// 					Type:   v1.NodeReady,
// 					Status: v1.ConditionTrue,
// 				}},
// 				DaemonEndpoints: v1.NodeDaemonEndpoints{
// 					KubeletEndpoint: v1.DaemonEndpoint{
// 						Port: int32(port),
// 					},
// 				},
// 			},
// 			Spec: v1.NodeSpec{
// 				PodCIDR: "",
// 			},
// 		},
// 	}
// 	return ed, ns, ka
// }
