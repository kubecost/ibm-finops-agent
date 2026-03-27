package nodes

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// -----------------------------------------------------------------------
// IPv4 / IPv6 endpoint formatting
// -----------------------------------------------------------------------

var _ = Describe("Endpoint formatting", func() {
	Context("directNode.formatEndpoint", func() {
		It("formats an IPv4 address correctly", func() {
			d := directNode{ip: "10.0.0.1", port: 10250}
			url := d.formatEndpoint("stats/summary")
			Expect(url).To(Equal("https://10.0.0.1:10250/stats/summary"))
		})

		It("formats an IPv6 address with brackets per RFC 3986", func() {
			d := directNode{ip: "fd00::1", port: 10250}
			url := d.formatEndpoint("stats/summary")
			Expect(url).To(Equal("https://[fd00::1]:10250/stats/summary"))
		})

		It("formats a full IPv6 address with brackets", func() {
			d := directNode{ip: "2001:0db8:85a3:0000:0000:8a2e:0370:7334", port: 10250}
			url := d.formatEndpoint("stats/summary")
			Expect(url).To(Equal("https://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:10250/stats/summary"))
		})
	})

	Context("proxyAPI.formatEndpoint", func() {
		It("formats the proxy URL correctly", func() {
			p := proxyAPI{clusterHostURL: "https://kubernetes.default", nodeName: "node-1"}
			url := p.formatEndpoint("stats/summary")
			Expect(url).To(Equal("https://kubernetes.default/api/v1/nodes/node-1/proxy/stats/summary"))
		})
	})
})

// -----------------------------------------------------------------------
// Integration: real HTTP server for node stats collection
// -----------------------------------------------------------------------

var _ = Describe("Node data collection with HTTP servers", func() {
	var (
		tempBearerFile string
		summaryData    []byte
	)

	BeforeEach(func() {
		file, err := os.CreateTemp("", "bearer-token-*")
		Expect(err).ToNot(HaveOccurred())
		_, err = file.WriteString("test-token")
		Expect(err).ToNot(HaveOccurred())
		file.Close()
		tempBearerFile = file.Name()

		// Minimal valid stats.Summary JSON
		summary := stats.Summary{
			Node: stats.NodeStats{
				NodeName: "test-node",
			},
		}
		summaryData, err = json.Marshal(summary)
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		os.Remove(tempBearerFile)
	})

	Context("with an IPv4 HTTP test server", func() {
		It("collects node stats from a direct node endpoint", func() {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/stats/summary"))
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			// Extract host and port from the test server so direct connection resolves correctly
			host, port := hostPort(server.URL)

			node := newTestNode("ipv4-node", host, port)
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			// Use the test server's own TLS client which trusts its self-signed cert
			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 2,
				DirectNodeClient:  client,
				InClusterClient:   client,
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  server.URL,
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(1))
			Expect(data[0].Node.NodeName).To(Equal("test-node"))
		})

		It("collects node stats via proxy endpoint", func() {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/api/v1/nodes/proxy-node/proxy/stats/summary"))
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			// Node IP doesn't matter for proxy mode — we force proxy and the proxy URL
			// is the test server itself
			node := newTestNode("proxy-node", "192.168.1.100", 10250)
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 2,
				DirectNodeClient:  Client{client: &failingHTTPClient{}, retries: 0},
				InClusterClient:   client,
				ProxyConfig: NodeClientProxyConfig{
					ForceKubeProxy: true,
				},
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  server.URL,
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(1))
			Expect(data[0].Node.NodeName).To(Equal("test-node"))
		})
	})

	Context("with a local proxy", func() {
		It("uses the local proxy URL instead of the cluster host", func() {
			var requestCount int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requestCount, 1)
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			node := newTestNode("local-proxy-node", "10.0.0.5", 10250)
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 2,
				DirectNodeClient:  Client{client: &failingHTTPClient{}, retries: 0},
				InClusterClient:   client,
				ProxyConfig: NodeClientProxyConfig{
					LocalProxy:     server.URL,
					ForceKubeProxy: true,
				},
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  "https://should-not-be-used:6443",
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			// With local proxy, bearer token is not read
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(1))
			Expect(atomic.LoadInt32(&requestCount)).To(BeNumerically(">=", 1))
		})
	})

	Context("concurrent node collection", func() {
		It("collects data from multiple nodes concurrently via proxy", func() {
			var requestCount int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&requestCount, 1)
				// Simulate small delay
				time.Sleep(10 * time.Millisecond)
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			testNodes := make([]*v1.Node, 5)
			for i := range 5 {
				testNodes[i] = newTestNode(
					fmt.Sprintf("node-%d", i),
					fmt.Sprintf("10.0.0.%d", i+1),
					10250,
				)
			}
			cache := &singleNodeCache{nodes: testNodes}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 3, // limit concurrency to 3
				InClusterClient:   client,
				ProxyConfig: NodeClientProxyConfig{
					ForceKubeProxy: true,
				},
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  server.URL,
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(5))
			Expect(atomic.LoadInt32(&requestCount)).To(BeNumerically("==", 5))
		})
	})

	Context("partial failure handling", func() {
		It("returns partial results and errors when some nodes fail", func() {
			callCount := int32(0)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := atomic.AddInt32(&callCount, 1)
				if count%2 == 0 {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			testNodes := make([]*v1.Node, 4)
			for i := range 4 {
				testNodes[i] = newTestNode(
					fmt.Sprintf("node-%d", i),
					fmt.Sprintf("10.0.0.%d", i+1),
					10250,
				)
			}
			cache := &singleNodeCache{nodes: testNodes}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 4,
				InClusterClient:   client,
				ProxyConfig: NodeClientProxyConfig{
					ForceKubeProxy: true,
				},
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  server.URL,
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			// Some succeed, some fail
			Expect(data).To(HaveLen(2))
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeUnwrappableErrorWith(HaveLen(2)))
		})
	})

	Context("Fargate node detection", func() {
		It("skips direct connection for Fargate nodes and uses proxy", func() {
			var proxyHit bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Proxy path includes /api/v1/nodes/<name>/proxy/
				Expect(r.URL.Path).To(ContainSubstring("/api/v1/nodes/fargate-node/proxy/"))
				proxyHit = true
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			node := newTestNode("fargate-node", "10.0.0.1", 10250)
			node.Labels["eks.amazonaws.com/compute-type"] = "fargate"
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 2,
				DirectNodeClient:  client,
				InClusterClient:   client,
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  server.URL,
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(1))
			Expect(proxyHit).To(BeTrue(), "Proxy endpoint should be used for Fargate nodes")
		})
	})

	Context("bearer token handling", func() {
		It("fails when bearer token file does not exist", func() {
			node := newTestNode("token-node", "10.0.0.1", 10250)
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 1,
				DirectNodeClient:  NewClient(&http.Client{}, 0),
				InClusterClient:   NewClient(&http.Client{}, 0),
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  "https://localhost:6443",
				bearerTokenFile: "/nonexistent/path/token",
			}

			data, err := nssc.GetNodeData()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("could not read bearer token"))
			Expect(data).To(BeNil())
		})

		It("sends bearer token in Authorization header", func() {
			var receivedAuth string
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedAuth = r.Header.Get("Authorization")
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			node := newTestNode("auth-node", "10.0.0.1", 10250)
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 1,
				DirectNodeClient:  Client{client: &failingHTTPClient{}, retries: 0},
				InClusterClient:   client,
				ProxyConfig:       NodeClientProxyConfig{ForceKubeProxy: true},
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  server.URL,
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(data).To(HaveLen(1))
			Expect(receivedAuth).To(Equal("bearer test-token"))
		})
	})

	Context("empty cluster", func() {
		It("returns nil when no ready nodes exist", func() {
			cache := &singleNodeCache{nodes: []*v1.Node{}}

			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 1,
			}

			nssc := &NodeStatsSummaryClient{
				config:          config,
				cache:           cache,
				endpoint:        "stats/summary",
				clusterHostUrl:  "https://localhost:6443",
				bearerTokenFile: tempBearerFile,
			}

			data, err := nssc.GetNodeData()
			Expect(err).To(BeNil())
			Expect(data).To(BeNil())
		})
	})
})

// -----------------------------------------------------------------------
// Integration: NodeStatsSummaryProvider (background collection)
// -----------------------------------------------------------------------

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

		// Wait for background refresh
		time.Sleep(250 * time.Millisecond)

		data, err = provider.GetNodeData()
		Expect(err).ToNot(HaveOccurred())
		Expect(data).To(HaveLen(1))
		// Should have been refreshed at least once more
		Expect(atomic.LoadInt32(&callCount)).To(BeNumerically(">=", 2))

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
					// First call succeeds
					return []*stats.Summary{
						{Node: stats.NodeStats{NodeName: "good-node"}},
					}, nil
				}
				// Subsequent calls fail completely
				return nil, fmt.Errorf("all nodes unreachable")
			},
		}

		provider := NewNodeStatsSummaryProvider(mockClient)
		started := provider.Start(50 * time.Millisecond)
		Expect(started).To(BeTrue())

		// Wait for a failed refresh
		time.Sleep(150 * time.Millisecond)

		// Data should still be from the first successful call
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
		started := provider.Start(time.Hour) // long interval so it doesn't retry
		Expect(started).To(BeTrue())

		// The initial call failed, so GetNodeData should return an error
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

// -----------------------------------------------------------------------
// Integration: NodeAddress helper
// -----------------------------------------------------------------------

var _ = Describe("NodeAddress", func() {
	It("returns IP and port for a node with InternalIP", func() {
		node := newTestNode("addr-node", "192.168.1.50", 10250)
		ip, port, err := NodeAddress(node)
		Expect(err).ToNot(HaveOccurred())
		Expect(ip).To(Equal("192.168.1.50"))
		Expect(port).To(Equal(int32(10250)))
	})

	It("returns error for a node without InternalIP", func() {
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "no-ip-node"},
			Status: v1.NodeStatus{
				Addresses: []v1.NodeAddress{
					{Type: v1.NodeExternalIP, Address: "1.2.3.4"},
				},
			},
		}
		_, _, err := NodeAddress(node)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("could not find internal IP"))
	})
})

// -----------------------------------------------------------------------
// Integration: connection options logic
// -----------------------------------------------------------------------

var _ = Describe("connectionOptions", func() {
	It("returns both direct and proxy methods by default", func() {
		node := *newTestNode("test-node", "10.0.0.1", 10250)
		config := NodeClientConfig{
			DirectNodeClient: NewClient(&http.Client{}, 0),
			InClusterClient:  NewClient(&http.Client{}, 0),
		}

		nd := nodeFetchData{nodeName: "test-node", ClusterHostURL: "https://k8s:6443"}
		methods := config.connectionOptions(node, nd)
		Expect(methods).To(HaveLen(2))
	})

	It("returns only proxy when ForceKubeProxy is true", func() {
		node := *newTestNode("test-node", "10.0.0.1", 10250)
		config := NodeClientConfig{
			DirectNodeClient: NewClient(&http.Client{}, 0),
			InClusterClient:  NewClient(&http.Client{}, 0),
			ProxyConfig:      NodeClientProxyConfig{ForceKubeProxy: true},
		}

		nd := nodeFetchData{nodeName: "test-node", ClusterHostURL: "https://k8s:6443"}
		methods := config.connectionOptions(node, nd)
		Expect(methods).To(HaveLen(1))
	})

	It("returns only proxy for Fargate nodes", func() {
		node := *newTestNode("fargate", "10.0.0.1", 10250)
		node.Labels["eks.amazonaws.com/compute-type"] = "fargate"
		config := NodeClientConfig{
			DirectNodeClient: NewClient(&http.Client{}, 0),
			InClusterClient:  NewClient(&http.Client{}, 0),
		}

		nd := nodeFetchData{nodeName: "fargate", ClusterHostURL: "https://k8s:6443"}
		methods := config.connectionOptions(node, nd)
		Expect(methods).To(HaveLen(1))
	})
})

// -----------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------

// singleNodeCache is a minimal ClusterCache returning a fixed list of nodes
type singleNodeCache struct {
	cluster.ClusterCache
	nodes []*v1.Node
}

func (s *singleNodeCache) GetAllNodes() []*v1.Node {
	return s.nodes
}

// failingHTTPClient always returns an error
type failingHTTPClient struct{}

func (f *failingHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("connection refused")
}

// mockStatsSummaryClient allows injecting a custom function for GetNodeData
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

// hostPort extracts host and port from a URL like "https://127.0.0.1:12345"
func hostPort(url string) (string, int32) {
	// strip scheme
	addr := strings.TrimPrefix(url, "https://")
	addr = strings.TrimPrefix(addr, "http://")
	host, portStr, _ := net.SplitHostPort(addr)
	var port int32
	fmt.Sscanf(portStr, "%d", &port)
	return host, port
}

