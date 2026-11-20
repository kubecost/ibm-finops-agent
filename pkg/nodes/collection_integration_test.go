package nodes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

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

			host, port := hostPort(server.URL)
			node := newTestNode("ipv4-node", host, port)
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
			Expect(data[0].Node.NodeName).To(Equal("test-node"))
		})

		It("collects node stats via proxy endpoint", func() {
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.URL.Path).To(Equal("/api/v1/nodes/proxy-node/proxy/stats/summary"))
				w.Header().Set("Content-Type", "application/json")
				w.Write(summaryData)
			}))
			defer server.Close()

			node := newTestNode("proxy-node", "192.168.1.100", 10250)
			cache := &singleNodeCache{nodes: []*v1.Node{node}}

			client := NewClient(server.Client(), 0)
			config := NodeClientConfig{
				ClusterName:       "test-cluster",
				ConcurrentPollers: 2,
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
				ConcurrentPollers: 3,
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
			Expect(data).To(HaveLen(2))
			Expect(err).To(HaveOccurred())
			Expect(err).To(BeUnwrappableErrorWith(HaveLen(2)))
		})
	})

	Context("Fargate node detection", func() {
		It("skips direct connection for Fargate nodes and uses proxy", func() {
			var proxyHit bool
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
