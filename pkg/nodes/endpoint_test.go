package nodes

import (
	"net/http"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
