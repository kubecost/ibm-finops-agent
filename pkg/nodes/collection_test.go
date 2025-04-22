package nodes

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"

	"github.com/ibm/finops-agent/pkg/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)

var _ = Describe("Raw node data", func() {
	var tempBearerFile string
	BeforeEach(func() {
		file, err := os.CreateTemp("", "")
		Expect(err).ToNot(HaveOccurred())
		tempBearerFile = file.Name()

		t := GinkgoT()
		t.Setenv("INSECURE", "true")
	})
	AfterEach(func() {
		err := os.RemoveAll(tempBearerFile)
		Expect(err).ToNot(HaveOccurred())
	})
	Context("Raw stats summary data", func() {
		It("can be downloaded directly and converted into stats summary data", func() {
			summaryClient := setupTestNodeStatSummaryClient(tempBearerFile, false, true)

			data, err := summaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(data)).To(BeNumerically(">", 0))
			Expect(data[0].Node.NodeName).Should(Equal("directnode"))
		})

		It("can be downloaded through proxy and converted into stats summary data", func() {
			summaryClient := setupTestNodeStatSummaryClient(tempBearerFile, true, false)

			data, err := summaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(data)).To(BeNumerically(">", 0))
			Expect(data[0].Node.NodeName).Should(Equal("proxynode"))
		})

		It("returns nothing on failed http requests", func() {
			summaryClient := setupTestNodeStatSummaryClient(tempBearerFile, true, true)

			data, err := summaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(data)).To(BeNumerically("==", 0))
		})
	})

	Context("Get all nodes", func() {
		It("can fetch all available nodes from cache", func() {
			mockConfig := NewMockClusterCache()
			mockNcs := NodeStatsSummaryClient{
				NodeClientConfig{},
				mockConfig,
				"",
				"",
				"",
			}

			nodes := getReadyNodes(mockNcs.cache)
			Expect(len(nodes)).To(BeNumerically("==", 4))
			// Note: Nodes.jsonl isn't in any order
			Expect(nodes[0].ObjectMeta.Name).Should(Equal("nodename4"))
		})
	})

	// TOOD: Add in cAdvisor tests once cAdvisor data struct is implemented
})

func setupTestNodeStatSummaryClient(tempBearerFile string, failDirect bool, failProxy bool) NodeStatsSummaryClient {
	ncc := NewNodeClientConfig()
	ncc.DirectNodeClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0), true, failDirect, failProxy)
	ncc.InClusterClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0), false, failDirect, failProxy)
	
	mockCache := NewMockClusterCache()
	mockInClusterConfig := &rest.Config{
		BearerTokenFile: tempBearerFile,
		Host: "testHost",
	}
	return NewNodeStatsSummaryClient(mockCache, ncc, mockInClusterConfig)
}

type mockClusterCache struct {
	cluster.ClusterCache
}

func NewMockClusterCache() (cluster.ClusterCache) {
	return mockClusterCache{}
}

func (m mockClusterCache) GetAllNodes() []*v1.Node {
	nodes, _ := loadNodes()
	return nodes
}

// Note: mockHTTPClient mocks statSummary data specifically, but can be changed later
type mockHTTPClient struct {
	isDirect		bool
	failDirect		bool
	failProxy		bool
}

func NewHTTPMockClient(c Client, isDirect bool, failDirect bool, failProxy bool) *mockHTTPClient {
	return &mockHTTPClient{
		isDirect:	isDirect,
		failDirect: failDirect,
		failProxy: 	failProxy,
	}
}

func (m *mockHTTPClient) Do(request *http.Request) (*http.Response, error) {
	proxyData, _ := os.ReadFile("testdata/summary-proxynode.json")
	directData, _ := os.ReadFile("testdata/summary-directnode.json")

	if m.isDirect {
		if !m.failDirect {
			resp := &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(directData)), Header: http.Header{}}
			return resp, nil
		} else {
			resp := &http.Response{StatusCode: 400, Header: http.Header{}}
			return resp, nil
		}
	} else {
		if !m.failProxy {
			resp := &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(proxyData)), Header: http.Header{}}
			return resp, nil
		} else {
			resp := &http.Response{StatusCode: 400, Header: http.Header{}}
			return resp, nil
		}
	}
}

func loadNodes() ([]*v1.Node, error) {
	file, err := os.Open("testdata/nodes.jsonl")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var nodes []*v1.Node
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var node v1.Node
		err := decoder.Decode(&node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &node)
	}
	return nodes, nil
}