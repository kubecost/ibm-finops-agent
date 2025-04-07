package nodes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ibm/finops-agent/pkg/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Node Collection Testing")
}

var _ = Describe("Raw node data", func() {
	Context("Raw stats summary data", func() {
		It("can be downloaded directly and converted into stats summary data", func() {
			summaryClient := setupTestNodeStatSummaryClient("https://localhost", false, 10, false, false)

			rawData, err := summaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(rawData)).To(BeNumerically(">", 0))
			
			statsSummary := ConvertToStatsSummary(rawData)
			Expect(len(statsSummary)).To(BeNumerically(">", 0))
			Expect(statsSummary[0].Node.NodeName).Should(Equal("nodename1"))
		})

		It("can be downloaded through proxy and converted into stats summary data", func() {
			summaryClient := setupTestNodeStatSummaryClient("https://localhost", true, 10, false, false)

			rawData, err := summaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(rawData)).To(BeNumerically(">", 0))
			
			statsSummary := ConvertToStatsSummary(rawData)
			Expect(len(statsSummary)).To(BeNumerically(">", 0))
			Expect(statsSummary[0].Node.NodeName).Should(Equal("nodename2"))
		})

		It("returns nothing on failed http requests", func() {
			summaryClient := setupTestNodeStatSummaryClient("https://localhost", false, 10, false, true)

			rawData, err := summaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(rawData)).To(BeNumerically("==", 0))
			
			statsSummary := ConvertToStatsSummary(rawData)
			Expect(len(statsSummary)).To(BeNumerically("==", 0))
		})
	})

	// TOOD: Add in cAdvisor tests once cAdvisor data struct is implemented
})

func setupTestNodeStatSummaryClient(clusterHostUrl string, forceKubeProxy bool, concurrentPollers int, insecure bool, failRequests bool) NodeClient {
	ncc := NewNodeClientConfig(clusterHostUrl, forceKubeProxy, concurrentPollers, insecure)
	ncc.DirectNodeClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0), failRequests)
	ncc.InClusterClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0), failRequests)
	
	mockCache := NewMockClusterCache()
	return NewNodeStatsSummaryClient(mockCache, ncc)
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

// Alex inquiry: I suspect I have one too many layers of interfaces for this mockHTTPclient but I can't seem to make it work otherwise.
type mockHTTPClient struct {
	FailRequests	bool
}

func NewHTTPMockClient(c Client, failRequests bool) *mockHTTPClient {
	return &mockHTTPClient{
		FailRequests: failRequests,
	}
}

func (m *mockHTTPClient) Do(request *http.Request) (*http.Response, error) {
	data, _ := os.ReadFile("testdata/summary-nodename1.json")
	data2, _ := os.ReadFile("testdata/summary-nodename2.json")

	if m.FailRequests {
		resp := &http.Response{StatusCode: 400, Header: http.Header{}}
		return resp, nil
	}

	if strings.Contains(request.URL.Path, "stats/summary") {
		if strings.Contains(request.URL.Host, "localhost") {
			resp := &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data2)), Header: http.Header{}}
			return resp, nil
		} else {
			resp := &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data)), Header: http.Header{}}
			return resp, nil
		}
	}

	err := fmt.Errorf("no data returned")
	return nil, err
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