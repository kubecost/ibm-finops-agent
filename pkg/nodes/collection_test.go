package nodes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ibm/finops-agent/pkg/cluster"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Node Collection Testing")
}

var _ = Describe("Raw node data", func() {
	Context("Raw stats summary data", func() {
		kac := NewKubeAgentConfig("https://localhost/", false, 10, false)
		kac.DirectNodeClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0))
		kac.InClusterClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0))
		
		mockCache := NewMockClusterCache()
		nodeStatsSummaryClient := NewNodeStatsSummaryClient(mockCache, kac)


		It("can be downloaded and converted into stats summary data", func() {
			rawData, err := nodeStatsSummaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(rawData)).To(BeNumerically(">", 0))
			
			statsSummary := ConvertToStatsSummary(rawData)
			Expect(len(statsSummary)).To(BeNumerically(">", 0))
			Expect(statsSummary[0].Node.NodeName).Should(Equal("nodename1"))
		})
	})

	// TOOD: Add in cAdvisor tests once cAdvisor data struct is implemented
	Context("Raw CAdvisor data", func() {
		kac := NewKubeAgentConfig("https://localhost/", false, 10, false)
		kac.DirectNodeClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0))
		kac.InClusterClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0))
		
		mockCache := NewMockClusterCache()
		nodeStatsSummaryClient := NewNodeCAdvisorClient(mockCache, kac)


		It("can be downloaded and converted into cAdvisor data", func() {
			rawData, err := nodeStatsSummaryClient.GetNodeData()
			Expect(err).ToNot(HaveOccurred())
			Expect(len(rawData)).To(BeNumerically(">", 0))
			
			statsSummary := ConvertToStatsSummary(rawData)
			Expect(len(statsSummary)).To(BeNumerically(">", 0))
			Expect(statsSummary[0].Node.NodeName).Should(Equal("nodename1"))
		})
	})
})


// func TestDownloadNodeData(t *testing.T) {
// 	returnCodes := []int{200, 200, 200, 400, 400, 400, 200, 200, 200, 400}
// 	ts := launchTLSTestServer(returnCodes)
// 	defer ts.Close()

// 	kac := NewKubeAgentConfig("https://" + ts.Listener.Addr().String(), false, 10, false)
// 	kac.DirectNodeClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0))
// 	kac.InClusterClient.HTTPClient = NewHTTPMockClient(NewClient(http.Client{}, 0))
	
// 	mockCache := NewMockClusterCache()
// 	nodeStatsSummaryClient := NewNodeStatsSummaryClient(mockCache, kac)

// 	t.Run("Ensure stats are added to summary list on successful call", func(t *testing.T) {
		
// 		rawData, err := nodeStatsSummaryClient.GetNodeData()
// 		Expect(err).ToNot(HaveOccurred())
// 		Expect(len(rawData)).To(BeNumerically(">", 0))
		
// 		statsSummary := ConvertToStatsSummary(rawData)
// 		Expect(len(statsSummary)).To(BeNumerically(">", 0))
// 		Expect(statsSummary[0].Node.NodeName).Should(Equal("nodename1"))
// 	})
// }

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

// Alex TODO: I suspect I have one too many layers of interfaces for this mockHTTPclient but I can't seem to make it work otherwise.
type mockHTTPClient struct {
}

func NewHTTPMockClient(Client) *mockHTTPClient {
	return &mockHTTPClient{}
}

func (m *mockHTTPClient) Do(request *http.Request) (*http.Response, error) {
	data, err := os.ReadFile("testdata/summary-nodename1.json")
	if err != nil {
		return nil, err
	}

	resp := &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(data)), Header: http.Header{}}
	return resp, nil
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

func loadStats() ([]*stats.Summary, error) {
	var data []*stats.Summary
	for i := 1; i <= 4; i++ {
		file, err := os.Open(fmt.Sprintf("testdata/summary-nodename%d.json", i))
		if err != nil {
			return nil, err
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		obj := &stats.Summary{}
		err = decoder.Decode(obj)
		if err != nil {
			return data, err
		}
		data = append(data, obj)
	}
	return data, nil
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