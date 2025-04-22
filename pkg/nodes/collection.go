package nodes

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/opencost/opencost/core/pkg/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type StatSummaryClient interface {
	GetNodeData() ([]*stats.Summary, error)
}

type NodeStatsSummaryClient struct {
	config          NodeClientConfig
	cache           cluster.ClusterCache
	endpoint        string
	clusterHostUrl  string
	bearerTokenFile string
}

func NewNodeStatsSummaryClient(cache cluster.ClusterCache, config NodeClientConfig, inClusterConfig *rest.Config) NodeStatsSummaryClient {
	return NodeStatsSummaryClient{
		config:          config,
		cache:           cache,
		endpoint:        "stats/summary",
		clusterHostUrl:  inClusterConfig.Host,
		bearerTokenFile: inClusterConfig.BearerTokenFile,
	}
}

// Note: Stubbed out cAdvisor client
// func NewNodeCAdvisorClient(cache cluster.ClusterCache, config NodeClientConfig) NodeClient {
// 	return NewNodeCAdvisorClient{
// 		config:   config,
// 		cache:    cache,
// 		endpoint: "metrics/cAdvisor",
// 	}
// }

// GetNodeData creates a number of goroutines that attempt to access a specified endpoint and return the
// corresponding stats data in slice of interfaces which can be converted into a stricter format.
func (nssc NodeStatsSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	var nodes []*v1.Node
	var statsList []*stats.Summary

	bearerToken, err := nssc.getBearerToken()
	if err != nil {
		return nil, err
	}
	
	nodes = getReadyNodes(nssc.cache)

	var wg sync.WaitGroup
	var m sync.Mutex

	// creates a max number of concurrent goroutines that are allowed
	limiter := make(chan struct{}, nssc.config.ConcurrentPollers)

	for _, n := range nodes {
		if n == nil {
			continue
		}

		// block if channel is full (limiting number of goroutines)
		limiter <- struct{}{}

		wg.Add(1)
		go func(currentNode v1.Node) {
			defer func() {
				<-limiter
				wg.Done()
			}()

			if currentNode.Spec.ProviderID == "" {
				log.Warnf("node ProviderID not set, skipping collection for %s", currentNode.Name)
				return
			}

			nd := nodeFetchData{
				nodeName:       currentNode.Name,
				ClusterHostURL: nssc.clusterHostUrl,
			}
			connectionMethods := nssc.config.connectionOptions(currentNode, nd)

			resp, err := retrieveNodeData(nd, currentNode, nssc.endpoint, connectionMethods, bearerToken)
			if err != nil {
				log.Warnf("error retrieving node data: %s", err)
			} else {
				data, err := nodeResponseToStatSummary(resp)
				if err != nil {
					log.Warnf("error converting node data: %s", err)
				} else {
					m.Lock()
					statsList = append(statsList, data)
					m.Unlock()
				}
			}
		}(*n)
	}

	wg.Wait()
	return statsList, nil
}

// Note: These functions are client-independent and can be reused within another function
// for a different datasource using the same config
type nodeFetchData struct {
	nodeName       string
	ClusterHostURL string
}

// retrieveNodeData fetches summary and container data for the node
func retrieveNodeData(nd nodeFetchData, n v1.Node, endpoint string, connectionMethods []connectionMethod, bearerToken string) (*http.Response, error) {

	// Fail after trying all connections the alloted number of retries
	for _, cm := range connectionMethods {
		data, err := cm.client.AttemptEndPoint(http.MethodGet, cm.API.formatEndpoint(endpoint), bearerToken)
		if err == nil {
			return data, err
		}
	}

	return nil, fmt.Errorf("problem getting node address: %v", endpoint)
}

// isFargateNode detects if it is a fargate node, disallowing direct connections
func isFargateNode(n v1.Node) bool {
	v := n.Labels["eks.amazonaws.com/compute-type"]
	if v == "fargate" {
		log.Warnf("Fargate node found: %s", n.Name)
		return true
	}
	return false
}

// getReadyNodes returns all nodes from a cache that have the ready status
func getReadyNodes(cache cluster.ClusterCache) []*v1.Node {
	var nodes = cache.GetAllNodes()

	var readyNodes []*v1.Node
	for _, n := range nodes {
		nc := getNodeCondition(&n.Status, v1.NodeReady)
		if nc != nil && nc.Type == v1.NodeReady {
			readyNodes = append(readyNodes, n)
		}
	}

	if len(readyNodes) == 0 {
		log.Warnf("no ready nodes were found")
		return nil
	}

	numReadyNodes := len(readyNodes)
	numTotalNodes := len(nodes)
	if numReadyNodes != numTotalNodes {
		log.Warnf("%v out of %v were in a not ready state when retrieving nodes", numTotalNodes-numReadyNodes, numTotalNodes)
	}

	return readyNodes
}

// getNodeCondition extracts the provided condition from the given status and returns that, nil if not present.
func getNodeCondition(status *v1.NodeStatus, conditionType v1.NodeConditionType) *v1.NodeCondition {
	if status == nil {
		return nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return &status.Conditions[i]
		}
	}
	return nil
}

// NodeAddress returns the internal IP address and kubelet port of a given node
func NodeAddress(node *v1.Node) (string, int32, error) {
	// adapted from k8s.io/kubernetes/pkg/util/node
	for _, addr := range node.Status.Addresses {
		if addr.Type == v1.NodeInternalIP {
			return addr.Address, node.Status.DaemonEndpoints.KubeletEndpoint.Port, nil
		}
	}
	return "", 0, fmt.Errorf("Could not find internal IP address for node %s ", node.Name)
}

func nodeResponseToStatSummary(resp *http.Response) (*stats.Summary, error) {
	data := &stats.Summary{}
	err := json.NewDecoder(resp.Body).Decode(&data)
	if err == nil {
		return data, nil
	}

	return nil, err
}

// getBearerToken reads the service account token
func (nssc NodeStatsSummaryClient) getBearerToken() (string, error) {
	token, err := os.ReadFile(nssc.bearerTokenFile)
	if err != nil {
		return "", fmt.Errorf("could not read bearer token from file")
	}
	return string(token), nil
}
