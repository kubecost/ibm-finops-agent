package nodes

import (
	"encoding/json"
	"errors"
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

// StalenessProvider is optionally implemented by a StatSummaryClient that can report
// how long since the last successful collection.
type StalenessProvider interface {
	SecondsSinceLastSuccess() float64
	HasEverSucceeded() bool
	LastCollectionResults() []NodeCollectionResult
}

type StatSummaryClient interface {
	GetNodeData() ([]*stats.Summary, []NodeCollectionResult, error)
}

type NodeStatsSummaryClient struct {
	config          NodeClientConfig
	cache           cluster.ClusterCache
	endpoint        string
	clusterHostUrl  string
	bearerTokenFile string
}

func NewNodeStatsSummaryClient(cache cluster.ClusterCache, config NodeClientConfig, inClusterConfig *rest.Config) *NodeStatsSummaryClient {
	return &NodeStatsSummaryClient{
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
func (nssc *NodeStatsSummaryClient) GetNodeData() ([]*stats.Summary, []NodeCollectionResult, error) {
	var nodes []*v1.Node
	var statsList []*stats.Summary

	var bearerToken string
	if !nssc.config.ProxyConfig.IsLocalProxy() {
		token, err := nssc.getBearerToken()
		if err != nil {
			return nil, nil, err
		}
		bearerToken = token
	}

	nodes = getReadyNodes(nssc.cache)

	var wg sync.WaitGroup
	var m sync.Mutex

	var resultsLock sync.Mutex
	var results []NodeCollectionResult

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

			nd := nodeFetchData{
				nodeName:       currentNode.Name,
				ClusterHostURL: nssc.clusterHostUrl,
			}
			connectionMethods := nssc.config.connectionOptions(currentNode, nd)

			methodDesc := describeConnectionMethods(connectionMethods)
			resp, err := retrieveNodeData(nssc.endpoint, connectionMethods, bearerToken)
			if err != nil {
				resultsLock.Lock()
				results = append(results, NodeCollectionResult{
					NodeName: currentNode.Name,
					Method:   methodDesc,
					Success:  false,
					Error:    fmt.Errorf("error retrieving node data: %w", err),
				})
				resultsLock.Unlock()
			} else {
				data, err := nodeResponseToStatSummary(resp)
				if err != nil {
					resultsLock.Lock()
					results = append(results, NodeCollectionResult{
						NodeName: currentNode.Name,
						Method:   methodDesc,
						Success:  false,
						Error:    fmt.Errorf("error converting node data: %w", err),
					})
					resultsLock.Unlock()
				} else {
					m.Lock()
					statsList = append(statsList, data)
					m.Unlock()
					resultsLock.Lock()
					results = append(results, NodeCollectionResult{
						NodeName: currentNode.Name,
						Method:   methodDesc,
						Success:  true,
					})
					resultsLock.Unlock()
				}
			}
		}(*n)
	}

	wg.Wait()

	// derive the aggregate error from results for backward compatibility
	var errs []error
	for _, r := range results {
		if !r.Success && r.Error != nil {
			errs = append(errs, r.Error)
		}
	}

	var err error
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}

	return statsList, results, err
}

// Note: These functions are client-independent and can be reused within another function
// for a different datasource using the same config
type nodeFetchData struct {
	nodeName       string
	ClusterHostURL string
}

// retrieveNodeData fetches summary and container data for the node
func retrieveNodeData(endpoint string, connectionMethods []connectionMethod, bearerToken string) ([]byte, error) {

	// Fail after trying all connections the alloted number of retries
	for _, cm := range connectionMethods {
		data, err := cm.client.AttemptEndPoint(http.MethodGet, cm.API.formatEndpoint(endpoint), bearerToken)
		if err == nil {
			return data, err
		} else {
			log.Debugf("failed to connect to node: %s, error: %s", cm.API.formatEndpoint(endpoint), err)
		}
	}

	return nil, fmt.Errorf("problem getting node address: %v. "+
		"Use DEBUG log level for individual connection method errors", endpoint)
}

// describeConnectionMethods returns a human-readable string describing the methods attempted.
func describeConnectionMethods(methods []connectionMethod) string {
	if len(methods) == 0 {
		return "none"
	}
	names := make([]string, 0, len(methods))
	for _, cm := range methods {
		switch cm.API.(type) {
		case directNode:
			names = append(names, "direct")
		case proxyAPI:
			names = append(names, "proxy")
		default:
			names = append(names, "unknown")
		}
	}
	seen := map[string]bool{}
	var unique []string
	for _, n := range names {
		if !seen[n] {
			seen[n] = true
			unique = append(unique, n)
		}
	}
	if len(unique) == 1 {
		return unique[0]
	}
	result := unique[0]
	for _, u := range unique[1:] {
		result += "+" + u
	}
	return result
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
	return "", 0, fmt.Errorf("could not find internal IP address for node %s ", node.Name)
}

func nodeResponseToStatSummary(resp []byte) (*stats.Summary, error) {
	data := &stats.Summary{}

	err := json.Unmarshal(resp, data)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal response body: %w", err)
	}

	return data, nil
}

// getBearerToken reads the service account token
func (nssc *NodeStatsSummaryClient) getBearerToken() (string, error) {
	token, err := os.ReadFile(nssc.bearerTokenFile)
	if err != nil {
		return "", fmt.Errorf("could not read bearer token from file")
	}
	return string(token), nil
}
