package nodes

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/ibm/finops-agent/pkg/cluster"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type NodeClient interface {
	GetNodeData() ([]interface{}, error)
}

type NodeClientSource struct {
	config   NodeClientConfig
	cache    cluster.ClusterCache
	endpoint string
	name     string
}

func NewNodeStatsSummaryClient(cache cluster.ClusterCache, config NodeClientConfig) NodeClient {
	return NodeClientSource{
		config:   config,
		cache:    cache,
		endpoint: "stats/summary",
		name:     "statsSummary",
	}
}

func NewNodeCAdvisorClient(cache cluster.ClusterCache, config NodeClientConfig) NodeClient {
	return NodeClientSource{
		config:   config,
		cache:    cache,
		endpoint: "metrics/cAdvisor",
		name:     "cAdvisor",
	}
}

func (ncs NodeClientSource) GetNodeData() ([]interface{}, error) {
	var nodes []*v1.Node
	var statsList []interface{}

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() (err error) {
		nodes = getReadyNodes(ncs)
		return
	})
	if err != nil {
		return nil, fmt.Errorf("cloudability metric agent is unable to get a list of nodes: %v", err)
	}

	var wg sync.WaitGroup
	var m sync.Mutex

	// creates a max number of concurrent goroutines that are allowed
	limiter := make(chan struct{}, ncs.config.ConcurrentPollers)

	for _, n := range nodes {
		// block if channel is full (limiting number of goroutines)
		limiter <- struct{}{}

		wg.Add(1)
		go func(currentNode v1.Node) {
			if currentNode.Spec.ProviderID == "" {
				// errMessage := "node ProviderID is not set which may be because the node is running in a " +
				// 	"self managed environment, and this may cause inconsistent gathering of metrics data."
				// log.Printf(errMessage)
				return
			}

			nd := nodeFetchData{
				nodeName:       currentNode.Name,
				ClusterHostURL: ncs.config.ClusterHostURL,
			}

			data, err := retrieveNodeData(nd, ncs, currentNode)
			if err != nil {

			} else {
				m.Lock()
				statsList = append(statsList, data)
				m.Unlock()
			}
			<-limiter
			wg.Done()
		}(*n)
	}

	wg.Wait()
	return statsList, nil
}

type nodeFetchData struct {
	nodeName       string
	ClusterHostURL string
}

// connectionOptions returns the connection methods that are allowed for this node based on config
// settings and cluster composition
func connectionOptions(ncs NodeClientSource, n v1.Node, nd nodeFetchData) []connectionMethod {
	connectionMethods := make([]connectionMethod, 0)

	// Do not allow direct connection to fargate nodes
	if !ncs.config.ForceKubeProxy && !isFargateNode(n) {
		directAPI, err := setupDirectNodeAPI(&n)
		if err != nil {
			// log.Printf(err.Error())
		} else {
			connectionMethods = append(connectionMethods, connectionMethod{directAPI, ncs.config.DirectNodeClient})
		}
	}
	proxyAPI := setupProxyAPI(ncs.config.ClusterHostURL, nd.nodeName)
	connectionMethods = append(connectionMethods, connectionMethod{proxyAPI, ncs.config.InClusterClient})
	return connectionMethods
}

// retrieveNodeData fetches summary and container data for the node
func retrieveNodeData(nd nodeFetchData, ncs NodeClientSource, n v1.Node) (interface{}, error) {
	connectionMethods := connectionOptions(ncs, n, nd)

	// Fail after trying all connections the alloted number of retries
	for _, cm := range connectionMethods {
		data, err := cm.client.AttemptEndPoint(http.MethodGet, cm.API.formatEndpoint(ncs.endpoint))
		if err == nil {
			return data, err
		}
	}

	err := fmt.Errorf("problem getting node address: %v", ncs.endpoint)
	return nil, err
}

// isFargateNode detects if it is a fargate node, disallowing direct connections
func isFargateNode(n v1.Node) bool {
	v := n.Labels["eks.amazonaws.com/compute-type"]
	if v == "fargate" {
		log.Printf("Fargate node found: %s", n.Name)
		return true
	}
	return false
}

func getReadyNodes(ncs NodeClientSource) []*v1.Node {
	var nodes = ncs.cache.GetAllNodes()

	var readyNodes []*v1.Node
	for _, n := range nodes {
		i, nc := getNodeCondition(
			&n.Status,
			v1.NodeReady)
		if i >= 0 && nc.Type == v1.NodeReady {
			readyNodes = append(readyNodes, n)
		}
	}

	if len(readyNodes) == 0 {
		return nil
	}

	if len(readyNodes) != len(nodes) {
		log.Printf("some nodes were in a not ready state when retrieving nodes")
	}

	return readyNodes
}

// getNodeCondition extracts the provided condition from the given status and returns that.
// Returns nil and -1 if the condition is not present, and the index of the located condition.
// Based on https://github.com/kubernetes/kubernetes/blob/v1.17.3/pkg/controller/util/node/controller_utils.go#L286
func getNodeCondition(status *v1.NodeStatus, conditionType v1.NodeConditionType) (int, *v1.NodeCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
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

func ConvertToStatsSummary(data []interface{}) ([]*stats.Summary, error) {
	var dataList []*stats.Summary

	// Alex inquiry: Should this throw an error on a non-ok? It's already assuming that it's the right format
	// because of the json decoding that's happening
	for _, item := range data {
		statsSum, ok := item.(*stats.Summary)
		if ok {
			dataList = append(dataList, statsSum)
		} else {
			return nil, fmt.Errorf("erorr converting data to stats summary")
		}
	}
	return dataList, nil
}
