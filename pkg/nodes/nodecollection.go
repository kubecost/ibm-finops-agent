package nodes

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type NodeClient interface {
	GetNodeData() ([]*stats.Summary, error)
}

type NodeClientSource struct {
	config   KubeAgentConfig
	cache    cluster.ClusterCache
	endpoint string
	name     string
}

func NewNodeStatsSummaryClient(cache cluster.ClusterCache, config KubeAgentConfig) NodeClient {
	return NodeClientSource{
		config:   config,
		cache:    cache,
		endpoint: "/stats/summary",
		name:     "statsSummary",
	}
}

// Alex TODO: Implement cAdvisor marshaling
func NewNodeCAdvisorClient(cache cluster.ClusterCache, config KubeAgentConfig) NodeClient {
	return NodeClientSource{
		config:   config,
		cache:    cache,
		endpoint: "/metrics/cAdvisor",
		name:     "cAdvisor",
	}
}

func (ncs NodeClientSource) GetNodeData() ([]*stats.Summary, error) {
	var nodes []*v1.Node
	var statsList []*stats.Summary

	err := retry.RetryOnConflict(retry.DefaultBackoff, func() (err error) {
		nodes = getReadyNodes(ncs)
		return
	})
	if err != nil {
		return nil, fmt.Errorf("cloudability metric agent is unable to get a list of nodes: %v", err)
	}

	// log.Debugln("Starting node collection loop")

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
				// errMessage := "Node ProviderID is not set which may be because the node is running in a " +
				// 	"self managed environment, and this may cause inconsistent gathering of metrics data."
				// log.Printf(errMessage)
				// Alex TODO: Should this be a case which doesn't pull properly?
			}

			nd := nodeFetchData{
				nodeName: currentNode.Name,
				ClusterHostURL: ncs.config.ClusterHostURL,
			}

			data, err := retrieveNodeData(nd, ncs, currentNode)
			if err != nil {
				// Alex TODO: Throw warning
			} else {
				m.Lock()
				statsList = append(statsList, data)
				m.Unlock()
			}
			<-limiter
			wg.Done()
		}(*n)
	}

	// log.Debugln("Currently Waiting for all node data to be gathered")
	wg.Wait()
	// log.Debugln("All nodes data has been gathered, no longer waiting")

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
	// The config shouldn't allow direct connection if Fargate nodes were
	// found in the cluster at startup, but check again here to be safe. ALEX NOTE: Maybe this part about the double-check is no longer true
	if !ncs.config.ForceKubeProxy && !isFargateNode(n) {
		directAPI, err := setupDirectNodeAPI(&n)
		if err != nil {
			// log.Debugf("Unable to attempt direct connection to node %s: %v", nd.nodeName, err)
		} else {
			connectionMethods = append(connectionMethods, connectionMethod{directAPI, ncs.config.DirectNodeClient})
		}
	}
	proxyAPI := setupProxyAPI(ncs.config.ClusterHostURL, nd.nodeName)
	connectionMethods = append(connectionMethods, connectionMethod{proxyAPI, ncs.config.InClusterClient})
	return connectionMethods
}

// retrieveNodeData fetches summary and container data for the node
func retrieveNodeData(nd nodeFetchData, ncs NodeClientSource, n v1.Node) (*stats.Summary, error) {
	connectionMethods := connectionOptions(ncs, n, nd)

	// if we receive an error after the max number of retries when attempting to hit an endpoint that
	// we had previously verified to work, we fail and assume the node is unreachable at this time
	// ALEX NOTE: Removed the validation step. Considering reimplementing
	for _, cm := range connectionMethods {
		// ALEX TODO: Change the name of nc.name... probably
		data, err := cm.client.CycleEndPoint(http.MethodGet, ncs.name, cm.API.formatEndpoint(ncs.endpoint))
		if err != nil {
			// Alex TODO: Error message
		} else {
			return data, err
		}
	}

	err := fmt.Errorf("problem getting node address:")
	return nil, err
}

// isFargateNode detects whether a node is a Fargate node, which affects
// how the agent will connect to it
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
		} else {
			// log.Debugf("node, %s, is in a notready state. Node Condition: %+v", n.Name, nc)
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

// Alex TODO: Check if this should be split off into another section
type connectionMethod struct {
	API    nodeAPI
	client Client
}

type nodeAPI interface {
	formatEndpoint(s string) string
}

type directNode struct {
	ip   string
	port int64
}

func (d directNode) formatEndpoint(s string) string {
	return fmt.Sprintf("https://%s:%v/%s", d.ip, d.port, s)
}

// setupDirectNodeAPI retrieves node stats directly from the node api
func setupDirectNodeAPI(n *v1.Node) (directNode, error) {
	ip, port, err := NodeAddress(n)
	if err != nil {
		return directNode{}, fmt.Errorf("problem getting node address: %s", err)
	}
	return directNodeEndpoint(ip, port), nil
}

func directNodeEndpoint(ip string, port int32) directNode {
	return directNode{
		ip:   ip,
		port: int64(port),
	}
}

type proxyAPI struct {
	clusterHostURL string
	nodeName       string
}

func (p proxyAPI) formatEndpoint(s string) string {
	return fmt.Sprintf("%s/api/v1/nodes/%s/proxy/%s", p.clusterHostURL, p.nodeName, s)
}

func setupProxyAPI(clusterHostURL, nodeName string) proxyAPI {
	return proxyAPI{
		clusterHostURL: clusterHostURL,
		nodeName:       nodeName,
	}
}

func NewKubeAgentConfig(clusterHostURL string, forceKubeProxy bool, concurrentPollers int, insecure bool) KubeAgentConfig {	
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: insecure,
		},
	}

	return KubeAgentConfig{
		ClusterHostURL: clusterHostURL,
		ForceKubeProxy: forceKubeProxy,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient: NewClient(http.Client{Transport: transport}, 0),
		InClusterClient: NewClient(http.Client{Transport: transport}, 0),
	}
}

// Alex TODO: Check if this should be split off into another section
type KubeAgentConfig struct {
	ClusterHostURL     string
	ForceKubeProxy     bool
	ConcurrentPollers  int
	DirectNodeClient   Client
	InClusterClient    Client
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client defines an HTTP Client
type Client struct {
	HTTPClient HTTPClient
	retries    uint
}

func NewClient(HTTPClient http.Client, retries uint) Client {
	return Client{
		HTTPClient: &HTTPClient,
		retries:    retries,
	}
}

// Alex TODO: Rename this and check the ability of its cycling
func (c *Client) CycleEndPoint(method string, sourceName string, URL string) (*stats.Summary, error) {
	attempts := c.retries + 1

	for i := uint(0); i < attempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
		}

		data, err := MakeRequest(c, method, URL)
		if err == nil {
			return data, nil
		}
		// if verbose {
		// 	log.Warnf("%v URL: %s -- retrying: %v", err, URL, i+1)
		// }
	}
	// Alex TODO: The requests failed
	err := fmt.Errorf("Request failed")
	return nil, err
}

func MakeRequest(c *Client, method string, URL string) (*stats.Summary, error) {
	request, err := http.NewRequest(method, URL, nil)
	if err != nil {
		// Alex TODO: Error generating request
	}

	resp, err := c.HTTPClient.Do(request)
	if err != nil {
		// Alex TODO: Fix this error
		return nil, errors.New("unable to connect")
	}

	// Alex TODO: Safe close?
	defer resp.Body.Close()

	if !(resp.StatusCode >= 200 && resp.StatusCode <= 299) {
		return nil, fmt.Errorf("invalid response %s", strconv.Itoa(resp.StatusCode))
	}

	// Alex TODO: Get type of return from initial config
	data := &stats.Summary{}
	json.NewDecoder(resp.Body).Decode(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}
