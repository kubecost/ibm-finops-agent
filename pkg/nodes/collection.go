package nodes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/opencost/opencost/core/pkg/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type StatSummaryClient interface {
	GetNodeData() ([]*stats.Summary, error)
}

// ContextStatSummaryClient is a StatSummaryClient whose collection can be bounded by a context.
// StatSummaryClient itself is shared with OpenCost's collector and stays context-free until
// OpenCost takes a context (U-5).
type ContextStatSummaryClient interface {
	StatSummaryClient
	GetNodeDataContext(ctx context.Context) ([]*stats.Summary, error)
}

// DurationObserver receives the duration of each node-stats collection in seconds. A
// prometheus.Histogram satisfies it.
type DurationObserver interface {
	Observe(seconds float64)
}

const (
	// DefaultCollectionTimeout bounds a context-free GetNodeData call when the config sets none.
	DefaultCollectionTimeout = 5 * time.Minute
	// DefaultNodeTimeout bounds the collection from one node across every connection method:
	// a direct attempt plus the API-server-proxy fallback, each bounded by the client timeout,
	// with a margin so the fallback isn't cut short of its own timeout.
	DefaultNodeTimeout = 2*DefaultHttpClientTimeout + 5*time.Second
	// fanoutDeadlineShare is the share of the caller's remaining time the fan-out may use, so
	// partial results are returned before the caller's own deadline.
	fanoutDeadlineShare = 0.9
)

type NodeStatsSummaryClient struct {
	config           NodeClientConfig
	cache            cluster.ClusterCache
	endpoint         string
	clusterHostUrl   string
	bearerTokenFile  string
	durationObserver DurationObserver
}

// defaultDurationObserver is the observer every client starts with (SetDefaultDurationObserver).
var defaultDurationObserver DurationObserver

// SetDefaultDurationObserver sets the observer that every NodeStatsSummaryClient created
// afterwards starts with: the agent's node_stats_duration_seconds histogram. Call it at startup,
// before any client is created.
func SetDefaultDurationObserver(observer DurationObserver) {
	defaultDurationObserver = observer
}

func NewNodeStatsSummaryClient(cache cluster.ClusterCache, config NodeClientConfig, inClusterConfig *rest.Config) *NodeStatsSummaryClient {
	return &NodeStatsSummaryClient{
		config:           config,
		cache:            cache,
		endpoint:         "stats/summary",
		clusterHostUrl:   inClusterConfig.Host,
		bearerTokenFile:  inClusterConfig.BearerTokenFile,
		durationObserver: defaultDurationObserver,
	}
}

// SetDurationObserver sets the observer that receives each collection's duration. It must be
// called before the client is used.
func (nssc *NodeStatsSummaryClient) SetDurationObserver(observer DurationObserver) {
	nssc.durationObserver = observer
}

// Note: Stubbed out cAdvisor client
// func NewNodeCAdvisorClient(cache cluster.ClusterCache, config NodeClientConfig) NodeClient {
// 	return NewNodeCAdvisorClient{
// 		config:   config,
// 		cache:    cache,
// 		endpoint: "metrics/cAdvisor",
// 	}
// }

// GetNodeData collects stats from every ready node, bounded by the config's CollectionTimeout
// (DefaultCollectionTimeout if unset). It adapts GetNodeDataContext to the context-free
// StatSummaryClient interface that OpenCost's collector uses.
func (nssc *NodeStatsSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	timeout := nssc.config.CollectionTimeout
	if timeout <= 0 {
		timeout = DefaultCollectionTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return nssc.GetNodeDataContext(ctx)
}

// GetNodeDataContext creates a number of goroutines that attempt to access a specified endpoint and return the
// corresponding stats data in slice of interfaces which can be converted into a stricter format.
//
// Each node gets at most the config's NodeTimeout (DefaultNodeTimeout if unset). If ctx has a
// deadline, the fan-out stops starting nodes shortly before it and returns what it has; nodes
// that time out or aren't reached are reported in the returned error.
func (nssc *NodeStatsSummaryClient) GetNodeDataContext(ctx context.Context) ([]*stats.Summary, error) {
	var nodes []*v1.Node
	var statsList []*stats.Summary

	start := time.Now()
	if nssc.durationObserver != nil {
		defer func() { nssc.durationObserver.Observe(time.Since(start).Seconds()) }()
	}

	var bearerToken string
	if !nssc.config.ProxyConfig.IsLocalProxy() {
		token, err := nssc.getBearerToken()
		if err != nil {
			return nil, err
		}
		bearerToken = token
	}

	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		budget := time.Duration(float64(time.Until(deadline)) * fanoutDeadlineShare)
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	nodeTimeout := nssc.config.NodeTimeout
	if nodeTimeout <= 0 {
		nodeTimeout = DefaultNodeTimeout
	}

	nodes = getReadyNodes(nssc.cache)

	var wg sync.WaitGroup
	var m sync.Mutex

	var errLock sync.Mutex
	var errs []error

	// creates a max number of concurrent goroutines that are allowed
	limiter := make(chan struct{}, nssc.config.ConcurrentPollers)

	notPolled := 0
	for i, n := range nodes {
		if n == nil {
			continue
		}

		// block if channel is full (limiting number of goroutines), or stop at the deadline
		select {
		case limiter <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			notPolled = len(nodes) - i
			break
		}

		wg.Add(1)
		go func(currentNode v1.Node) {
			defer func() {
				<-limiter
				wg.Done()
			}()

			nodeCtx, cancel := context.WithTimeout(ctx, nodeTimeout)
			defer cancel()

			nd := nodeFetchData{
				nodeName:       currentNode.Name,
				ClusterHostURL: nssc.clusterHostUrl,
			}
			connectionMethods := nssc.config.connectionOptions(currentNode, nd)

			resp, err := retrieveNodeData(nodeCtx, nssc.endpoint, connectionMethods, bearerToken)
			if err != nil {
				errLock.Lock()
				errs = append(errs, fmt.Errorf("error retrieving node data: %w", err))
				errLock.Unlock()
			} else {
				data, err := nodeResponseToStatSummary(resp)
				if err != nil {
					errLock.Lock()
					errs = append(errs, fmt.Errorf("error converting node data: %w", err))
					errLock.Unlock()
				} else {
					m.Lock()
					statsList = append(statsList, data)
					m.Unlock()
				}
			}
		}(*n)
	}

	wg.Wait()

	// no need to lock, as the concurrent collect blocks until all complete
	if notPolled > 0 {
		errs = append(errs, fmt.Errorf("node stats budget exhausted after %s: %d of %d ready nodes not polled this cycle", time.Since(start).Round(time.Millisecond), notPolled, len(nodes)))
	}
	var err error = nil
	if len(errs) > 0 {
		err = errors.Join(errs...)
	}

	return statsList, err
}

// Note: These functions are client-independent and can be reused within another function
// for a different datasource using the same config
type nodeFetchData struct {
	nodeName       string
	ClusterHostURL string
}

// retrieveNodeData fetches summary and container data for the node
func retrieveNodeData(ctx context.Context, endpoint string, connectionMethods []connectionMethod, bearerToken string) ([]byte, error) {

	// Fail after trying all connections the alloted number of retries
	for _, cm := range connectionMethods {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("problem getting node address: %v: %w", endpoint, ctx.Err())
		}
		data, err := cm.client.AttemptEndPoint(ctx, http.MethodGet, cm.API.formatEndpoint(endpoint), bearerToken)
		if err == nil {
			return data, err
		} else {
			log.Debugf("failed to connect to node: %s, error: %s", cm.API.formatEndpoint(endpoint), err)
		}
	}

	return nil, fmt.Errorf("problem getting node address: %v. "+
		"Use DEBUG log level for individual connection method errors", endpoint)
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

// getReadyNodes returns all nodes from a cache whose Ready condition is True. NotReady and Unknown
// nodes are usually unreachable, and polling them costs a full per-node timeout each (F-43).
func getReadyNodes(cache cluster.ClusterCache) []*v1.Node {
	var nodes = cache.GetAllNodes()

	var readyNodes []*v1.Node
	for _, n := range nodes {
		nc := getNodeCondition(&n.Status, v1.NodeReady)
		if nc != nil && nc.Status == v1.ConditionTrue {
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
