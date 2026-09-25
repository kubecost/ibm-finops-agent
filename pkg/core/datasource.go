package core

import (
	"context"
	"fmt"
	"time"

	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/core/opencost"
	"github.com/ibm/finops-agent/pkg/env"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/pkg/cloud/models"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/julienschmidt/httprouter"
)

// NOTE: We can use this as an intermediate data source local to this project. We can defer pushing all the implementation down
// NOTE: into OpenCost until we have a better understanding of the reusability of the opencost code, and/or what it's lacking.
type DataSource interface {
	// OpenCost Data Source
	OpenCostSource() source.OpenCostDataSource

	// OpenCost implementation of the cloud provider's public pricing.
	OpenCostCloudCostProvider() models.Provider

	// Opencost Metrics Query API
	Metrics() source.MetricsQuerier

	// Kubernetes Cluster Informers
	Cluster() cluster.ClusterCache

	// Node Stats Summary Client
	StatsSummary() nodes.StatSummaryClient

	// K8s Version Object
	ClusterMetadata() cluster.Metadata
}

// NewAgentDataSource builds the data source and starts its informers and node-stats collection.
// Cancelling ctx during startup abandons the informer sync wait and the OpenCost retries. Stop
// stops what it started.
func NewAgentDataSource(
	ctx context.Context,
	kubeConfig *rest.Config,
	kubeClientset kubernetes.Interface,
	router *httprouter.Router,
	diag diagnostics.DiagnosticService,
	interval time.Duration,
) (*AgentDataSource, error) {
	discClient, err := discovery.NewDiscoveryClientForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kubernetes discovery client: %w", err)
	}

	// The version is informational (Cloudability's agent-measurement), so a failure isn't fatal.
	versionInfo, err := discClient.ServerVersion()
	if err != nil {
		log.Warnf("Failed to fetch Kubernetes version: %s", err.Error())
	}
	clusterMetadata := cluster.NewClusterMetadata(versionInfo)

	nodeClientConfig, err := nodes.NewNodeClientConfigFromEnv()
	if err != nil {
		return nil, fmt.Errorf("error retrieving node client config: %w", err)
	}

	informerCfg := cluster.LoadInformerConfig()
	// Create Kubernetes Cluster Cache + Watchers
	k8sCache, err := cluster.NewDynamicClusterCache(kubeConfig, informerCfg.ResyncInterval, informerCfg.SanitizeData, interval)
	if err != nil {
		return nil, fmt.Errorf("failed to build Kubernetes client: %w", err)
	}

	// Informers run until Stop. Startup waits for them to sync for at most the sync timeout; any
	// still unsynced keep retrying, and the agent stays up not ready with informers_unsynced
	// rather than hanging or exiting (F-12, D9).
	informerCtx, stopInformers := context.WithCancel(context.Background())
	ads := &AgentDataSource{clusterCache: k8sCache, clusterMetadata: clusterMetadata, stopInformers: stopInformers}
	// The wait for the sync runs in its own goroutine so that cancelling ctx ends startup at
	// once; Stop then cancels informerCtx, which ends the wait.
	synced := make(chan []schema.GroupVersionResource, 1)
	go func() {
		if sr, ok := k8sCache.(cluster.SyncReporter); ok {
			synced <- sr.StartWithTimeout(informerCtx, informerCfg.SyncTimeout)
		} else {
			k8sCache.Start(informerCtx.Done())
			synced <- nil
		}
	}()
	select {
	case unsynced := <-synced:
		if len(unsynced) > 0 {
			log.Errorf("Kubernetes %s after %s; starting anyway, not ready, and they keep retrying",
				cluster.InformersUnsyncedMessage(unsynced), informerCfg.SyncTimeout)
		}
	case <-ctx.Done():
		ads.stopAfterFailedStart()
		return nil, ctx.Err()
	}

	var nodeStatsProvider nodes.StatSummaryClient
	nodeStatsSummaryClient := nodes.NewNodeStatsSummaryClient(k8sCache, nodeClientConfig, kubeConfig)

	// If we use a background service, we leverage the client to refresh node data on an interval
	// otherwise, we retrieve node data _at_ snapshot time
	if nodeClientConfig.BackgroundNodeCollection {
		nodesProvider := nodes.NewNodeStatsSummaryProvider(nodeStatsSummaryClient)
		nodesProvider.Start(nodeClientConfig.RefreshInterval)
		ads.nodesProvider = nodesProvider

		nodeStatsProvider = nodesProvider
	} else {
		// record collection times for node-stats freshness (D1), which the background provider
		// records itself
		nodeStatsProvider = nodes.NewCollectionRecorder(nodeStatsSummaryClient)
	}

	var opencostCloudCostProvider models.Provider
	var opencostSource source.OpenCostDataSource
	if env.IsOpenCostDataSourceEnabled() {
		opencostConf := opencost.NewOpenCostConfigFromEnv()
		opencostSource, opencostCloudCostProvider, err = opencost.NewOpenCostDataSource(ctx, kubeClientset, k8sCache, nodeStatsSummaryClient, router, diag, opencostConf)
		if err != nil {
			ads.stopAfterFailedStart()
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
	} else {
		// fulfill the contract with a no-op opencost datasource
		opencostSource = opencost.NewNoOpOpenCostDataSource()
		opencostCloudCostProvider = nil
	}

	// TODO: Initialization of any other data sources here

	ads.opencostSource = opencostSource
	ads.opencostCloudCostProvider = opencostCloudCostProvider
	ads.metrics = opencostSource.Metrics()
	ads.nodeStatsSummaryClient = nodeStatsProvider
	return ads, nil
}

// AgentDataSource is the agent's DataSource.
type AgentDataSource struct {
	// opencost data source
	opencostSource source.OpenCostDataSource

	// opencost public pricing data for cloud
	opencostCloudCostProvider models.Provider

	// OpenCost Metrics Query API
	metrics source.MetricsQuerier

	// Kubernetes Cluster Informers
	clusterCache cluster.ClusterCache

	// Node Stats Summary Client
	nodeStatsSummaryClient nodes.StatSummaryClient

	// Cluster Metadata
	clusterMetadata cluster.Metadata

	// stopInformers stops the informers, and nodesProvider is the background node-stats
	// collection, if enabled. Stop stops both.
	stopInformers context.CancelFunc
	nodesProvider *nodes.NodeStatsSummaryProvider

	// TODO: HTTP Server/Proxy for Turbo?
}

// abortStopTimeout bounds stopping what NewAgentDataSource had started when it fails.
const abortStopTimeout = 10 * time.Second

// stopAfterFailedStart stops what NewAgentDataSource had started before it failed.
func (ads *AgentDataSource) stopAfterFailedStart() {
	ctx, cancel := context.WithTimeout(context.Background(), abortStopTimeout)
	defer cancel()
	if err := ads.Stop(ctx); err != nil {
		log.Warnf("%s", err)
	}
}

// Stop stops the informers and background node-stats collection and waits for them, bounded by
// ctx. The OpenCost components have no stop API and keep running (U-6).
func (ads *AgentDataSource) Stop(ctx context.Context) error {
	ads.stopInformers()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if ads.nodesProvider != nil {
			ads.nodesProvider.Stop()
		}
		ads.clusterCache.Shutdown()
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("the informers and node-stats collection did not stop in time: %w", ctx.Err())
	}
}

func (ads *AgentDataSource) OpenCostSource() source.OpenCostDataSource {
	return ads.opencostSource
}

func (ads *AgentDataSource) OpenCostCloudCostProvider() models.Provider {
	return ads.opencostCloudCostProvider
}

func (ads *AgentDataSource) Metrics() source.MetricsQuerier {
	return ads.metrics
}

func (ads *AgentDataSource) Cluster() cluster.ClusterCache {
	return ads.clusterCache
}

func (ads *AgentDataSource) StatsSummary() nodes.StatSummaryClient {
	return ads.nodeStatsSummaryClient
}

func (ads *AgentDataSource) ClusterMetadata() cluster.Metadata {
	return ads.clusterMetadata
}
