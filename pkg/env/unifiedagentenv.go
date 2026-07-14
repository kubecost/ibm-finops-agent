package env

import (
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

const (
	// Emitter Configuration
	KubecostEmitterEnabledEnvVar = "KUBECOST_EMITTER_ENABLED"
	CloudyEmitterEnabledEnvVar   = "CLOUDABILITY_EMITTER_ENABLED"
	TurboEmitterEnabledEnvVar    = "TURBO_EMITTER_ENABLED"

	// Exporter Emission Interval
	ExporterEmissionIntervalEnvVar = "EXPORTER_EMISSION_INTERVAL"

	// Go Debug
	PProfEnabledEnvVar = "PPROF_ENABLED"

	// Agent DataSource Configuration
	OpenCostDataSourceEnabledEnvVar = "OPENCOST_SOURCE_ENABLED"

	// Snapshot Configuration
	MinuteMetricsEnabledEnvVar       = "MINUTE_METRICS_ENABLED"
	CollectorDataSourceEnabledEnvVar = "COLLECTOR_DATA_SOURCE_ENABLED"

	// Go Automatic Memory Limit Management for GC Throttling. Enabling this will sample heap usage,
	// and increase the GOMEMLIMIT automatically as heap usage grows and exceeds the current limit.
	// If the GOMEMLIMIT is set manually, the auto-limiter starts at the set value. For more
	// information about the GOMEMLIMIT environment variable: https://go.dev/doc/gc-guide
	AutoMemLimitEnabledEnvVar = "AUTO_MEMLIMIT_ENABLED"

	// Node Stats Client Configuration (can be prefixed)
	NodeStatsForceKubeProxyEnvVar              = "FORCE_KUBE_PROXY"
	NodeStatsLocalProxyEnvVar                  = "LOCAL_PROXY"
	NodeStatsConcurrentPollersEnvVar           = "NUMBER_OF_CONCURRENT_NODE_POLLERS"
	NodeStatsBackgroundCollectionEnabledEnvVar = "NODE_STATS_BG_COLLECTION_ENABLED"
	NodeStatsInsecureEnvVar                    = "INSECURE"
	NodeStatsCertFileEnvVar                    = "CERT_FILE"
	NodeStatsKeyFileEnvVar                     = "KEY_FILE"

	// Name and ID represent the same identifier for the cluster
	NodeStatsClusterNameEnvVar = "CLUSTER_NAME"
	NodeStatsClusterIDEnvVar   = "CLUSTER_ID"

	// InformerResyncIntervalEnvVar is the resync interval for informers
	InformerResyncIntervalEnvVar = "INFORMER_RESYNC_INTERVAL"

	// ParseMetricDataEnvVar env var for sanitizing k8s resources
	ParseMetricDataEnvVar = "PARSE_METRIC_DATA"

	// External.NodeLabels environment variables configure the ConfigMap used to read custom node labels.
	// Set EXTERNAL_NODELABELS_CONFIG_MAP_NAME to enable this feature. The namespace defaults to the
	// agent's namespace if not specified.
	// For block-scalar ConfigMaps, set EXTERNAL_NODELABELS_KEY to the data key containing the YAML
	// document and EXTERNAL_NODELABELS_ROUTE to the path within the YAML document that contains the
	// node labels.
	ExternalNodeLabelsConfigMapNameEnvVar = "EXTERNAL_NODELABELS_CONFIG_MAP_NAME"
	ExternalNodeLabelsNamespaceEnvVar     = "EXTERNAL_NODELABELS_NAMESPACE"
	ExternalNodeLabelsKeyEnvVar           = "EXTERNAL_NODELABELS_KEY"
	ExternalNodeLabelsRouteEnvVar         = "EXTERNAL_NODELABELS_ROUTE"

	// Prefixes for
	CloudabilityPrefix = "CLOUDABILITY_"
)

func IsKubecostEmitterEnabled() bool {
	return env.GetBool(KubecostEmitterEnabledEnvVar, true)
}

func IsCloudyEmitterEnabled() bool {
	// support a legacy env var
	if !env.GetBool("CLOUDY_EMITTER_ENABLED", true) {
		return false
	}

	return env.GetBool(CloudyEmitterEnabledEnvVar, true)
}

func IsTurboEmitterEnabled() bool {
	return env.GetBool(TurboEmitterEnabledEnvVar, true)
}

// GetExporterEmissionInterval returns the interval at which the exporter will snapshot and emit data
// to all the emitters. The default is 1 minute.
func GetExporterEmissionInterval() time.Duration {
	return env.GetDuration(ExporterEmissionIntervalEnvVar, 1*time.Minute)
}

func IsOpenCostDataSourceEnabled() bool {
	return env.GetBool(OpenCostDataSourceEnabledEnvVar, true)
}

func IsPProfEnabled() bool {
	return env.GetBool(PProfEnabledEnvVar, false)
}

// Go Automatic Memory Limit Management for GC Throttling. Enabling this will sample heap usage,
// and increase the GOMEMLIMIT automatically as heap usage grows and exceeds the current limit
// and passes a few statistical criteria. Note that this limit is a _soft_ limit. For more
// information about the GOMEMLIMIT environment variable: https://go.dev/doc/gc-guide
//
// The auto-memory-limiter is a run-time memory statistics collector that maintains a soft memory limit
// designed to adjust the soft limit based on meaningful changes to overall heap allocation, leveraging
// exponential moving average windows, confidence interval gates, breach detection, and cumulative sum
// control chart to detect deviations from the mean.
//
// If the GOMEMLIMIT is set manually, the auto-limiter starts at the set value.
func IsAutoMemLimitEnabled() bool {
	return env.GetBool(AutoMemLimitEnabledEnvVar, false)
}

// IsCollectorDataSourceEnabeled returns the environment variable which enables a source.OpencostDatasource
// which does not use uses Prometheus
func IsCollectorDataSourceEnabled() bool {
	return env.GetBool(CollectorDataSourceEnabledEnvVar, true)
}

// IsMinuteMetricsEnabled returns true if the 10m resolution metrics snapshot
// should be enabled.
func IsMinuteMetricsEnabled() bool {
	return env.GetBool(MinuteMetricsEnabledEnvVar, false)
}

// IsNodeStatsForceKubeProxy returns true if the node stats client should force the kube proxy direct end
// point formatting
func IsNodeStatsForceKubeProxy() bool {
	return getValueWithPotentialPrefixOrDefault(NodeStatsForceKubeProxyEnvVar, CloudabilityPrefix, false, cast.ToBool)
}

// IsNodeStatsBackgroundCollectionEnabled determines if the node stats client should be continually refreshed on an interval
// to reduce latency in the snapshot process. This ensures continuous snapshotting which may result in less accurate
// node stats capture. This should be used with clusters containing 1000+ nodes.
func IsNodeStatsBackgroundCollectionEnabled() bool {
	return getValueWithPotentialPrefixOrDefault(NodeStatsBackgroundCollectionEnabledEnvVar, CloudabilityPrefix, false, cast.ToBool)
}

// GetNodeStatsLocalProxy returns the fully qualified local proxy endpoint for the node stats client IFF the proxyAPI
// is selected.
func GetNodeStatsLocalProxy() string {
	return getValueWithPotentialPrefixOrDefault(NodeStatsLocalProxyEnvVar, CloudabilityPrefix, "", cast.ToString)
}

// GetNodeStatsConcurrentPollers returns the number of concurrent requests to make to the node stats endpoints
func GetNodeStatsConcurrentPollers() int {
	return getValueWithPotentialPrefixOrDefault(NodeStatsConcurrentPollersEnvVar, CloudabilityPrefix, 10, cast.ToInt)
}

// IsNodeStatsInsecure returns true if the node stats client should skip TLS verification
func IsNodeStatsInsecure() bool {
	return getValueWithPotentialPrefixOrDefault(NodeStatsInsecureEnvVar, CloudabilityPrefix, false, cast.ToBool)
}

// GetNodeStatsCertFile returns the path of the cert file
func GetNodeStatsCertFile() string {
	return getValueWithPotentialPrefixOrDefault(NodeStatsCertFileEnvVar, CloudabilityPrefix, "", cast.ToString)
}

// GetNodeStatsKeyFile returns the path of the key file
func GetNodeStatsKeyFile() string {
	return getValueWithPotentialPrefixOrDefault(NodeStatsKeyFileEnvVar, CloudabilityPrefix, "", cast.ToString)
}

// GetNodeStatsClusterIDName returns the id/name of the cluster. It tries to read CLUSTER_ID first, then CLUSTER_NAME
// to unify past cloudability and kubecost environment variables.
func GetNodeStatsClusterIDName() string {
	idName := getValueWithPotentialPrefixOrDefault(NodeStatsClusterIDEnvVar, CloudabilityPrefix, "", cast.ToString)
	if idName == "" {
		idName = getValueWithPotentialPrefixOrDefault(NodeStatsClusterNameEnvVar, CloudabilityPrefix, "", cast.ToString)
	}
	return idName
}

// GetInformerReSyncInterval returns the informer resync interval
func GetInformerReSyncInterval() time.Duration {
	return getValueWithPotentialPrefixOrDefault(InformerResyncIntervalEnvVar, CloudabilityPrefix, 24*time.Hour, cast.ToDuration)
}

// GetSanitizeData returns bool that further sanitizes k8s resources if true
func GetSanitizeData() bool {
	return getValueWithPotentialPrefixOrDefault(ParseMetricDataEnvVar, CloudabilityPrefix, false, cast.ToBool)
}

// getValueWithPotentialPrefixOrDefault attempts to read the environment variable raw and then with the specified prefix,
// converting it to the relevant type if found. Necessary that it doesn't default immediately.
func getValueWithPotentialPrefixOrDefault[T any](envVariable string, prefix string, defaultValue T, convert func(interface{}) T) T {
	var envValue interface{}

	// Attempt without prefix first
	envValue = viper.Get(envVariable)
	if envValue == nil {
		// Attempt with prefix
		envValue = viper.Get(prefix + envVariable)
		if envValue == nil {
			// Set to default value
			envValue = defaultValue
		}
	}

	return convert(envValue)
}

// GetExternalNodeLabelsConfigMapName returns the name of the ConfigMap that contains the external node labels.
func GetExternalNodeLabelsConfigMapName() string {
	return env.Get(ExternalNodeLabelsConfigMapNameEnvVar, "")
}

// GetExternalNodeLabelsNamespace returns the namespace of the external node labels ConfigMap.
// An empty string means the agent's own namespace should be used.
func GetExternalNodeLabelsNamespace() string {
	return env.Get(ExternalNodeLabelsNamespaceEnvVar, "")
}

// GetExternalNodeLabelsKey returns the ConfigMap data key that holds the YAML document
// for block-scalar ConfigMaps. Empty for traditional ConfigMaps.
func GetExternalNodeLabelsKey() string {
	return env.Get(ExternalNodeLabelsKeyEnvVar, "")
}

// GetExternalNodeLabelsRoute returns the dot-separated path to the labels map within
// the parsed YAML document. Empty for traditional ConfigMaps.
func GetExternalNodeLabelsRoute() string {
	return env.Get(ExternalNodeLabelsRouteEnvVar, "")
}
