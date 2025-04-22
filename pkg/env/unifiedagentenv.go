package env

import "github.com/opencost/opencost/core/pkg/env"

const (
	// Emitter Configuration
	KubecostEmitterEnabledEnvVar = "KUBECOST_EMITTER_ENABLED"
	CloudyEmitterEnabledEnvVar   = "CLOUDY_EMITTER_ENABLED"
	TurboEmitterEnabledEnvVar    = "TURBO_EMITTER_ENABLED"

	// Agent DataSource Configuration
	OpenCostDataSourceEnabledEnvVar = "OPENCOST_SOURCE_ENABLED"

	// Snapshot Configuration
	MinuteMetricsEnabledEnvVar = "MINUTE_METRICS_ENABLED"

	// Node Stats Client Configuration
	NodeStatsForceKubeProxyEnvVar     = "NODE_STATS_FORCE_KUBE_PROXY"
	NodeStatsLocalProxyEnvVar         = "NODE_STATS_LOCAL_PROXY"
	NodeStatsConcurrentPollersEnvVar  = "NODE_STATS_CONCURRENT_POLLERS"
	NodeStatsInsecureSkipVerifyEnvVar = "NODE_STATS_INSECURE_SKIP_VERIFY"
)

func IsKubecostEmitterEnabled() bool {
	return env.GetBool(KubecostEmitterEnabledEnvVar, true)
}

func IsCloudyEmitterEnabled() bool {
	return env.GetBool(CloudyEmitterEnabledEnvVar, true)
}

func IsTurboEmitterEnabled() bool {
	return env.GetBool(TurboEmitterEnabledEnvVar, true)
}

func IsOpenCostDataSourceEnabled() bool {
	return env.GetBool(OpenCostDataSourceEnabledEnvVar, true)
}

// IsMinuteMetricsEnabled returns true if the 10m resolution metrics snapshot
// should be enabled.
func IsMinuteMetricsEnabled() bool {
	return env.GetBool(MinuteMetricsEnabledEnvVar, false)
}

// IsNodeStatsForceKubeProxy returns true if the node stats client should force the kube proxy direct end
// point formatting
func IsNodeStatsForceKubeProxy() bool {
	return env.GetBool(NodeStatsForceKubeProxyEnvVar, false)
}

// GetNodeStatsLocalProxy returns the fully qualified local proxy endpoint for the node stats client IFF the proxyAPI
// is selected.
func GetNodeStatsLocalProxy() string {
	return env.Get(NodeStatsLocalProxyEnvVar, "")
}

// GetNodeStatsConcurrentPollers returns the number of concurrent requests to make to the node stats endpoints
func GetNodeStatsConcurrentPollers() int {
	return env.GetInt(NodeStatsConcurrentPollersEnvVar, 10)
}

// IsNodeStatsInsecureSkipVerify returns true if the node stats client should skip TLS verification
func IsNodeStatsInsecureSkipVerify() bool {
	return env.GetBool(NodeStatsInsecureSkipVerifyEnvVar, false)
}
