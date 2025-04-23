package env

import (
	"github.com/opencost/opencost/core/pkg/env"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

const (
	// Emitter Configuration
	KubecostEmitterEnabledEnvVar = "KUBECOST_EMITTER_ENABLED"
	CloudyEmitterEnabledEnvVar   = "CLOUDY_EMITTER_ENABLED"
	TurboEmitterEnabledEnvVar    = "TURBO_EMITTER_ENABLED"

	// Agent DataSource Configuration
	OpenCostDataSourceEnabledEnvVar = "OPENCOST_SOURCE_ENABLED"

	// Snapshot Configuration
	MinuteMetricsEnabledEnvVar = "MINUTE_METRICS_ENABLED"

	// Node Stats Client Configuration (can be prefixed)
	NodeStatsForceKubeProxyEnvVar     = "FORCE_KUBE_PROXY"
	NodeStatsLocalProxyEnvVar         = "LOCAL_PROXY"
	NodeStatsConcurrentPollersEnvVar  = "NUMBER_OF_CONCURRENT_NODE_POLLERS"
	NodeStatsInsecureEnvVar           = "INSECURE"
	NodeStatsCertFileEnvVar           = "CERT_FILE"
	NodeStatsKeyFileEnvVar            = "KEY_FILE"
	NodeStatsClusterNameEnvVar        = "CLUSTER_NAME"

	// Prefixes for 
	CloudabilityPrefix  = "CLOUDABILITY_"
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
	return getValueWithPotentialPrefixOrDefault(NodeStatsForceKubeProxyEnvVar, CloudabilityPrefix, false, cast.ToBool)
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

// Note: This one might have some overlap with kubecost and should have individual logic parsing
// GetNodeStatsClusterName returns the name of the cluster
func GetNodeStatsClusterName() string {
	return getValueWithPotentialPrefixOrDefault(NodeStatsClusterNameEnvVar, CloudabilityPrefix, "", cast.ToString)
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