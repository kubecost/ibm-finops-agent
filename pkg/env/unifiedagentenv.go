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
	MinuteMetricsEnabledEnvVar = "MINUTE_METRICS_ENABLED"
	PromlessEnvVar             = "PROMLESS"

	// Node Stats Client Configuration (can be prefixed)
	NodeStatsForceKubeProxyEnvVar    = "FORCE_KUBE_PROXY"
	NodeStatsLocalProxyEnvVar        = "LOCAL_PROXY"
	NodeStatsConcurrentPollersEnvVar = "NUMBER_OF_CONCURRENT_NODE_POLLERS"
	NodeStatsInsecureEnvVar          = "INSECURE"
	NodeStatsCertFileEnvVar          = "CERT_FILE"
	NodeStatsKeyFileEnvVar           = "KEY_FILE"

	// Name and ID represent the same identifier for the cluster
	NodeStatsClusterNameEnvVar = "CLUSTER_NAME"
	NodeStatsClusterIDEnvVar   = "CLUSTER_ID"

	// InformerResyncIntervalEnvVar is the resync interval for informers
	InformerResyncIntervalEnvVar = "INFORMER_RESYNC_INTERVAL"

	// ParseMetricDataEnvVar env var for sanitizing k8s resources
	ParseMetricDataEnvVar = "PARSE_METRIC_DATA"

	// Log Export Configuration
	LogExportEnabledEnvVar      = "LOG_EXPORT_ENABLED"
	LogExportIntervalEnvVar     = "LOG_EXPORT_INTERVAL"
	LogExportBufferSizeEnvVar   = "LOG_EXPORT_BUFFER_SIZE"
	LogExportPathPrefixEnvVar   = "LOG_EXPORT_PATH_PREFIX"
	LogExportDirPathEnvVar      = "LOG_EXPORT_DIR_PATH"
	LogExportSyncIntervalEnvVar = "LOG_EXPORT_SYNC_INTERVAL"

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

// IsPromless returns true if the agent should run without a dependency on Prometheus. This
// is the default mode of operation.
func IsPromless() bool {
	return env.GetBool(PromlessEnvVar, true)
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

// IsLogExportEnabled returns true if log export to federated storage is enabled
func IsLogExportEnabled() bool {
	return env.GetBool(LogExportEnabledEnvVar, false)
}

// GetLogExportInterval returns the configured interval for log uploads
func GetLogExportInterval() time.Duration {
	return env.GetDuration(LogExportIntervalEnvVar, 30*time.Minute)
}

// GetLogExportBufferSize returns the maximum buffer size in bytes before forced upload
func GetLogExportBufferSize() int64 {
	return env.GetInt64(LogExportBufferSizeEnvVar, 5*1024*1024) // Default 5MB
}

// GetLogExportPathPrefix returns the path prefix for log files in bucket storage
func GetLogExportPathPrefix() string {
	return env.Get(LogExportPathPrefixEnvVar, "finops-agent-logs")
}

// GetLogExportDirPath returns the directory path where log files will be stored
func GetLogExportDirPath() string {
	return env.Get(LogExportDirPathEnvVar, "/opt/finops-agent/logs")
}

// GetLogExportSyncInterval returns how often to sync log file to disk (default 5s). Stop() always syncs on shutdown.
func GetLogExportSyncInterval() time.Duration {
	return env.GetDuration(LogExportSyncIntervalEnvVar, 5*time.Second)
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
