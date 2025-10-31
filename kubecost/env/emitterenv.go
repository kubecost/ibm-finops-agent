package env

import (
	"path"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
)

const (
	HeartbeatExportEnabledEnvVar   = "HEARTBEAT_EXPORT_ENABLED"
	DiagnosticsExportEnabledEnvVar = "DIAGNOSTICS_EXPORT_ENABLED"
	MinuteMetricsEnabledEnvVar     = "MINUTE_METRICS_ENABLED"

	AllocationExportIntervalEnvVar     = "ALLOCATION_EXPORT_INTERVAL"
	AssetExportIntervalEnvVar          = "ASSET_EXPORT_INTERVAL"
	NetworkInsightExportIntervalEnvVar = "NETWORK_INSIGHT_EXPORT_INTERVAL"
	KubeModelExportIntervalEnvVar      = "KUBEMODEL_EXPORT_INTERVAL"
	HeartbeatExportIntervalEnvVar      = "HEARTBEAT_EXPORT_INTERVAL"
	DiagnosticsExportIntervalEnvVar    = "DIAGNOSTICS_EXPORT_INTERVAL"
)

// IsMinuteMetricsEnabled returns true if the 10m resolution emitter for kubecost
// is enabled. This is used to emit 10m resolution allocation and asset pipeline data.
func IsMinuteMetricsEnabled() bool {
	return coreenv.GetBool(MinuteMetricsEnabledEnvVar, false)
}

// GetAllocationExportInterval returns the configured interval for allocation exports.
func GetAllocationExportInterval() time.Duration {
	return coreenv.GetDuration(AllocationExportIntervalEnvVar, 10*time.Minute)
}

// GetAssetExportInterval returns the configured interval for asset exports.
func GetAssetExportInterval() time.Duration {
	return coreenv.GetDuration(AssetExportIntervalEnvVar, 10*time.Minute)
}

// GetNetworkInsightExportInterval returns the configured interval for network insight exports.
func GetNetworkInsightExportInterval() time.Duration {
	return coreenv.GetDuration(NetworkInsightExportIntervalEnvVar, 10*time.Minute)
}

// GetKubeModelExportInterval returns the configured interval for allocation exports.
func GetKubeModelExportInterval() time.Duration {
	return coreenv.GetDuration(KubeModelExportIntervalEnvVar, 10*time.Minute)
}

// GetHeartbeatExportInterval returns the configured interval for heartbeat exports.
func GetHeartbeatExportInterval() time.Duration {
	return coreenv.GetDuration(HeartbeatExportIntervalEnvVar, 5*time.Minute)
}

// IsHeartbeatExportEnabled returns true if the heartbeat export is enabled.
func IsHeartbeatExportEnabled() bool {
	return coreenv.GetBool(HeartbeatExportEnabledEnvVar, true)
}

// GetDiagnosticsExportInterval returns the configured interval for diagnostics exports.
func GetDiagnosticsExportInterval() time.Duration {
	return coreenv.GetDuration(DiagnosticsExportIntervalEnvVar, 3*time.Minute)
}

// IsDiagnosticsExportEnabled returns true if the diagnostics export is enabled.
func IsDiagnosticsExportEnabled() bool {
	return coreenv.GetBool(DiagnosticsExportEnabledEnvVar, true)
}

// GetExportBucketConfigFile returns the file path for the export bucket
func GetExportBucketConfigFile() string {
	return path.Join(coreenv.GetConfigPath(), "federated-store.yaml")
}
