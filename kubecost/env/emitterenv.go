package env

import (
	"time"

	ocenv "github.com/opencost/opencost/core/pkg/env"
)

const (
	HeartbeatExportEnabledEnvVar   = "HEARTBEAT_EXPORT_ENABLED"
	DiagnosticsExportEnabledEnvVar = "DIAGNOSTICS_EXPORT_ENABLED"
	MinuteMetricsEnabledEnvVar     = "MINUTE_METRICS_ENABLED"

	AllocationExportIntervalEnvVar     = "ALLOCATION_EXPORT_INTERVAL"
	AssetExportIntervalEnvVar          = "ASSET_EXPORT_INTERVAL"
	NetworkInsightExportIntervalEnvVar = "NETWORK_INSIGHT_EXPORT_INTERVAL"
	HeartbeatExportIntervalEnvVar      = "HEARTBEAT_EXPORT_INTERVAL"
	DiagnosticsExportIntervalEnvVar    = "DIAGNOSTICS_EXPORT_INTERVAL"
)

// IsMinuteMetricsEnabled returns true if the 10m resolution emitter for kubecost
// is enabled. This is used to emit 10m resolution allocation and asset pipeline data.
func IsMinuteMetricsEnabled() bool {
	return ocenv.GetBool(MinuteMetricsEnabledEnvVar, false)
}

// GetAllocationExportInterval returns the configured interval for allocation exports.
func GetAllocationExportInterval() time.Duration {
	return ocenv.GetDuration(AllocationExportIntervalEnvVar, 10*time.Minute)
}

// GetAssetExportInterval returns the configured interval for asset exports.
func GetAssetExportInterval() time.Duration {
	return ocenv.GetDuration(AssetExportIntervalEnvVar, 10*time.Minute)
}

// GetNetworkInsightExportInterval returns the configured interval for network insight exports.
func GetNetworkInsightExportInterval() time.Duration {
	return ocenv.GetDuration(NetworkInsightExportIntervalEnvVar, 10*time.Minute)
}

// IsHeartbeatExportEnabled returns true if the heartbeat export is enabled.
func IsHeartbeatExportEnabled() bool {
	return ocenv.GetBool(HeartbeatExportEnabledEnvVar, true)
}

// GetHeartbeatExportInterval returns the configured interval for heartbeat exports.
func GetHeartbeatExportInterval() time.Duration {
	return ocenv.GetDuration(HeartbeatExportIntervalEnvVar, 5*time.Minute)
}

// IsDiagnosticsExportEnabled returns true if the diagnostics export is enabled.
func IsDiagnosticsExportEnabled() bool {
	return ocenv.GetBool(DiagnosticsExportEnabledEnvVar, true)
}

// GetDiagnosticsExportInterval returns the configured interval for diagnostics exports.
func GetDiagnosticsExportInterval() time.Duration {
	return ocenv.GetDuration(DiagnosticsExportIntervalEnvVar, 3*time.Minute)
}
