package env

import (
	"path"
	"time"

	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost/exporter"
)

const (
	FinOpsAgentDefault             = "finops-agent"
	HeartbeatExportEnabledEnvVar   = "HEARTBEAT_EXPORT_ENABLED"
	DiagnosticsExportEnabledEnvVar = "DIAGNOSTICS_EXPORT_ENABLED"
	MinuteMetricsEnabledEnvVar     = "MINUTE_METRICS_ENABLED"

	AllocationExportIntervalEnvVar        = "ALLOCATION_EXPORT_INTERVAL"
	AssetExportIntervalEnvVar             = "ASSET_EXPORT_INTERVAL"
	NetworkInsightExportIntervalEnvVar    = "NETWORK_INSIGHT_EXPORT_INTERVAL"
	KubeModelExportIntervalEnvVar         = "KUBEMODEL_EXPORT_INTERVAL"
	HeartbeatExportIntervalEnvVar         = "HEARTBEAT_EXPORT_INTERVAL"
	DiagnosticsExportIntervalEnvVar       = "DIAGNOSTICS_EXPORT_INTERVAL"
	StreamingExportEnabledEnvVar          = "STREAMING_EXPORT_ENABLED"
	StreamingExportCompressionLevelEnvVar = "STREAMING_EXPORT_COMPRESSION_LEVEL"
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

// IsStreamingExportEnabled returns true if the bingen pipeline exporters should use a streaming io.Writer
// when exporting data, as opposed to encoding a []byte, then uploading.
func IsStreamingExportEnabled() bool {
	return coreenv.GetBool(StreamingExportEnabledEnvVar, false)
}

// GetStreamingExportCompressionLevel returns the compression level to use for streaming compressed exporter uploads. This value is
// set via env var to a level of 0 (disabled), -1 (default compression), or within the range: 1 (best speed) -> 9 (best compression).
// Any values outside of the valid range will default to the 1 (best speed) compression level. If the env var is not set, and streaming
// is enabled, then compression is disabled by default.
//
// Note that these compression level values align with the gzip encoding API in the go standard library.
func GetStreamingExportCompressionLevel() exporter.ExportCompressionLevel {
	// for now, this can only be enabled if streaming is enabled, so we ensure this is the case here:
	if !IsStreamingExportEnabled() {
		return exporter.ExportCompressionLevelNone
	}

	// defaults to 0 (compression disabled)
	level := exporter.ExportCompressionLevel(coreenv.GetInt(StreamingExportCompressionLevelEnvVar, 0))

	// if the level provided is invalid (explicitly set, but not in the 0-9 range),
	// the more "acceptable" default for our use-case is speed.
	if !level.IsValid() {
		log.Debugf("Invalid export compression level set: %d - Defaulting to 1 (BestSpeed).", level)
		return exporter.ExportCompressionLevelBestSpeed
	}

	return level
}

// GetExportBucketConfigFile returns the file path for the export bucket
func GetExportBucketConfigFile() string {
	return path.Join(coreenv.GetConfigPath(), "federated-store.yaml")
}

func GetFinOpsAgentNamespace() string {
	return coreenv.GetInstallNamespace(FinOpsAgentDefault)
}

func GetFinOpsAgentAppName() string {
	return coreenv.Get(coreenv.AppNameEnvVar, FinOpsAgentDefault)
}

// IsFinOpsAgentKubeModelExported distinct implementation from opencost changes default to true.
// exporting KubeModel is required to support resource quotas. There is a seperate env var "FORCE_KUBEMODEL_V1"
// which is currently defaulted to true which prevents additional data from making it through the pipeline.
func IsFinOpsAgentKubeModelExported() bool {
	return coreenv.GetBool(coreenv.ExportKubeModelEnvVar, true)
}
