package kubecost

import (
	"fmt"
	"os"
	"path"
	"slices"
	"time"

	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/ibm/finops-agent/kubecost/errors"
	"github.com/ibm/finops-agent/pkg/emitter"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/opencost/opencost/pkg/env"
)

// ExportIntervalConfig holds the export intervals for various data types in the kubecost emitter.
// While it does not give you explicit interval control per resolution exports for Allocation, Asset, and
// NetworkInsights, it does allow you to set the intervals for each type.
//
// Heartbeat and Diagnostics do *not* have per-resolution control, as they are not resolution-based exports.
type ExportIntervalConfig struct {
	AllocationInterval     time.Duration
	AssetInterval          time.Duration
	NetworkInsightInterval time.Duration
	KubeModelInterval      time.Duration
	HeartbeatInterval      time.Duration
	DiagnosticsInterval    time.Duration
}

// DefaultExportIntervalConfig returns a default ExportIntervalConfig with reasonable defaults.
func DefaultExportIntervalConfig() *ExportIntervalConfig {
	return &ExportIntervalConfig{
		AllocationInterval:     10 * time.Minute,
		AssetInterval:          10 * time.Minute,
		NetworkInsightInterval: 10 * time.Minute,
		KubeModelInterval:      10 * time.Minute,
		HeartbeatInterval:      5 * time.Minute,
		DiagnosticsInterval:    3 * time.Minute,
	}
}

// NewExportIntervalConfigFromEnv creates an ExportIntervalConfig from environment variables.
func NewExportIntervalConfigFromEnv() *ExportIntervalConfig {
	return &ExportIntervalConfig{
		AllocationInterval:     kcenv.GetAllocationExportInterval(),
		AssetInterval:          kcenv.GetAssetExportInterval(),
		NetworkInsightInterval: kcenv.GetNetworkInsightExportInterval(),
		KubeModelInterval:      kcenv.GetKubeModelExportInterval(),
		HeartbeatInterval:      kcenv.GetHeartbeatExportInterval(),
		DiagnosticsInterval:    kcenv.GetDiagnosticsExportInterval(),
	}
}

// EmitterConfig is a struct that holds the configuration for the kubecost emitter.
type EmitterConfig struct {
	ClusterUID                     string
	ClusterName                    string
	AppName                        string
	ConfigPath                     string
	CloudProviderAPIKey            string
	InstallNamespace               string
	BucketConfigFile               string
	ExportIntervals                *ExportIntervalConfig
	QueryResolution                time.Duration
	EmitAllocationMinuteResolution bool
	EmitAssetMinuteResolution      bool
	EmitKubeModelMinuteResolution  bool
	KubernetesResourcesRequired    []string
}

// NewEmitterConfigFromEnv creates a new EmitterConfig from environment variables.
func NewEmitterConfigFromEnv(clusterUID string) *EmitterConfig {
	return &EmitterConfig{
		ClusterUID:                     clusterUID,
		ClusterName:                    coreenv.GetClusterID(),
		AppName:                        coreenv.GetAppName(),
		ConfigPath:                     coreenv.GetConfigPath(),
		CloudProviderAPIKey:            env.GetCloudProviderAPIKey(),
		InstallNamespace:               coreenv.GetInstallNamespace(""),
		BucketConfigFile:               kcenv.GetExportBucketConfigFile(),
		ExportIntervals:                NewExportIntervalConfigFromEnv(),
		QueryResolution:                1 * time.Minute,
		EmitAllocationMinuteResolution: kcenv.IsMinuteMetricsEnabled(),
		EmitAssetMinuteResolution:      kcenv.IsMinuteMetricsEnabled(),
		EmitKubeModelMinuteResolution:  kcenv.IsMinuteMetricsEnabled(),
		// Kubecost emitter requires all kubernetes resources to be enabled
		KubernetesResourcesRequired: slices.Clone(emitter.SnapshotAllResources),
	}
}

// ValidateConfig validates the EmitterConfig to ensure that it is properly configured for the
// kubecost emitter to function properly. It checks for the presence of required fields and
// verifies the bucket storage configuration correctly loads, and that the bucket storage
// has all the required permissions to write, read, and delete data.
func ValidateConfig(config *EmitterConfig) error {
	// ClusterUID is required for kubecost emitter to function properly.
	if config.ClusterUID == "" {
		//nolint: staticcheck
		return fmt.Errorf("Kubecost Configuration missing valid ClusterUID")
	}

	// ClusterName is required for kubecost emitter to function properly.
	if config.ClusterName == "" {
		//nolint: staticcheck
		return fmt.Errorf("Kubecost Configuration missing valid ClusterName")
	}

	// BucketConfigFile is the most important configuration for the emitter
	// 1) Let's ensure that the file exists and loads correctly.
	// 2) Ensure that the configuration can be passed to the storage API and
	//    return a valid storage.Storage implementation.
	// 3) Attempt to write and delete test data to the bucket to ensure that
	//    we have the correct write permissions.

	if config.BucketConfigFile == "" {
		return errors.MissingExportBucketConfigFileErr()
	}

	// Read the file
	bucketConfig, err := os.ReadFile(config.BucketConfigFile)
	if err != nil {
		log.Errorf("Failed to read bucket config file: %s", err)
		return errors.BucketConfigFileLoadErr(err)
	}

	// Create the bucket storage from the configuration
	bucketStore, err := storage.NewBucketStorage(bucketConfig)
	if err != nil {
		log.Errorf("Failed to create bucket storage. Please ensure that the yaml configuration file is formatted correctly: %s", err)
		return errors.BucketStorageCreationErr(err)
	}

	// Attempt to write data to the bucket, and then remove it to ensure that we have the correct permissions to run the emitter
	testData := "test write permissions for bucket storage"
	testPath := path.Join(config.ClusterName, "write-test", "test.txt")
	err = bucketStore.Write(testPath, []byte(testData))
	if err != nil {
		logPermissionsError("write", err)
		return errors.BucketWriteErr(err)
	}

	// Load the test data back to ensure that it was written correctly, and that we have read permissions
	data, err := bucketStore.Read(testPath)
	if err != nil || string(data) == "" {
		logPermissionsError("read", err)
		return errors.BucketReadErr(err)
	}

	// Delete the test data -- this is necessary due to automated cleanup of bucket storage for the metric collector api.
	err = bucketStore.Remove(testPath)
	if err != nil {
		logPermissionsError("delete", err)
		return errors.BucketDeleteErr(err)
	}

	return nil
}

func logPermissionsError(action string, err error) {
	log.Errorf("Failed to %s test data to/from storage bucket.\n*** Error: %s ***\n*** Please ensure that the bucket is configured correctly and that you have the necessary %s permissions ***", action, err, action)
}
