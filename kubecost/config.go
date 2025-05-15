package kubecost

import (
	"time"

	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/opencost/opencost/pkg/env"
)

// EmitterConfig is a struct that holds the configuration for the kubecost emitter.
type EmitterConfig struct {
	ClusterID                      string // env.GetClusterID()
	ConfigPath                     string // env.GetConfigPathWithDefault("/var/configs/")
	CloudProviderAPIKey            string // env.GetCloudProviderAPIKey()
	KubecostNamespace              string // env.GetKubecostNamespace()
	BucketConfigFile               string // env.GetExportBucketConfigFile()
	ExportInterval                 time.Duration
	QueryResolution                time.Duration
	EmitAllocationMinuteResolution bool
	EmitAssetMinuteResolution      bool
}

// NewEmitterConfigFromEnv creates a new EmitterConfig from environment variables.
func NewEmitterConfigFromEnv() *EmitterConfig {
	return &EmitterConfig{
		ClusterID:                      env.GetClusterID(),
		ConfigPath:                     env.GetConfigPathWithDefault("/var/configs/"),
		CloudProviderAPIKey:            env.GetCloudProviderAPIKey(),
		KubecostNamespace:              env.GetInstallNamespace(), // this is used to receive configmap updates -- poorly named
		BucketConfigFile:               env.GetExportBucketConfigFile(),
		ExportInterval:                 10 * time.Minute, // may want to make all pipelines configurable
		QueryResolution:                env.GetETLResolution(),
		EmitAllocationMinuteResolution: kcenv.IsMinuteMetricsEnabled(),
		EmitAssetMinuteResolution:      kcenv.IsMinuteMetricsEnabled(),
	}
}
