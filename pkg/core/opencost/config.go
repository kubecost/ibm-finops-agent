package opencost

import "github.com/opencost/opencost/pkg/env"

type OpenCostConfig struct {
	ConfigPath                 string // env.GetConfigPathWithDefault("/var/configs/")
	CloudProviderAPIKey        string // env.GetCloudProviderAPIKey()
	CollectorDataSourceEnabled bool   // env.IsCollectorDataSourceEnabled()
	BucketConfigFile           string // env.GetExportBucketConfigFile()
}

// NewOpenCostConfig creates a new OpenCostConfig with values parsed from the environment variables.
func NewOpenCostConfigFromEnv() *OpenCostConfig {
	return &OpenCostConfig{
		ConfigPath:                 env.GetConfigPathWithDefault("/var/configs/"),
		CloudProviderAPIKey:        env.GetCloudProviderAPIKey(),
		CollectorDataSourceEnabled: env.IsCollectorDataSourceEnabled(),
		BucketConfigFile:           env.GetExportBucketConfigFile(),
	}
}
