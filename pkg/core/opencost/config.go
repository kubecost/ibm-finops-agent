package opencost

import (
	"github.com/ibm/finops-agent/kubecost/env"
	agentenv "github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/externallabels"
	ocenv "github.com/opencost/opencost/pkg/env"
)

type OpenCostConfig struct {
	ConfigPath                 string                 // env.GetConfigPathWithDefault("/var/configs/")
	CloudProviderAPIKey        string                 // env.GetCloudProviderAPIKey()
	CollectorDataSourceEnabled bool                   // env.IsCollectorDataSourceEnabled()
	BucketConfigFile           string                 // env.GetExportBucketConfigFile()
	ExternalLabelsCfg          *externallabels.Config // external labels configuration to apply to node labels
}

// NewOpenCostConfig creates a new OpenCostConfig with values parsed from the environment variables.
func NewOpenCostConfigFromEnv() *OpenCostConfig {
	ocCfg := &OpenCostConfig{
		ConfigPath:                 coreenv.GetConfigPath(),
		CloudProviderAPIKey:        ocenv.GetCloudProviderAPIKey(),
		CollectorDataSourceEnabled: ocenv.IsCollectorDataSourceEnabled(),
		BucketConfigFile:           env.GetExportBucketConfigFile(),
	}

	externalLabelsCMName := agentenv.GetExternalLabelsConfigMapName()
	if externalLabelsCMName != "" {
		ocCfg.ExternalLabelsCfg = &externallabels.Config{
			ConfigMapName: externalLabelsCMName,
			Namespace:     agentenv.GetExternalLabelsNamespace(),
			Key:           agentenv.GetExternalLabelsKey(),
			Route:         agentenv.GetExternalLabelsRoute(),
		}
	}
	return ocCfg
}
