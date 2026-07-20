package opencost

import (
	"github.com/ibm/finops-agent/kubecost/env"
	kcenv "github.com/ibm/finops-agent/kubecost/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/external"
	ocenv "github.com/opencost/opencost/pkg/env"
)

type OpenCostConfig struct {
	ConfigPath                 string           // env.GetConfigPathWithDefault("/var/configs/")
	CloudProviderAPIKey        string           // env.GetCloudProviderAPIKey()
	CollectorDataSourceEnabled bool             // env.IsCollectorDataSourceEnabled()
	BucketConfigFile           string           // env.GetExportBucketConfigFile()
	AgentNamespace             string           // kcenv.GetFinOpsAgentNamespace()
	ExternalCfg                *external.Config // for any external config
}

// NewOpenCostConfig creates a new OpenCostConfig with values parsed from the environment variables.
func NewOpenCostConfigFromEnv() *OpenCostConfig {
	ocCfg := &OpenCostConfig{
		ConfigPath:                 coreenv.GetConfigPath(),
		CloudProviderAPIKey:        ocenv.GetCloudProviderAPIKey(),
		CollectorDataSourceEnabled: ocenv.IsCollectorDataSourceEnabled(),
		BucketConfigFile:           env.GetExportBucketConfigFile(),
		AgentNamespace:             kcenv.GetFinOpsAgentNamespace(),
	}

	externalnodeLabelsCM := ocenv.GetExternalNodeLabelsConfigMapName()
	if externalnodeLabelsCM != "" {
		nodeLabelsCfg := external.NewNodeLabelConfig(
			externalnodeLabelsCM,
			ocenv.GetExternalNodeLabelsNamespace(),
			ocenv.GetExternalNodeLabelsKey(),
			ocenv.GetExternalNodeLabelsRoute(),
		)

		// Any future external labels can be appended as a separate config.
		// example external pod labels, namespace labels etc.
		ocCfg.ExternalCfg = external.NewConfig(nodeLabelsCfg)
	}
	return ocCfg
}
