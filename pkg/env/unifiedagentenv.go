package env

import "github.com/opencost/opencost/core/pkg/env"

const (
	KubecostEmitterEnabledEnvVar = "KUBECOST_EMITTER_ENABLED"
	CloudyEmitterEnabledEnvVar   = "CLOUDY_EMITTER_ENABLED"
	TurboEmitterEnabledEnvVar    = "TURBO_EMITTER_ENABLED"

	OpenCostDataSourceEnabledEnvVar = "OPENCOST_SOURCE_ENABLED"
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
