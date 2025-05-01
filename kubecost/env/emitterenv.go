package env

import ocenv "github.com/opencost/opencost/core/pkg/env"

const (
	MinuteMetricsEnabledEnvVar = "MINUTE_METRICS_ENABLED"
)

// IsMinuteMetricsEnabled returns true if the 10m resolution emitter for kubecost
// is enabled. This is used to emit 10m resolution allocation and asset pipeline data.
func IsMinuteMetricsEnabled() bool {
	return ocenv.GetBool(MinuteMetricsEnabledEnvVar, false)
}
