package emitter

import "github.com/ibm/finops-agent/pkg/env"

// SnapshotConfig holds the configuration for general snapshotting options.
type SnapshotConfig struct {
	// MinutelyMetricsEnabled indicates whether or not to snapshot 10m resolution
	// metrics when the snapshot occurs.
	MinutelyMetricsEnabled bool
}

// NewSnapshotConfig creates a new SnapshotConfig instance populated with values
// parsed from environment variables.
func NewSnapshotConfigFromEnv() *SnapshotConfig {
	return &SnapshotConfig{
		MinutelyMetricsEnabled: env.IsMinuteMetricsEnabled(),
	}
}

// DefaultSnapshotConfig returns a default SnapshotConfig instance.
func DefaultSnapshotConfig() *SnapshotConfig {
	return &SnapshotConfig{
		MinutelyMetricsEnabled: false,
	}
}
