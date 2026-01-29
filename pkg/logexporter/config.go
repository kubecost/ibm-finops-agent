package logexporter

import (
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
)

// Config holds the configuration for log export to PVC and bucket upload
type Config struct {
	BufferSize     int64          // Max file size before new file is created
	LogDirPath     string         // Directory path for log files
	SyncInterval   time.Duration  // How often to sync to disk; Stop() always syncs on shutdown
	ClusterName    string         // Cluster name for bucket path (logs/<cluster>/...)
	PathPrefix     string         // Path prefix in bucket (default "logs")
	UploadInterval time.Duration  // How often to upload closed log files to bucket
}

// NewConfigFromEnv creates a new Config from environment variables.
// clusterName is passed from main (e.g. coreenv.GetClusterID()).
func NewConfigFromEnv() *Config {
	return &Config{
		BufferSize:     env.GetLogExportBufferSize(),
		LogDirPath:     env.GetLogExportDirPath(),
		SyncInterval:   env.GetLogExportSyncInterval(),
		ClusterName:    coreenv.GetClusterID(),
		PathPrefix:     env.GetLogExportPathPrefix(),
		UploadInterval: env.GetLogExportInterval(),
	}
}
