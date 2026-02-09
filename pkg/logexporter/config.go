package logexporter

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
)

const (
	minBufferSize     = 1024 * 1024       // 1MB minimum
	maxBufferSize     = 1024 * 1024 * 100 // 100MB maximum
	minSyncInterval   = 1 * time.Second
	minExportInterval = 1 * time.Minute
)

type Config struct {
	BufferSize     int64         // Max file size allowed before new log file is created
	LogDirPath     string        // Directory path for log files in the PVC/container
	SyncInterval   time.Duration // How often to sync logs to log file on disk
	ClusterName    string        // Cluster name for bucket path (logs/<cluster>/...)
	PathPrefix     string        // Path prefix in bucket (default "finops-agent-logs")
	ExportInterval time.Duration // How often to rotate and upload log files to bucket
}

func NewConfigFromEnv() *Config {
	return &Config{
		BufferSize:     env.GetLogExportBufferSize(),
		LogDirPath:     env.GetLogExportDirPath(),
		SyncInterval:   env.GetLogExportSyncInterval(),
		ClusterName:    coreenv.GetClusterID(),
		PathPrefix:     env.GetLogExportPathPrefix(),
		ExportInterval: env.GetLogExportInterval(),
	}
}

// Validate checks if the configuration is valid and returns an error if not.
func (c *Config) Validate() error {
	if c.BufferSize < minBufferSize {
		return fmt.Errorf("buffer size %d is below minimum %d", c.BufferSize, minBufferSize)
	}
	if c.BufferSize > maxBufferSize {
		return fmt.Errorf("buffer size %d exceeds maximum %d", c.BufferSize, maxBufferSize)
	}

	if !filepath.IsAbs(c.LogDirPath) {
		return fmt.Errorf("log directory path must be absolute: %s", c.LogDirPath)
	}

	if c.SyncInterval != 0 && c.SyncInterval < minSyncInterval {
		return fmt.Errorf("sync interval %v is below minimum %v", c.SyncInterval, minSyncInterval)
	}

	if c.ExportInterval < minExportInterval {
		return fmt.Errorf("upload interval %v is below minimum %v", c.ExportInterval, minExportInterval)
	}

	if c.ClusterName == "" {
		return fmt.Errorf("cluster name cannot be empty")
	}

	return nil
}
