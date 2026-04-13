package logexporter

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
)

const (
	// Validation constants
	minBufferSize     = 1024 * 1024      // 1MB minimum
	maxBufferSize     = 1024 * 1024 * 100 // 100MB maximum
	minSyncInterval   = time.Second
	minUploadInterval = time.Minute
)

type Config struct {
	BufferSize     int64         // Max file size before new file is created
	LogDirPath     string        // Directory path for log files
	SyncInterval   time.Duration // How often to sync to disk; Stop() always syncs on shutdown
	ClusterName    string        // Cluster name for bucket path (logs/<cluster>/...)
	PathPrefix     string        // Path prefix in bucket (default "logs")
	UploadInterval time.Duration // How often to rotate and upload log files to bucket
}

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

// Validate checks if the configuration is valid and returns an error if not.
func (c *Config) Validate() error {
	if c.BufferSize < minBufferSize {
		return fmt.Errorf("buffer size %d is below minimum %d", c.BufferSize, minBufferSize)
	}
	if c.BufferSize > maxBufferSize {
		return fmt.Errorf("buffer size %d exceeds maximum %d", c.BufferSize, maxBufferSize)
	}
	
	if c.LogDirPath == "" {
		return fmt.Errorf("log directory path cannot be empty")
	}
	if !filepath.IsAbs(c.LogDirPath) {
		return fmt.Errorf("log directory path must be absolute: %s", c.LogDirPath)
	}
	
	if c.SyncInterval > 0 && c.SyncInterval < minSyncInterval {
		return fmt.Errorf("sync interval %v is below minimum %v", c.SyncInterval, minSyncInterval)
	}
	
	if c.UploadInterval < minUploadInterval {
		return fmt.Errorf("upload interval %v is below minimum %v", c.UploadInterval, minUploadInterval)
	}
	
	if c.ClusterName == "" {
		return fmt.Errorf("cluster name cannot be empty")
	}
	
	// Validate PathPrefix doesn't contain invalid characters
	if strings.ContainsAny(c.PathPrefix, "\x00") {
		return fmt.Errorf("path prefix contains invalid characters")
	}
	
	return nil
}
