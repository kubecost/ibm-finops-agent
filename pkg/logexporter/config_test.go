package logexporter

import (
	"fmt"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "/var/log/finops",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "buffer size too small",
			config: &Config{
				BufferSize:     512 * 1024, // 512KB < 1MB minimum
				LogDirPath:     "/var/log/finops",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "buffer size",
		},
		{
			name: "buffer size too large",
			config: &Config{
				BufferSize:     200 * 1024 * 1024, // 200MB > 100MB maximum
				LogDirPath:     "/var/log/finops",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "buffer size",
		},
		{
			name: "empty log dir path(Default value is used)",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "relative log dir path",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "relative/path",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "log directory path must be absolute",
		},
		{
			name: "sync interval too small",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "/var/log/finops",
				SyncInterval:   500 * time.Millisecond, // < 1 second minimum
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "sync interval",
		},
		{
			name: "zero sync interval is valid (disabled)",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "/var/log/finops",
				SyncInterval:   0, // Disabled is valid
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "upload interval too small",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "/var/log/finops",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 30 * time.Second, // < 1 minute minimum
			},
			wantErr: true,
			errMsg:  "upload interval",
		},
		{
			name: "empty cluster name",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     "/var/log/finops",
				SyncInterval:   5 * time.Second,
				ClusterName:    "",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
			errMsg:  "cluster name cannot be empty",
		},
		{
			name: "no path prefix (default value is used)",
			config: &Config{
				BufferSize:     minBufferSize,
				LogDirPath:     "/tmp",
				SyncInterval:   minSyncInterval,
				ClusterName:    "c",
				PathPrefix:     "",
				ExportInterval: minExportInterval,
			},
			wantErr: false,
		},
		{
			name: "Upload interval too small",
			config: &Config{
				BufferSize:     minBufferSize,
				LogDirPath:     "/tmp",
				SyncInterval:   minSyncInterval,
				ClusterName:    "c",
				PathPrefix:     "logs",
				ExportInterval: 30 * time.Second,
			},
			wantErr: true,
			errMsg:  "upload interval 30s is below minimum 1m0s",
		},
		{
			name: "maximum valid buffer size",
			config: &Config{
				BufferSize:     maxBufferSize,
				LogDirPath:     "/var/log/finops",
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// set env vars
			t.Setenv(env.LogExportBufferSizeEnvVar, fmt.Sprintf("%d", tt.config.BufferSize))
			t.Setenv(env.LogExportDirPathEnvVar, tt.config.LogDirPath)
			t.Setenv(env.LogExportSyncIntervalEnvVar, fmt.Sprintf("%v", tt.config.SyncInterval))
			t.Setenv(coreenv.ClusterIDEnvVar, tt.config.ClusterName)
			t.Setenv(env.LogExportPathPrefixEnvVar, tt.config.PathPrefix)
			t.Setenv(env.LogExportIntervalEnvVar, fmt.Sprintf("%v", tt.config.ExportInterval))

			config := NewConfigFromEnv()
			err := config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr && tt.errMsg != "" {
				if err == nil || len(err.Error()) == 0 {
					t.Errorf("Config.Validate() expected error containing %q, got nil", tt.errMsg)
				}
			}
		})
	}
}

// Made with Bob
