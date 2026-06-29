package kubecost

import (
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/opencost/exporter"
)

func CreateTestBucketConfigFile(t *testing.T, contents string) string {
	dir := t.TempDir()
	testBucketConfigFile := path.Join(dir, "federated-store.yaml")
	err := os.WriteFile(testBucketConfigFile, []byte(contents), 0644)
	if err != nil {
		t.Fatalf("Failed to write test bucket config file: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Remove(testBucketConfigFile); err != nil {
			t.Logf("Failed to remove test bucket config file: %v", err)
		}
	})

	return dir

}

func TestMissingBucketConfigFileValidateConfig(t *testing.T) {
	t.Setenv("CLUSTER_ID", "test-cluster")
	tempDir := os.TempDir()
	t.Setenv(env.ConfigPathEnvVar, tempDir)

	config := NewEmitterConfigFromEnv("cluster-uid")
	if err := ValidateConfig(config); err == nil {
		t.Fatal("Expected ValidateConfig to fail due to missing bucket config file, but it succeeded")
	} else {
		t.Logf("ValidateConfig failed as expected: %v", err)
		msg := err.Error()
		if !strings.Contains(msg, "failed to load bucket configuration file") {
			t.Fatalf("Expected error message to contain 'failed to load bucket configuration file', got: %s", msg)
		}
	}
}

func TestInvalidBucketConfigFileValidateConfig(t *testing.T) {
	tempDir := CreateTestBucketConfigFile(t, `type: GCS
config:
  bucket: unified-agent-test
  service_account: |-
    {
      "type": "service_account",
      "junk": "not valid",
    }`)

	t.Setenv(env.ClusterIDEnvVar, "test-cluster")
	t.Setenv(env.ConfigPathEnvVar, tempDir)

	config := NewEmitterConfigFromEnv("cluster-uid")
	if err := ValidateConfig(config); err == nil {
		t.Fatal("Expected ValidateConfig to fail due to invalid bucket config file, but it succeeded")
	} else {
		t.Logf("ValidateConfig failed as expected: %v", err)
		msg := err.Error()
		if !strings.Contains(msg, "failed to create bucket storage") {
			t.Fatalf("Expected error message to contain 'failed to create bucket storage', got: %s", msg)
		}
	}
}

func TestValidBucketValidateConfig(t *testing.T) {
	t.Skip("Test Requires a Valid Bucket Configuration")

	tempDir := CreateTestBucketConfigFile(t, `<valid bucket config contents>`)

	t.Setenv(env.ClusterIDEnvVar, "test-cluster")
	t.Setenv(env.ConfigPathEnvVar, tempDir)

	config := NewEmitterConfigFromEnv("cluster-uid")
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("ValidateConfig failed: %v", err)
	}
	t.Logf("ValidateConfig succeeded with config")
}

func TestStreamingAndCompressionConfigs(t *testing.T) {
	type expected struct {
		isStreaming      bool
		compressionLevel exporter.ExportCompressionLevel
	}
	type testCase struct {
		isStreaming      string
		compressionLevel string
		exp              expected
	}

	cases := []testCase{
		{
			isStreaming:      "true",
			compressionLevel: "",
			exp:              expected{isStreaming: true, compressionLevel: exporter.ExportCompressionLevelNone},
		},
		{
			isStreaming:      "false",
			compressionLevel: "1",
			exp:              expected{isStreaming: false, compressionLevel: exporter.ExportCompressionLevelNone},
		},
		{
			isStreaming:      "true",
			compressionLevel: "1",
			exp:              expected{isStreaming: true, compressionLevel: exporter.ExportCompressionLevelBestSpeed},
		},
		{
			isStreaming:      "true",
			compressionLevel: "55",
			exp:              expected{isStreaming: true, compressionLevel: exporter.ExportCompressionLevelBestSpeed},
		},
		{
			isStreaming:      "true",
			compressionLevel: "9",
			exp:              expected{isStreaming: true, compressionLevel: exporter.ExportCompressionLevelBestCompression},
		},
		{
			isStreaming:      "true",
			compressionLevel: "-1",
			exp:              expected{isStreaming: true, compressionLevel: exporter.ExportCompressionLevelDefault},
		},
		{
			isStreaming:      "true",
			compressionLevel: "-5",
			exp:              expected{isStreaming: true, compressionLevel: exporter.ExportCompressionLevelBestSpeed},
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("streaming and compression config stream=%s level=%s", c.isStreaming, c.compressionLevel), func(t *testing.T) {
			tempDir := t.TempDir()
			t.Setenv(env.ClusterIDEnvVar, "test-cluster")
			t.Setenv(env.ConfigPathEnvVar, tempDir)
			t.Setenv(kcenv.StreamingExportEnabledEnvVar, c.isStreaming)
			t.Setenv(kcenv.StreamingExportCompressionLevelEnvVar, c.compressionLevel)

			config := NewEmitterConfigFromEnv("cluster-uid")

			if config.StreamingExportEnabled != c.exp.isStreaming {
				t.Errorf("config.StreamingExportEnabled (%t) != expected (%t)", config.StreamingExportEnabled, c.exp.isStreaming)
				return
			}

			if config.StreamingExportCompressionLevel != c.exp.compressionLevel {
				t.Errorf("config.StreamingExportCompressionLevel (%d) != expected (%d)", config.StreamingExportCompressionLevel, c.exp.compressionLevel)
				return
			}
		})
	}
}
