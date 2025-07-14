package kubecost

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/env"
)

func CreateTestBucketConfigFile(t *testing.T, contents string) string {
	dir := t.TempDir()
	testBucketConfigFile := path.Join(dir, "storage-config.yaml")
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

	config := NewEmitterConfigFromEnv()
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

	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv(env.ConfigPathEnvVar, tempDir)

	config := NewEmitterConfigFromEnv()
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

	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv(env.ConfigPathEnvVar, tempDir)

	config := NewEmitterConfigFromEnv()
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("ValidateConfig failed: %v", err)
	}
	t.Logf("ValidateConfig succeeded with config")
}
