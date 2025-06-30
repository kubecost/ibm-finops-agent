package kubecost

import (
	"os"
	"path"
	"strings"
	"testing"

	"github.com/opencost/opencost/pkg/env"
)

func CreateTestBucketConfigFile(t *testing.T, fileName string, contents string) string {
	dir := t.TempDir()
	testBucketConfigFile := path.Join(dir, fileName)
	err := os.WriteFile(testBucketConfigFile, []byte(contents), 0644)
	if err != nil {
		t.Fatalf("Failed to write test bucket config file: %v", err)
	}

	t.Cleanup(func() {
		if err := os.Remove(testBucketConfigFile); err != nil {
			t.Logf("Failed to remove test bucket config file: %v", err)
		}
	})

	return testBucketConfigFile

}

func TestMissingEnvVarValidateConfig(t *testing.T) {
	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv(env.ExportBucketConfigFileEnvVar, "")

	config := NewEmitterConfigFromEnv()
	if err := ValidateConfig(config); err == nil {
		t.Fatal("Expected ValidateConfig to fail due to missing env var for bucket config file, but it succeeded")
	} else {
		t.Logf("ValidateConfig failed as expected: %v", err)
		msg := err.Error()
		if !strings.Contains(msg, "configuration missing valid ExportBucketConfigFile") {
			t.Fatalf("Expected error message to contain 'configuration missing valid ExportBucketConfigFile', got: %s", msg)
		}
	}
}

func TestMissingBucketConfigFileValidateConfig(t *testing.T) {
	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv(env.ExportBucketConfigFileEnvVar, "made-up-path.yaml")

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
	testBucketConfigFile := CreateTestBucketConfigFile(t, "test-bucket-config.yaml", `type: GCS
config:
  bucket: unified-agent-test
  service_account: |-
    {
      "type": "service_account",
      "junk": "not valid",
    }`)

	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv(env.ExportBucketConfigFileEnvVar, testBucketConfigFile)

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

	testBucketConfigFile := CreateTestBucketConfigFile(t, "test-bucket-config.yaml", `<valid bucket config contents>`)

	t.Setenv("CLUSTER_ID", "test-cluster")
	t.Setenv(env.ExportBucketConfigFileEnvVar, testBucketConfigFile)

	config := NewEmitterConfigFromEnv()
	if err := ValidateConfig(config); err != nil {
		t.Fatalf("ValidateConfig failed: %v", err)
	}
	t.Logf("ValidateConfig succeeded with config")
}
