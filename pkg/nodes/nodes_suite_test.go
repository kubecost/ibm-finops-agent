package nodes

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)
	SetTestEnvironmentVariables(t)
	RunSpecs(t, "Node Collection Testing")
}

func SetTestEnvironmentVariables(t *testing.T) {
	t.Setenv("INSECURE", "true")
	t.Setenv("CLOUDABILITY_INSECURE", "false")
	t.Setenv("CLOUDABILITY_NUMBER_OF_CONCURRENT_NODE_POLLERS", "17")
}