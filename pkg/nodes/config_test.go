package nodes

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cast"
)

var _ = Describe("Node config", func() {
	BeforeEach(func() {
		t := GinkgoT()
		t.Setenv("CLOUDABILITY_NUMBER_OF_CONCURRENT_NODE_POLLERS", "17")
		t.Setenv("INSECURE", "true")
		t.Setenv("CLOUDABILITY_INSECURE", "false")
		t.Setenv("CLUSTER_NAME", "test")
	})
	It("should return proper values for dual-CLDY environment variables", func() {

		nodeClientConfig, err := NewNodeClientConfig()
		Expect(err).ToNot(HaveOccurred())

		// Variable not set, should default
		Expect(nodeClientConfig.ForceKubeProxy).To(BeFalse())

		// CLOUDABILITY_ version is set, should not use default
		Expect(nodeClientConfig.ConcurrentPollers).To(BeNumerically("==", 17))

		// INSECURE and CLOUDABILITY_INSECURE are set, should favor INSECURE
		insecure := getEnvValueOrDefault("INSECURE", false, cast.ToBool)
		Expect(insecure).To(BeTrue())
	})
})
