package nodes

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/spf13/cast"
)

var _ = Describe("Node config", func() {
	It("should return proper values for dual-CLDY environment variables", func() {
		nodeClientConfig := NewNodeClientConfig()

		// Variable not set, should default
		Expect(nodeClientConfig.ForceKubeProxy).To(BeFalse())

		// CLOUDABILITY_ version is set, should not use default
		Expect(nodeClientConfig.ConcurrentPollers).To(BeNumerically("==", 17))

		// INSECURE and CLOUDABILITY_INSECURE are set, should favor INSECURE
		insecure := getEnvValueOrDefault("INSECURE", false, cast.ToBool)
		Expect(insecure).To(BeTrue())
	})
})