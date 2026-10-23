//go:build e2e

package e2e_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ibm/finops-agent/e2e/harness"
)

// cfg is shared by every spec in the suite.
var cfg harness.Config

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FinOps Agent E2E Suite")
}

var _ = BeforeSuite(func() {
	var err error
	cfg, err = harness.ConfigFromEnv()
	Expect(err).NotTo(HaveOccurred(), "invalid e2e settings")
	GinkgoWriter.Printf("e2e config: %+v\n", cfg)
	// PR 2: harness.EnsureCluster + LoadImage go here; AfterSuite deletes unless cfg.KeepCluster.
})

var _ = It("loads config", func() {
	Expect(cfg.ImageRef.Tag).NotTo(BeEmpty())
})
