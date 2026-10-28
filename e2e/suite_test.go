//go:build e2e

package e2e_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/ibm/finops-agent/e2e/harness"
)

var (
	// cfg is shared by every spec in the suite.
	cfg harness.Config
	// cluster is the kind cluster the suite runs against; nil if setup failed early.
	cluster *harness.Cluster
)

func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "FinOps Agent E2E Suite")
}

var _ = BeforeSuite(func(ctx SpecContext) {
	harness.LogWriter = GinkgoWriter

	var err error
	// environment setup
	cfg, err = harness.ConfigFromEnv()
	Expect(err).NotTo(HaveOccurred(), "invalid e2e settings")
	GinkgoWriter.Printf("e2e config: %+v\n", cfg)

	// create a valid and living cluster
	cluster, err = harness.EnsureCluster(ctx, cfg)
	Expect(err).NotTo(HaveOccurred())
	GinkgoWriter.Printf("kubeconfig: %s\n", cluster.Kubeconfig)

	// pull the image and ensure the image is valid
	Expect(cluster.LoadImage(ctx, cfg.Image)).To(Succeed())
}, NodeTimeout(10*time.Minute))

var _ = AfterSuite(func(ctx SpecContext) {
	if cluster == nil {
		return
	}
	if cfg.KeepCluster {
		GinkgoWriter.Printf("KEEP_CLUSTER set; leaving %s. Use: export KUBECONFIG=%s\n", cluster.Name, cluster.Kubeconfig)
		return
	}
	Expect(cluster.Delete(ctx)).To(Succeed())
}, NodeTimeout(2*time.Minute))

// Placeholder so BeforeSuite runs; removed in step 4 when install_test.go adds real specs.
var _ = It("loads config", func() {
	Expect(cfg.ImageRef.Tag).NotTo(BeEmpty())
})
