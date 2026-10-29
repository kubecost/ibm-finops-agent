//go:build e2e

package e2e_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes"

	"github.com/ibm/finops-agent/e2e/harness"
)

const (
	// setupTimeout bounds all of BeforeSuite: cluster, image, localstack, agent.
	setupTimeout = 15 * time.Minute
	// setupGrace is how long BeforeSuite may keep running after a timeout or
	// Ctrl-C, so EnsureCluster's cleanup (up to 2m) can finish.
	setupGrace = 3 * time.Minute
	// waitTimeout bounds each wait inside setup (localstack Ready, agent rollout, stress pod).
	waitTimeout = 5 * time.Minute
)

var (
	// cfg is shared by every spec in the suite.
	cfg harness.Config
	// cluster is the kind cluster the suite runs against; nil if setup failed early.
	cluster *harness.Cluster
	// kube is a client for the cluster, for specs that query it.
	kube kubernetes.Interface
	// installer installs releases on the cluster; specs can reinstall the agent
	// with different values through installer.InstallAgent.
	installer *harness.Installer
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

	kube, err = harness.NewClient(cluster.Kubeconfig)
	Expect(err).NotTo(HaveOccurred())
	installer = &harness.Installer{Cluster: cluster, Client: kube, Config: cfg, Timeout: waitTimeout}

	// fake S3 for the agent's federated storage
	By("installing localstack")
	Expect(installer.InstallLocalstack(ctx)).To(Succeed())
	By("creating the " + harness.Bucket + " bucket")
	Expect(installer.EnsureBucket(ctx)).To(Succeed())

	// a known workload for the agent to report on (as e2e.sh did). It must be
	// Ready before the agent starts, so every sample the agent emits includes it.
	By("deploying the stress pod")
	Expect(harness.DeployStress(ctx, kube, waitTimeout)).To(Succeed())

	// the agent under test, at the pinned chart version
	By("installing the agent")
	Expect(installer.InstallAgent(ctx, harness.AgentInstall{})).To(Succeed())
}, NodeTimeout(setupTimeout), GracePeriod(setupGrace))

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
