//go:build e2e

package e2e_test

import (
	"net/http"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ibm/finops-agent/e2e/harness"
)

// The agent is up and answering. These check the running agent, not the data
// it reports (that is in the data specs).
var _ = Describe("Agent install", Ordered, Label("install"), func() {
	var agent harness.Agent

	BeforeAll(func() {
		agent = harness.Agent{Client: kube, Namespace: cfg.Namespace, Release: cfg.ReleaseName}
	})

	It("has one Ready agent pod that has not restarted", func(ctx SpecContext) {
		list, err := kube.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: harness.AgentSelector(cfg.ReleaseName),
		})
		Expect(err).NotTo(HaveOccurred())

		var running []corev1.Pod
		for _, p := range list.Items {
			if p.DeletionTimestamp == nil && p.Status.Phase != corev1.PodFailed && p.Status.Phase != corev1.PodSucceeded {
				running = append(running, p)
			}
		}
		Expect(running).To(HaveLen(1), "agent pods for release %s", cfg.ReleaseName)
		p := running[0]
		Expect(p.Status.Conditions).To(ContainElement(And(
			HaveField("Type", corev1.PodReady),
			HaveField("Status", corev1.ConditionTrue),
		)), "pod %s is not Ready", p.Name)
		// A restart this early means the agent crashed during startup.
		for _, cs := range p.Status.ContainerStatuses {
			Expect(cs.RestartCount).To(BeZero(), "container %s restarted (last state: %+v)", cs.Name, cs.LastTerminationState)
		}
	})

	It("returns 200 from /healthz", func(ctx SpecContext) {
		// Polled briefly: emitters publish their health checks shortly after start.
		Eventually(func(g Gomega) {
			res, err := agent.Get(ctx, "/healthz")
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(res.StatusCode).To(Equal(http.StatusOK), "body: %s", res.Body)
		}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	})

	It("reports a version on /version", func(ctx SpecContext) {
		res, err := agent.Get(ctx, "/version")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode).To(Equal(http.StatusOK), "body: %s", res.Body)
		// Not compared with the image: it comes from the build's version arg
		// ("dev" for local builds). An expected value can be added for CI.
		Expect(res.TrimmedBody()).NotTo(BeEmpty())
		GinkgoWriter.Printf("agent version: %s\n", res.TrimmedBody())
	})

	It("serves Prometheus metrics on /metrics", func(ctx SpecContext) {
		res, err := agent.Get(ctx, "/metrics")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.StatusCode).To(Equal(http.StatusOK))

		families, err := harness.ParseMetrics(res.Body)
		Expect(err).NotTo(HaveOccurred())
		// From the Go runtime collector, so present whenever the endpoint works.
		Expect(families).To(HaveKey("go_goroutines"))
		GinkgoWriter.Printf("/metrics: %d metric families\n", len(families))
	})
})
