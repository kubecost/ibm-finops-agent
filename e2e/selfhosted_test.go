//go:build e2e

package e2e_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ibm/finops-agent/e2e/harness"
)

const (
	// minStressMinutes is how much stress-pod runtime every window must hold
	// before the specs read it. CPU usage comes from pairs of 30s scrapes of a
	// counter the kubelet refreshes only every 11-19s, so one pair can be off
	// by over 50%. Over N seconds the pairs' errors cancel down to about
	// (refresh gap)/N: under 20% after 2 minutes, inside the CPU bounds below.
	minStressMinutes = 2.0

	// bucketTimeout bounds the wait. minStressMinutes of runtime is normally
	// there about 2.5 minutes after the agent starts. Crossing a 10m boundary
	// (about 1 run in 3) starts a new 10m window that needs its own
	// minStressMinutes, so the worst case is about 4.8 minutes. The timeout
	// leaves room for slow shared CI runners; a passing run doesn't wait for it.
	bucketTimeout = 8 * time.Minute

	// The stress pod runs harness.StressCPUMillicores (100m) and
	// harness.StressMemoryMegabytes (128 MB, about 136 MiB working set). The
	// CPU bounds are tight on purpose: a collector bug at short scrape
	// intervals reported about 3x the real usage, and a loose bound hid it.
	minStressCores = 0.070
	maxStressCores = 0.150
	minStressBytes = 100 * 1024 * 1024
	maxStressBytes = 256 * 1024 * 1024
)

var resolutionDurations = map[string]time.Duration{"10m": 10 * time.Minute, "1h": time.Hour, "1d": 24 * time.Hour}

// The Kubecost emitter exports allocations, assets, kubemodel, a heartbeat
// and a diagnostics report to the federated-storage bucket (localstack).
var _ = Describe("Self-hosted bucket export", Ordered, Label("selfhosted"), func() {
	var (
		reader harness.BucketReader
		snap   *harness.BucketSnapshot
		// lastErr is the last failed read, saved with the snapshot on failure.
		lastErr error
		dumped  bool
	)

	// On the first failure, save what the bucket held, so a CI failure can be
	// read from the uploaded artifacts instead of a kept cluster.
	AfterEach(func() {
		if dumped || !CurrentSpecReport().Failed() {
			return
		}
		dumped = true
		dumpBucket(snap, lastErr)
	})

	BeforeAll(func(ctx SpecContext) {
		reader = harness.BucketReader{Client: kube, Namespace: cfg.Namespace, Cluster: cluster, ClusterID: clusterID}

		// Ready once every pipeline and resolution has a non-empty set and the
		// stress pod has usage and minStressMinutes of runtime in all of them.
		polls := 0
		Eventually(func(g Gomega) {
			polls++
			next, err := reader.Read(ctx)
			lastErr = err
			g.Expect(err).NotTo(HaveOccurred())
			snap = next // keep the last good read if a later one fails
			for _, res := range harness.BucketResolutions {
				s := stressAllocation(snap.Allocations[res])
				g.Expect(s).NotTo(BeNil(), "stress allocation in allocations/%s", res)
				g.Expect(s.CPUCoreUsageAverage).To(BeNumerically(">", 0), "stress cpu in allocations/%s", res)
				g.Expect(s.RAMBytesUsageAverage).To(BeNumerically(">", 0), "stress memory in allocations/%s", res)
				g.Expect(s.Minutes()).To(BeNumerically(">=", minStressMinutes), "stress runtime in allocations/%s", res)
			}
		}).WithContext(ctx).WithTimeout(bucketTimeout).WithPolling(10 * time.Second).Should(Succeed())

		// How long usage took to reach the bucket. An upper bound when the
		// first poll succeeds: other specs may have run since the install.
		if started := agentStartTime(ctx); !started.IsZero() {
			GinkgoWriter.Printf("bucket ready %s after the agent pod started (%d polls)\n", time.Since(started).Round(time.Second), polls)
		}
		for k, v := range snap.Keys {
			GinkgoWriter.Printf("  %s: %s\n", k, v)
		}
	}, NodeTimeout(bucketTimeout+time.Minute))

	It("exports a heartbeat that advances", func(ctx SpecContext) {
		hb := snap.Heartbeat
		Expect(hb.Application).To(Equal("finops-agent"))
		Expect(hb.Version).NotTo(BeEmpty())
		Expect(hb.Metadata).To(HaveKeyWithValue("name", clusterID))
		Expect(hb.Metadata).To(HaveKeyWithValue("logLevel", Not(BeEmpty())))

		// The heartbeat exports every 10s, each to a new object.
		Eventually(func(g Gomega) {
			next, err := reader.Read(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(next.Heartbeat.Timestamp).To(BeTemporally(">", hb.Timestamp), "newest heartbeat %s", next.Keys["heartbeat"])
		}).WithContext(ctx).WithTimeout(time.Minute).WithPolling(10 * time.Second).Should(Succeed())
		// NodeTimeout, not SpecTimeout: a SpecTimeout would also cover the
		// BeforeAll, which runs as part of whichever spec comes first.
	}, NodeTimeout(2*time.Minute))

	It("exports diagnostics with no failures", func() {
		Expect(snap.Diagnostics.Application).To(Equal("finops-agent"))
		Expect(snap.Diagnostics.Results).NotTo(BeEmpty())
		for _, r := range snap.Diagnostics.Results {
			Expect(r.Error).To(BeEmpty(), "diagnostic %q (%s) in %s", r.Name, r.Category, snap.Keys["diagnostics"])
		}
	})

	It("exports non-empty allocation sets at every resolution", func() {
		for _, res := range harness.BucketResolutions {
			set := snap.Allocations[res]
			Expect(set.IsEmpty()).To(BeFalse(), "allocations/%s", res)
			expectWindow(set.Window, res, snap.Keys["allocations/"+res])
		}
	})

	It("exports non-empty asset sets at every resolution", func() {
		for _, res := range harness.BucketResolutions {
			set := snap.Assets[res]
			Expect(set.IsEmpty()).To(BeFalse(), "assets/%s", res)
			expectWindow(set.Window, res, snap.Keys["assets/"+res])
		}
	})

	It("exports kubemodel sets with the stress namespace and pod", func() {
		for _, res := range harness.BucketResolutions {
			set := snap.KubeModels[res]
			key := snap.Keys["kubemodel/"+res]
			Expect(set.IsEmpty()).To(BeFalse(), key)
			start, end := set.Window.Start, set.Window.End
			expectWindow(opencost.NewClosedWindow(start, end), res, key)

			ns := kubeModelNamespace(set, harness.StressNamespace)
			Expect(ns).NotTo(BeNil(), "stress namespace in %s", key)
			var pod *kubemodel.Pod
			for _, p := range set.Pods {
				if p.Name == "stress" && p.NamespaceUID == ns.UID {
					pod = p
				}
			}
			Expect(pod).NotTo(BeNil(), "stress pod in %s", key)
		}
	})

	It("allocates the agent pod's containers", func() {
		node := cfg.ClusterName + "-control-plane"
		controller := cfg.ReleaseName + "-finops-agent"
		for _, res := range harness.BucketResolutions {
			key := snap.Keys["allocations/"+res]
			containers := map[string]*opencost.Allocation{}
			pods := map[string]bool{}
			for _, a := range snap.Allocations[res].Allocations {
				p := a.Properties
				if p == nil || p.Namespace != cfg.Namespace || !strings.HasPrefix(p.Pod, controller+"-") {
					continue
				}
				containers[p.Container] = a
				pods[p.Pod] = true
			}
			Expect(pods).To(HaveLen(1), "one agent pod in %s", key)
			Expect(containers).To(HaveKey("finops-agent"), key)
			Expect(containers).To(HaveKey("e2e-reader"), key)

			for name, a := range containers {
				p := a.Properties
				Expect(p.Node).To(Equal(node), "%s node in %s", name, key)
				Expect(p.ControllerKind).To(Equal("deployment"), "%s controller kind in %s", name, key)
				Expect(p.Controller).To(Equal(controller), "%s controller in %s", name, key)
				Expect(p.Labels).To(HaveKeyWithValue("app_kubernetes_io_name", "finops-agent"), "%s pod labels in %s", name, key)
				Expect(p.Labels).To(HaveKeyWithValue("app_kubernetes_io_instance", cfg.ReleaseName), "%s pod labels in %s", name, key)
				Expect(p.NamespaceLabels).To(HaveKeyWithValue("kubernetes_io_metadata_name", cfg.Namespace), "%s namespace labels in %s", name, key)
			}
			// The e2e-reader sidecar only sleeps, so only the agent must show usage.
			agent := containers["finops-agent"]
			Expect(agent.CPUCoreUsageAverage).To(BeNumerically(">", 0), "agent cpu in %s", key)
			Expect(agent.RAMBytesUsageAverage).To(BeNumerically(">", 0), "agent memory in %s", key)
		}
	})

	// Covers what specs 6 and 7 don't: hours, requests, usage maxes and costs on
	// the allocations, and the node and disk assets' values. A successful export
	// can still carry zeros (e.g. a metric the collector stopped scraping, or a
	// failed price lookup), so each must be non-zero. Costs are not checked for
	// correctness: kind uses the default custom pricing.
	It("exports non-zero hours, requests and costs for the agent, stress pod, node and disk", func() {
		nonZero := func(name string, v float64, where string) {
			GinkgoHelper()
			Expect(v).To(BeNumerically(">", 0), "%s in %s", name, where)
		}
		for _, res := range harness.BucketResolutions {
			allocKey, assetKey := snap.Keys["allocations/"+res], snap.Keys["assets/"+res]

			var agent *opencost.Allocation
			for _, a := range snap.Allocations[res].Allocations {
				if p := a.Properties; p != nil && p.Namespace == cfg.Namespace && p.Container == "finops-agent" {
					agent = a
				}
			}
			stress := stressAllocation(snap.Allocations[res])
			Expect(agent).NotTo(BeNil(), "agent container in %s", allocKey)
			Expect(stress).NotTo(BeNil(), "stress container in %s", allocKey)
			for who, a := range map[string]*opencost.Allocation{"agent": agent, "stress": stress} {
				where := who + " allocation in " + allocKey
				nonZero("cpuCoreHours", a.CPUCoreHours, where)
				nonZero("ramByteHours", a.RAMByteHours, where)
				nonZero("cpuCoreRequestAverage", a.CPUCoreRequestAverage, where)
				nonZero("ramByteRequestAverage", a.RAMBytesRequestAverage, where)
				Expect(a.RawAllocationOnly).NotTo(BeNil(), "usage maxes in %s", where)
				nonZero("cpuCoreUsageMax", a.RawAllocationOnly.CPUCoreUsageMax, where)
				nonZero("ramByteUsageMax", a.RawAllocationOnly.RAMBytesUsageMax, where)
				nonZero("cpuCost", a.CPUCost, where)
				nonZero("ramCost", a.RAMCost, where)
				nonZero("totalCost", a.TotalCost(), where)
			}

			var nodes, disks int
			for _, asset := range snap.Assets[res].Assets {
				switch a := asset.(type) {
				case *opencost.Node:
					nodes++
					where := "node asset in " + assetKey
					Expect(a.Properties.Name).To(Equal(cfg.ClusterName+"-control-plane"), where)
					nonZero("cpuCores", a.CPUCores(), where)
					nonZero("ramBytes", a.RAMBytes(), where)
					nonZero("cpuCost", a.CPUCost, where)
					nonZero("ramCost", a.RAMCost, where)
					nonZero("totalCost", a.TotalCost(), where)
				case *opencost.Disk:
					disks++
					where := "disk asset in " + assetKey
					nonZero("bytes", a.Bytes(), where)
					nonZero("byteHours", a.ByteHours, where)
					nonZero("totalCost", a.TotalCost(), where)
				}
			}
			Expect(nodes).To(Equal(1), "kind has one node; node assets in %s", assetKey)
			Expect(disks).To(BeNumerically(">=", 1), "disk assets in %s", assetKey)
		}
	})

	It("reports the stress pod's usage and limits", func() {
		limits := harness.StressLimits()
		for _, res := range harness.BucketResolutions {
			key := snap.Keys["allocations/"+res]
			s := stressAllocation(snap.Allocations[res])
			Expect(s).NotTo(BeNil(), key)
			GinkgoWriter.Printf("stress in %s: cpu %.1fm, memory %.1f MiB\n", key, s.CPUCoreUsageAverage*1000, s.RAMBytesUsageAverage/(1<<20))

			// Averages, not core-hours: a partial window is too short for hours to mean much.
			Expect(s.CPUCoreUsageAverage).To(BeNumerically(">=", minStressCores), "stress cpu in %s", key)
			Expect(s.CPUCoreUsageAverage).To(BeNumerically("<=", maxStressCores), "stress cpu in %s", key)
			Expect(s.RAMBytesUsageAverage).To(BeNumerically(">=", minStressBytes), "stress memory in %s", key)
			Expect(s.RAMBytesUsageAverage).To(BeNumerically("<=", maxStressBytes), "stress memory in %s", key)

			Expect(s.CPUCoreLimitAverage).To(BeNumerically("~", limits.Cpu().AsApproximateFloat64(), 1e-9), "stress cpu limit in %s", key)
			Expect(s.RAMBytesLimitAverage).To(BeNumerically("~", float64(limits.Memory().Value()), 1), "stress memory limit in %s", key)
		}
	})
})

// stressAllocation returns the stress pod's container allocation, or nil.
func stressAllocation(set *opencost.AllocationSet) *opencost.Allocation {
	if set == nil {
		return nil
	}
	for _, a := range set.Allocations {
		if p := a.Properties; p != nil && p.Namespace == harness.StressNamespace && p.Pod == "stress" {
			return a
		}
	}
	return nil
}

func kubeModelNamespace(set *kubemodel.KubeModelSet, name string) *kubemodel.Namespace {
	for _, ns := range set.Namespaces {
		if ns.Name == name {
			return ns
		}
	}
	return nil
}

// expectWindow checks a set covers exactly one window of res, starting on a
// UTC multiple of res (what the agent's set validator enforces).
func expectWindow(w opencost.Window, res, key string) {
	GinkgoHelper()
	Expect(w.Start()).NotTo(BeNil(), key)
	Expect(w.End()).NotTo(BeNil(), key)
	start, end := w.Start().UTC(), w.End().UTC()
	Expect(end.Sub(start)).To(Equal(resolutionDurations[res]), "window length of %s", key)
	Expect(start).To(Equal(start.Truncate(resolutionDurations[res])), "window start of %s is a UTC multiple of %s", key, res)
}

// agentStartTime returns when the Ready agent pod started, or zero.
func agentStartTime(ctx SpecContext) time.Time {
	pods, err := kube.CoreV1().Pods(cfg.Namespace).List(ctx, metav1.ListOptions{LabelSelector: harness.AgentSelector(cfg.ReleaseName)})
	if err != nil {
		return time.Time{}
	}
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil && p.Status.StartTime != nil {
			return p.Status.StartTime.Time
		}
	}
	return time.Time{}
}

// dumpBucket writes the last decoded bucket read and the last read error to
// <ARTIFACTS_DIR>/selfhosted, which CI uploads.
func dumpBucket(snap *harness.BucketSnapshot, readErr error) {
	dir := filepath.Join(cfg.ArtifactsDir, "selfhosted")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		GinkgoWriter.Printf("save bucket snapshot: %v\n", err)
		return
	}
	if readErr != nil {
		_ = os.WriteFile(filepath.Join(dir, "last-read-error.txt"), []byte(readErr.Error()+"\n"), 0o644)
	}
	if snap == nil {
		GinkgoWriter.Printf("no successful bucket read to save; last error in %s\n", dir)
		return
	}
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		GinkgoWriter.Printf("save bucket snapshot: %v\n", err)
		return
	}
	path := filepath.Join(dir, "bucket-snapshot.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		GinkgoWriter.Printf("save bucket snapshot: %v\n", err)
		return
	}
	GinkgoWriter.Printf("saved the decoded bucket read to %s\n", path)
}
