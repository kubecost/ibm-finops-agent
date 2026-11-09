//go:build e2e

package e2e_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"github.com/ibm/finops-agent/e2e/harness"
)

const (
	clusterID                    = "e2e"
	cloudabilityEmissionInterval = "10s"
	cloudabilityRegion           = "staging"

	// sampleTimeout bounds wait for first sample; should be under 10 minutes
	// since that is the default for the uploader sending the samples
	sampleTimeout = 5 * time.Minute
)

// The Cloudability emitter writes a sample directory into the agent's scratch volume
var _ = Describe("Cloudability sample", Ordered, Label("cloudability"), func() {
	var files map[string][]byte

	BeforeAll(func(ctx SpecContext) {
		agent := harness.Agent{Client: kube, Namespace: cfg.Namespace, Release: cfg.ReleaseName, Cluster: cluster}
		Eventually(func(g Gomega) {
			var err error
			files, err = agent.ReadScratch(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(harness.MissingScratchFiles(files)).To(BeEmpty(), "sample files: %v", fileNames(files))
		}).WithContext(ctx).WithTimeout(sampleTimeout).WithPolling(10 * time.Second).Should(Succeed())
		GinkgoWriter.Printf("sample files: %v\n", fileNames(files))
	}, NodeTimeout(sampleTimeout+time.Minute))

	It("has every file the uploader requires", func() {
		Expect(harness.MissingScratchFiles(files)).To(BeEmpty())
	})

	It("has the node", func() {
		nodes := decodeJSONL[corev1.Node](files["nodes.jsonl"])
		Expect(nodes).To(HaveLen(1), "kind has one node")
		Expect(nodes[0].Name).To(Equal(cfg.ClusterName + "-control-plane"))
	})

	It("has the namespaces", func() {
		var names []string
		for _, ns := range decodeJSONL[corev1.Namespace](files["namespaces.jsonl"]) {
			names = append(names, ns.Name)
		}
		Expect(names).To(ContainElements(harness.StressNamespace, cfg.Namespace))
	})

	It("has the agent and stress pods", func() {
		var stress *corev1.Pod
		var names []string
		pods := decodeJSONL[corev1.Pod](files["pods.jsonl"])
		for i := range pods {
			p := &pods[i]
			names = append(names, p.Namespace+"/"+p.Name)
			if p.Namespace == harness.StressNamespace && p.Name == "stress" {
				stress = p
			}
		}
		Expect(names).To(ContainElement(HavePrefix(cfg.Namespace + "/" + cfg.ReleaseName)))

		Expect(stress).NotTo(BeNil(), "stress pod among %v", names)
		Expect(stress.Spec.Containers).To(HaveLen(1))
		limits := stress.Spec.Containers[0].Resources.Limits
		for name, want := range harness.StressLimits() {
			got := limits[name]
			Expect(got.Cmp(want)).To(BeZero(), "stress %s limit = %s, want %s", name, got.String(), want.String())
		}
	})

	It("reports the stress pod's usage in the kubelet stats", func() {
		var summaries []stats.Summary
		for name, body := range files {
			if strings.HasPrefix(name, "stats-summary") {
				summaries = append(summaries, decodeJSON[stats.Summary](body))
			}
		}
		Expect(summaries).To(HaveLen(1), "one stats-summary file per node")
		Expect(summaries[0].Node.NodeName).To(Equal(cfg.ClusterName + "-control-plane"))

		var pod *stats.PodStats
		for i := range summaries[0].Pods {
			ref := summaries[0].Pods[i].PodRef
			if ref.Namespace == harness.StressNamespace && ref.Name == "stress" {
				pod = &summaries[0].Pods[i]
			}
		}
		Expect(pod).NotTo(BeNil(), "stress pod in the stats-summary")
		Expect(pod.CPU).NotTo(BeNil())
		Expect(pod.CPU.UsageNanoCores).NotTo(BeNil())
		Expect(pod.Memory).NotTo(BeNil())
		Expect(pod.Memory.WorkingSetBytes).NotTo(BeNil())

		// The stress pod is loaded with StressCPUMillicores (100m) and StressMemoryMegabytes (128MB).
		// Validate that the stats reported by the agent fall within expected bounds.
		const (
			minNanoCores = 50 * 1_000_000  // 50m
			maxNanoCores = 500 * 1_000_000 // 500m (pod limit)
			minMemBytes  = 100 * 1024 * 1024 // 100 MiB
			maxMemBytes  = 256 * 1024 * 1024 // 256 MiB (pod limit)
		)
		Expect(*pod.CPU.UsageNanoCores).To(BeNumerically(">=", minNanoCores), "cpu usageNanoCores")
		Expect(*pod.CPU.UsageNanoCores).To(BeNumerically("<=", maxNanoCores), "cpu usageNanoCores")
		Expect(*pod.Memory.WorkingSetBytes).To(BeNumerically(">=", minMemBytes), "memory workingSetBytes")
		Expect(*pod.Memory.WorkingSetBytes).To(BeNumerically("<=", maxMemBytes), "memory workingSetBytes")
	})

	It("reports a healthy agent in agent-measurement.json", func() {
		m := decodeJSON[agentMeasurement](files["agent-measurement.json"])

		Expect(m.Name).To(Equal("cldy_agent_status"))
		Expect(m.Metrics).To(HaveKeyWithValue("nodes_failed", 0))
		Expect(m.Errors).To(BeEmpty())
		Expect(m.Tags).To(HaveKeyWithValue("cluster_uid", Not(BeEmpty())))
		Expect(m.Values).To(HaveKeyWithValue("upload_region", cloudabilityRegion))
		Expect(m.Values).To(HaveKeyWithValue("emission_interval", cloudabilityEmissionInterval))
		Expect(m.Values).To(HaveKeyWithValue("cluster_name", clusterID))
	})
})

// agentMeasurement is the part of agent-measurement.json the specs read
type agentMeasurement struct {
	Name    string            `json:"name"`
	Metrics map[string]int    `json:"metrics"`
	Tags    map[string]string `json:"tags"`
	Values  map[string]string `json:"values"`
	Errors  []json.RawMessage `json:"errors"`
}

func fileNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	return names
}

// decodeJSON decodes one JSON document, failing the spec if it is not valid.
func decodeJSON[T any](body []byte) T {
	GinkgoHelper()
	var v T
	Expect(json.Unmarshal(body, &v)).To(Succeed(), "decode %.200s", body)
	return v
}

// decodeJSONL decodes a stream of JSON objects (the agent writes them
// back-to-back, not necessarily one per line). An empty file gives no items.
func decodeJSONL[T any](body []byte) []T {
	GinkgoHelper()
	var out []T
	dec := json.NewDecoder(bytes.NewReader(body))
	for {
		var v T
		err := dec.Decode(&v)
		if errors.Is(err, io.EOF) {
			return out
		}
		Expect(err).NotTo(HaveOccurred(), "decode object %d", len(out)+1)
		out = append(out, v)
	}
}
