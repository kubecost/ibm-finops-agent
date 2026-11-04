package harness

import (
	"context"
	"slices"
	"strings"
	"testing"

	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const (
	scratchCluster = "cluster-uid"
	execPrefix     = "kubectl exec --namespace " + testNS + " unified-agent-0 -c e2e-reader -- "
)

// lsOf is a fake kubectl handler that lists names, one per line.
func lsOf(names ...string) string {
	return "printf '%s\\n' " + "'" + strings.Join(names, "' '") + "'"
}

// scratchAgent returns an Agent over a fake client holding one Ready agent
// pod, plus the fake CLIs from f. Not parallel (fakeTools sets PATH).
func scratchAgent(t *testing.T, f fake) (Agent, func() []string) {
	t.Helper()
	calls := fakeTools(t, f)
	pod := readyPodIn("unified-agent-0", map[string]string{
		"app.kubernetes.io/name": "finops-agent", "app.kubernetes.io/instance": testRelease,
	}, true)
	return Agent{
		Client:    k8sfake.NewClientset(pod),
		Namespace: testNS,
		Release:   testRelease,
		Cluster:   &Cluster{Name: "e2e-fake", Kubeconfig: "/tmp/e2e-fake/kubeconfig", runtime: "podman"},
	}, calls
}

func TestReadScratch(t *testing.T) {
	dir := scratchRoot + "/" + scratchCluster
	newest := dir + "/1700000002000_2"
	older := dir + "/1700000001000_1"

	t.Run("reads the newest complete sample", func(t *testing.T) {
		a, calls := scratchAgent(t, fake{on: map[string]string{
			"kubectl exec":                                           "exit 1", // anything unexpected fails
			execPrefix + "ls -1 " + scratchRoot:                      lsOf(scratchCluster),
			execPrefix + "ls -1 " + dir:                              lsOf("1700000001000_1", "1700000002000_2", "1700000000500_0"),
			execPrefix + "ls -1 " + newest:                           lsOf("agent-measurement.json", "pods.jsonl"),
			execPrefix + "cat " + newest + "/agent-measurement.json": `echo '{"name":"cldy_agent_status"}'`,
			execPrefix + "cat " + newest + "/pods.jsonl":             `echo '{"kind":"Pod"}'`,
		}})
		files, err := a.ReadScratch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(files) != 2 || string(files["pods.jsonl"]) != `{"kind":"Pod"}` ||
			string(files["agent-measurement.json"]) != `{"name":"cldy_agent_status"}` {
			t.Fatalf("files = %v", files)
		}
		for _, c := range calls() {
			if field(c, "kubeconfig") != a.Cluster.Kubeconfig {
				t.Errorf("call %q did not use the suite's kubeconfig", c)
			}
		}
	})

	t.Run("skips a sample still being written", func(t *testing.T) {
		a, _ := scratchAgent(t, fake{on: map[string]string{
			"kubectl exec":                                          "exit 1",
			execPrefix + "ls -1 " + scratchRoot:                     lsOf(scratchCluster),
			execPrefix + "ls -1 " + dir:                             lsOf("1700000001000_1", "1700000002000_2"),
			execPrefix + "ls -1 " + newest:                          lsOf("pods.jsonl"), // no agent-measurement.json yet
			execPrefix + "ls -1 " + older:                           lsOf("agent-measurement.json", "nodes.jsonl"),
			execPrefix + "cat " + older + "/agent-measurement.json": `echo '{}'`,
			execPrefix + "cat " + older + "/nodes.jsonl":            `echo node`,
		}})
		files, err := a.ReadScratch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := files["nodes.jsonl"]; !ok || len(files) != 2 {
			t.Fatalf("files = %v, want the older complete sample", files)
		}
	})

	t.Run("no complete sample is an error", func(t *testing.T) {
		a, _ := scratchAgent(t, fake{on: map[string]string{
			"kubectl exec":                      "exit 1",
			execPrefix + "ls -1 " + scratchRoot: lsOf(scratchCluster),
			execPrefix + "ls -1 " + dir:         lsOf("1700000002000_2"),
			execPrefix + "ls -1 " + newest:      lsOf("pods.jsonl"),
		}})
		_, err := a.ReadScratch(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no complete sample") {
			t.Fatalf("err = %v, want no complete sample", err)
		}
	})

	t.Run("a file that fails to read is an error", func(t *testing.T) {
		// e.g. the uploader deleted the sample between the listing and the read.
		a, _ := scratchAgent(t, fake{on: map[string]string{
			"kubectl exec":                                           "exit 1",
			execPrefix + "ls -1 " + scratchRoot:                      lsOf(scratchCluster),
			execPrefix + "ls -1 " + dir:                              lsOf("1700000002000_2"),
			execPrefix + "ls -1 " + newest:                           lsOf("agent-measurement.json", "pods.jsonl"),
			execPrefix + "cat " + newest + "/agent-measurement.json": `echo '{}'`,
			execPrefix + "cat " + newest + "/pods.jsonl":             `echo "No such file or directory" >&2; exit 1`,
		}})
		files, err := a.ReadScratch(context.Background())
		if err == nil || !strings.Contains(err.Error(), "No such file or directory") {
			t.Fatalf("err = %v, want the cat error", err)
		}
		if files != nil {
			t.Fatalf("files = %v, want none on error", files)
		}
	})

	t.Run("empty scratch root is an error", func(t *testing.T) {
		a, _ := scratchAgent(t, fake{on: map[string]string{
			"kubectl exec":                      "exit 1",
			execPrefix + "ls -1 " + scratchRoot: "true",
		}})
		_, err := a.ReadScratch(context.Background())
		if err == nil || !strings.Contains(err.Error(), "want 1 cluster directory") {
			t.Fatalf("err = %v, want cluster directory error", err)
		}
	})

	t.Run("kubectl failure is reported", func(t *testing.T) {
		a, _ := scratchAgent(t, fake{on: map[string]string{
			"kubectl exec": `echo "container e2e-reader not found" >&2; exit 1`,
		}})
		_, err := a.ReadScratch(context.Background())
		if err == nil || !strings.Contains(err.Error(), "e2e-reader not found") {
			t.Fatalf("err = %v, want the kubectl error", err)
		}
	})

	t.Run("no Ready pod makes no kubectl call", func(t *testing.T) {
		a, calls := scratchAgent(t, fake{})
		a.Release = "other"
		_, err := a.ReadScratch(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no Ready pod") {
			t.Fatalf("err = %v, want no Ready pod", err)
		}
		if got := calls(); len(got) != 0 {
			t.Fatalf("calls = %v, want none", got)
		}
	})

	t.Run("needs a Cluster", func(t *testing.T) {
		a, _ := scratchAgent(t, fake{})
		a.Cluster = nil
		if _, err := a.ReadScratch(context.Background()); err == nil {
			t.Fatal("want an error without Cluster")
		}
	})
}

func TestReadScratchIgnoresTerminatingPod(t *testing.T) {
	a, _ := scratchAgent(t, fake{})
	p := readyPodIn("unified-agent-0", map[string]string{
		"app.kubernetes.io/name": "finops-agent", "app.kubernetes.io/instance": testRelease,
	}, true)
	p.DeletionTimestamp = &metav1Now
	a.Client = k8sfake.NewClientset(p)
	if _, err := a.readyPod(context.Background()); err == nil {
		t.Fatal("a terminating pod must not be used")
	}
}

func TestMissingScratchFiles(t *testing.T) {
	t.Parallel()
	full := map[string][]byte{}
	for _, req := range requiredScratchFiles {
		full[req+"-x.json"] = nil // empty files count as present
	}
	if got := MissingScratchFiles(full); len(got) != 0 {
		t.Fatalf("complete sample: missing = %v", got)
	}

	// The first sample has no baseline-summary file.
	first := map[string][]byte{}
	for name := range full {
		if !strings.HasPrefix(name, "baseline-summary") {
			first[name] = nil
		}
	}
	first["something-extra.json"] = nil // extra files are fine
	if got := MissingScratchFiles(first); !slices.Equal(got, []string{"baseline-summary"}) {
		t.Fatalf("first sample: missing = %v, want [baseline-summary]", got)
	}

	// Real names: stats-summary and baseline-summary carry the node name.
	real := map[string][]byte{"stats-summary-e2e-v1.34.0-control-plane.json": nil, "pods.jsonl": nil}
	got := MissingScratchFiles(real)
	for _, want := range []string{"baseline-summary", "nodes", "agent-measurement"} {
		if !slices.Contains(got, want) {
			t.Errorf("missing = %v, want it to include %q", got, want)
		}
	}
	if slices.Contains(got, "stats-summary") || slices.Contains(got, "pods") {
		t.Errorf("missing = %v, but stats-summary and pods are present", got)
	}
}

func TestNewestFirst(t *testing.T) {
	t.Parallel()
	got := newestFirst([]string{"1700000001000_1", "junk", "1700000002000_10", "1700000002000_9", "x_y", "1700000000000_0"})
	want := []string{"1700000002000_10", "1700000002000_9", "1700000001000_1", "1700000000000_0"}
	if !slices.Equal(got, want) {
		t.Fatalf("newestFirst = %v, want %v", got, want)
	}
}
