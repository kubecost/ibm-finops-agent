package harness

import (
	"cmp"
	"context"
	"fmt"
	"path"
	"slices"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	scratchContainer   = "e2e-reader"
	scratchRoot        = "/opt/finops-agent/scratch"
	sampleCompleteFile = "agent-measurement.json"
)

// requiredScratchFiles should mirror the list of files uploaded by the uploader
var requiredScratchFiles = []string{
	"baseline-summary", "stats-summary", "statefulsets", "services", "replicationcontrollers",
	"replicasets", "pods", "persistentvolumes", "persistentvolumeclaims", "nodes", "namespaces",
	"jobs", "deployments", "daemonsets", "agent-measurement",
}

// MissingScratchFiles should return any missing sample files. Notably, the
// first sample doesn't have a baseline-summary
func MissingScratchFiles(files map[string][]byte) []string {
	var missing []string
	for _, req := range requiredScratchFiles {
		found := false
		for name := range files {
			if strings.HasPrefix(name, req) {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, req)
		}
	}
	return missing
}

// ReadScratch returns the files of the newest complete Cloudability sample,
// keyed by file name (e.g. "pods.jsonl")
func (a Agent) ReadScratch(ctx context.Context) (map[string][]byte, error) {
	if a.Cluster == nil {
		return nil, fmt.Errorf("read scratch: Agent.Cluster is not set")
	}

	pod, err := a.readyPod(ctx)
	if err != nil {
		return nil, fmt.Errorf("read scratch: %w", err)
	}

	// Each exec gets its own timeout, so one hung call can't use up the caller's.
	exec := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, defaultExecTimeout)
		defer cancel()
		base := []string{"exec", "--namespace", a.Namespace, pod, "-c", scratchContainer, "--"}
		return run(ctx, a.Cluster.Env(), "kubectl", append(base, args...)...)
	}

	out, err := exec("ls", "-1", scratchRoot)
	if err != nil {
		return nil, fmt.Errorf("read scratch: %w", err)
	}

	clusters := strings.Fields(out)
	if len(clusters) != 1 {
		return nil, fmt.Errorf("read scratch: want 1 cluster directory in %s, found %v", scratchRoot, clusters)
	}
	clusterDir := path.Join(scratchRoot, clusters[0])

	out, err = exec("ls", "-1", clusterDir)
	if err != nil {
		return nil, fmt.Errorf("read scratch: %w", err)
	}

	samples := newestFirst(strings.Fields(out))
	for _, s := range samples {
		dir := path.Join(clusterDir, s)
		out, err := exec("ls", "-1", dir)
		if err != nil {
			return nil, fmt.Errorf("read scratch: %w", err)
		}

		names := strings.Fields(out)
		if !slices.Contains(names, sampleCompleteFile) {
			continue
		}

		files := make(map[string][]byte, len(names))
		for _, name := range names {
			body, err := exec("cat", path.Join(dir, name))
			if err != nil {
				return nil, fmt.Errorf("read scratch: %w", err)
			}
			files[name] = []byte(body)
		}

		return files, nil
	}
	return nil, fmt.Errorf("read scratch: no complete sample (one with %s) among %d in %s", sampleCompleteFile, len(samples), clusterDir)
}

// readyPod returns the name of a Ready, non-terminating agent pod.
func (a Agent) readyPod(ctx context.Context) (string, error) {
	sel := AgentSelector(a.Release)
	pods, err := a.Client.CoreV1().Pods(a.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return "", fmt.Errorf("find agent pod: %w", err)
	}

	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp == nil && isPodReady(p) {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("find agent pod: no Ready pod matching %q in %s", sel, a.Namespace)
}

// newestFirst sorts sample directory names ("<unixMilli>_<n>") newest first
// and drops names that don't match
func newestFirst(names []string) []string {
	type sample struct {
		name  string
		ms, n int64
	}

	var samples []sample
	for _, name := range names {
		msStr, nStr, ok := strings.Cut(name, "_")
		if !ok {
			continue
		}
		ms, err1 := strconv.ParseInt(msStr, 10, 64)
		n, err2 := strconv.ParseInt(nStr, 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		samples = append(samples, sample{name, ms, n})
	}

	slices.SortFunc(samples, func(a, b sample) int {
		return cmp.Or(cmp.Compare(b.ms, a.ms), cmp.Compare(b.n, a.n))
	})
	out := make([]string, len(samples))
	for i, s := range samples {
		out[i] = s.name
	}
	return out
}
