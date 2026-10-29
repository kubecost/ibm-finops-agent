package harness

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// fakedTools are the CLIs fakeTools replaces. Each fake records every call and
// exits 0 unless a handler says otherwise.
var fakedTools = []string{"kind", "kubectl", "helm"}

// fake configures the scripts fakeTools puts on PATH. Each value is a shell
// snippet run for matching calls; an unset value means "print nothing, exit 0".
// The named fields are shortcuts for the kind calls the cluster tests use.
type fake struct {
	clusters string // stdout of `kind get clusters`, one name per line
	create   string // `kind create cluster ...`
	del      string // `kind delete cluster ...`
	export   string // `kind export kubeconfig ...`
	wait     string // `kubectl wait ...`

	// on maps "<tool> <leading args>" to a snippet, e.g.
	// "helm status": "exit 1" or "kubectl rollout status": `echo "timed out" >&2; exit 1`.
	// A call matches when its args start with the leading args; the longest
	// match wins. Entries here override the named fields.
	on map[string]string
}

// handlers returns per-tool snippets keyed by leading args.
func (f fake) handlers() map[string]map[string]string {
	h := map[string]map[string]string{}
	for _, tool := range fakedTools {
		h[tool] = map[string]string{}
	}
	set := func(key, snippet string) {
		if snippet == "" {
			return
		}
		tool, args, _ := strings.Cut(key, " ")
		if _, ok := h[tool]; !ok {
			panic("fake: unknown tool in " + key)
		}
		h[tool][args] = snippet
	}
	if f.clusters != "" {
		set("kind get clusters", fmt.Sprintf(`printf '%%s\n' '%s'`, f.clusters))
	}
	set("kind create", f.create)
	set("kind delete", f.del)
	set("kind export", f.export)
	set("kubectl wait", f.wait)
	for k, v := range f.on {
		set(k, v)
	}
	return h
}

// failOnce is a fake snippet that fails the first call and succeeds after,
// e.g. a broken kept cluster whose replacement becomes Ready.
const failOnce = `if [ -f "$0.called" ]; then exit 0; fi; touch "$0.called"; exit 1`

// fakeTools puts fake kind, kubectl and helm scripts first on PATH and returns
// a function that reads the calls they received, one line per call:
// "<tool> provider=<KIND_EXPERIMENTAL_PROVIDER> kubeconfig=<KUBECONFIG> args=<args>".
//
// Callers can't be parallel, because this changes PATH with t.Setenv.
func fakeTools(t *testing.T, f fake) (calls func() []string) {
	t.Helper()
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	logLine := fmt.Sprintf(`echo "$(basename "$0") provider=$KIND_EXPERIMENTAL_PROVIDER kubeconfig=$KUBECONFIG args=$*" >> '%s'`, log)

	for tool, hs := range f.handlers() {
		// Longest prefix first, so "rollout status" wins over "rollout".
		prefixes := slices.SortedFunc(func(yield func(string) bool) {
			for p := range hs {
				if !yield(p) {
					return
				}
			}
		}, func(a, b string) int { return cmp.Compare(len(b), len(a)) })

		var script strings.Builder
		fmt.Fprintf(&script, "#!/bin/sh\n%s\ncase \"$*\" in\n", logLine)
		for _, p := range prefixes {
			fmt.Fprintf(&script, "  %q*) %s ;;\n", p, hs[p])
		}
		script.WriteString("esac\n")

		if err := os.WriteFile(filepath.Join(dir, tool), []byte(script.String()), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	return func() []string {
		b, err := os.ReadFile(log)
		if errors.Is(err, os.ErrNotExist) {
			return nil // no tool was called
		}
		if err != nil {
			t.Fatalf("read fake tool log: %v", err)
		}
		return strings.Split(strings.TrimSpace(string(b)), "\n")
	}
}

func fakeConfig(t *testing.T, runtime string) Config {
	return Config{
		ClusterName:       "e2e-fake",
		KubernetesVersion: "v1.34.0",
		ContainerRuntime:  runtime,
		ArtifactsDir:      t.TempDir(),
	}
}

// subcommands returns the tool and first two args of each call, e.g. "kind create cluster".
func subcommands(calls []string) []string {
	var out []string
	for _, c := range calls {
		tool, _, _ := strings.Cut(c, " ")
		_, args, _ := strings.Cut(c, "args=")
		f := strings.Fields(args)
		if len(f) >= 2 {
			out = append(out, tool+" "+f[0]+" "+f[1])
		}
	}
	return out
}

// argsOf returns the args of a recorded call, e.g. "upgrade --install ...".
func argsOf(call string) string {
	_, args, _ := strings.Cut(call, "args=")
	return args
}

// field returns the value of key (e.g. "kubeconfig") in a recorded call.
func field(call, key string) string {
	for f := range strings.FieldsSeq(call) {
		if v, ok := strings.CutPrefix(f, key+"="); ok {
			return v
		}
	}
	return ""
}

// captureLog sends LogWriter to a buffer for this test. Only safe in tests that
// aren't parallel; parallel tests don't start until those finish.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prev := LogWriter
	LogWriter = &buf
	t.Cleanup(func() { LogWriter = prev })
	return &buf
}

// TestFakeTools checks the fake itself: prefix matching, longest match first,
// unknown calls exit 0, and calls are recorded in order.
func TestFakeTools(t *testing.T) {
	calls := fakeTools(t, fake{on: map[string]string{
		"kubectl rollout":        `echo rollout`,
		"kubectl rollout status": `echo status; exit 3`,
		"helm status":            `exit 1`,
	}})

	out, err := run(t.Context(), nil, "kubectl", "rollout", "status", "deployment/x")
	if err == nil || out != "status" {
		t.Errorf("rollout status: out=%q err=%v, want the longer match (exit 3)", out, err)
	}
	if out, err := run(t.Context(), nil, "kubectl", "rollout", "restart", "deployment/x"); err != nil || out != "rollout" {
		t.Errorf("rollout restart: out=%q err=%v, want the shorter match", out, err)
	}
	if _, err := run(t.Context(), nil, "helm", "status", "x"); err == nil {
		t.Error("helm status: want exit 1")
	}
	if out, err := run(t.Context(), nil, "helm", "upgrade", "--install", "x"); err != nil || out != "" {
		t.Errorf("unhandled call: out=%q err=%v, want silent success", out, err)
	}

	want := []string{"kubectl rollout status", "kubectl rollout restart", "helm status x", "helm upgrade --install"}
	if got := subcommands(calls()); !slices.Equal(got, want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
}
