package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestHasCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		out  string
		want bool
	}{
		{name: "empty output", out: "", want: false},
		{name: "only cluster", out: "e2e-v1.34.0", want: true},
		{name: "one of several", out: "kind\ne2e-v1.34.0\nother", want: true},
		{name: "prefix of another name", out: "e2e-v1.34.0-old", want: false},
		{name: "surrounding whitespace", out: "  e2e-v1.34.0  \n", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := hasCluster(tt.out, "e2e-v1.34.0"); got != tt.want {
				t.Fatalf("hasCluster(%q) = %v, want %v", tt.out, got, tt.want)
			}
		})
	}
}

func TestClusterEnv(t *testing.T) {
	t.Parallel()
	podman := (&Cluster{Kubeconfig: "/tmp/kc", runtime: "podman"}).Env()
	if !slices.Contains(podman, "KUBECONFIG=/tmp/kc") || !slices.Contains(podman, "KIND_EXPERIMENTAL_PROVIDER=podman") {
		t.Fatalf("podman env = %v", podman)
	}
	// docker is set explicitly so an inherited KIND_EXPERIMENTAL_PROVIDER=podman can't win.
	docker := (&Cluster{Kubeconfig: "/tmp/kc", runtime: "docker"}).Env()
	if !slices.Equal(docker, []string{"KUBECONFIG=/tmp/kc", "KIND_EXPERIMENTAL_PROVIDER=docker"}) {
		t.Fatalf("docker env = %v", docker)
	}
}

func TestRetry(t *testing.T) {
	t.Parallel()
	errBoom := errors.New("boom")

	t.Run("succeeds first try", func(t *testing.T) {
		t.Parallel()
		calls := 0
		err := retry(context.Background(), 3, time.Millisecond, func() error { calls++; return nil })
		if err != nil || calls != 1 {
			t.Fatalf("err = %v, calls = %d", err, calls)
		}
	})

	t.Run("succeeds after failures", func(t *testing.T) {
		t.Parallel()
		calls := 0
		err := retry(context.Background(), 3, time.Millisecond, func() error {
			calls++
			if calls < 3 {
				return errBoom
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("err = %v, calls = %d", err, calls)
		}
	})

	t.Run("returns last error when all fail", func(t *testing.T) {
		t.Parallel()
		calls := 0
		err := retry(context.Background(), 3, time.Millisecond, func() error { calls++; return errBoom })
		if !errors.Is(err, errBoom) || calls != 3 || !strings.Contains(err.Error(), "3 attempts") {
			t.Fatalf("err = %v, calls = %d", err, calls)
		}
	})

	t.Run("stops when context is done", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		err := retry(ctx, 5, time.Hour, func() error { calls++; cancel(); return errBoom })
		if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) || calls != 1 {
			t.Fatalf("err = %v, calls = %d", err, calls)
		}
	})
}

func TestLoadImageUnsupportedRuntime(t *testing.T) {
	t.Parallel()
	err := (&Cluster{runtime: "containerd"}).LoadImage(context.Background(), "ghcr.io/x/y:1")
	if err == nil || !strings.Contains(err.Error(), "unsupported container runtime") {
		t.Fatalf("err = %v", err)
	}
}

// Not parallel: t.Setenv changes the process environment.
func TestEnsureClusterWithoutKind(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := EnsureCluster(context.Background(), Config{ClusterName: "x", ArtifactsDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "kind not found") {
		t.Fatalf("err = %v, want kind not found", err)
	}
}

// fake configures the fake `kind` and `kubectl` that fakeTools puts on PATH.
// Each field is a shell snippet; empty means "exit 0".
type fake struct {
	clusters string // stdout of `kind get clusters`, one name per line
	create   string // `kind create cluster ...`
	del      string // `kind delete cluster ...`
	export   string // `kind export kubeconfig ...`
	wait     string // `kubectl wait ...`
}

// failOnce is a fake snippet that fails the first call and succeeds after,
// e.g. a broken kept cluster whose replacement becomes Ready.
const failOnce = `if [ -f "$0.called" ]; then exit 0; fi; touch "$0.called"; exit 1`

// fakeTools puts fake `kind` and `kubectl` scripts first on PATH and returns a
// function that reads the calls they received, one line per call:
// "<tool> provider=<KIND_EXPERIMENTAL_PROVIDER> kubeconfig=<KUBECONFIG> args=<args>".
//
// Callers can't be parallel, because this changes PATH with t.Setenv.
func fakeTools(t *testing.T, f fake) (calls func() []string) {
	t.Helper()
	or := func(s string) string {
		if s == "" {
			return "exit 0"
		}
		return s
	}
	dir := t.TempDir()
	log := filepath.Join(dir, "calls.log")
	logLine := fmt.Sprintf(`echo "$(basename "$0") provider=$KIND_EXPERIMENTAL_PROVIDER kubeconfig=$KUBECONFIG args=$*" >> '%s'`, log)

	kind := fmt.Sprintf(`#!/bin/sh
%s
case "$1" in
  get) printf '%%s\n' '%s' ;;
  create) %s ;;
  delete) %s ;;
  export) %s ;;
esac
`, logLine, f.clusters, or(f.create), or(f.del), or(f.export))
	kubectl := fmt.Sprintf("#!/bin/sh\n%s\n%s\n", logLine, or(f.wait))

	for name, script := range map[string]string{"kind": kind, "kubectl": kubectl} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o755); err != nil {
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

// field returns the value of key (e.g. "kubeconfig") in a recorded call.
func field(call, key string) string {
	for f := range strings.FieldsSeq(call) {
		if v, ok := strings.CutPrefix(f, key+"="); ok {
			return v
		}
	}
	return ""
}

// TestEnsureClusterCalls checks the exact kind/kubectl sequence and result of
// EnsureCluster for each scenario. Together the rows document its behavior:
//   - kind's --wait exits 0 even on timeout, so Ready is always checked with kubectl.
//   - A cluster this run created (or half-created) is deleted if anything fails.
//   - A reused cluster (KEEP_CLUSTER=1) is kept if usable, and replaced if not.
func TestEnsureClusterCalls(t *testing.T) {
	const (
		get    = "kind get clusters"
		create = "kind create cluster"
		del    = "kind delete cluster"
		export = "kind export kubeconfig"
		wait   = "kubectl wait --for=condition=Ready"
	)
	tests := []struct {
		name      string
		fake      fake
		keep      bool
		wantErr   []string // substrings; nil means success
		wantCalls []string
		wantLog   string // substring of LogWriter output; empty means not checked
	}{
		{
			name:      "fresh create waits for Ready",
			wantCalls: []string{get, create, wait},
		},
		{
			name:      "existing harness cluster is recreated",
			fake:      fake{clusters: "kind\ne2e-fake"},
			wantCalls: []string{get, del, create, wait},
		},
		{
			name:      "reuse with KEEP_CLUSTER waits for Ready",
			fake:      fake{clusters: "e2e-fake"},
			keep:      true,
			wantCalls: []string{get, export, wait},
		},
		{
			name:      "failed create is deleted",
			fake:      fake{create: `echo "boom" >&2; exit 1`},
			wantErr:   []string{"create kind cluster e2e-fake", "boom"},
			wantCalls: []string{get, create, del},
		},
		{
			name:      "created cluster not Ready is deleted",
			fake:      fake{wait: `echo "timed out waiting for the condition" >&2; exit 1`},
			wantErr:   []string{"not Ready", "timed out waiting"},
			wantCalls: []string{get, create, wait, del},
		},
		{
			name:      "reused cluster not Ready is recreated",
			fake:      fake{clusters: "e2e-fake", wait: failOnce},
			keep:      true,
			wantCalls: []string{get, export, wait, del, create, wait},
			wantLog:   "reused kind cluster e2e-fake is not usable, recreating it",
		},
		{
			name:      "reused cluster whose kubeconfig can't be exported is recreated",
			fake:      fake{clusters: "e2e-fake", export: `echo "node container not running" >&2; exit 1`},
			keep:      true,
			wantCalls: []string{get, export, del, create, wait},
			wantLog:   "node container not running",
		},
		{
			name:      "replacement for a broken reused cluster that also fails is deleted",
			fake:      fake{clusters: "e2e-fake", wait: `exit 1`},
			keep:      true,
			wantErr:   []string{"not Ready"},
			wantCalls: []string{get, export, wait, del, create, wait, del},
		},
		{
			name:      "failed cleanup reports both errors",
			fake:      fake{create: `exit 1`, del: `echo "delete broke" >&2; exit 1`},
			wantErr:   []string{"create kind cluster", "cleanup after failed create", "delete broke"},
			wantCalls: []string{get, create, del},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := fakeTools(t, tt.fake)
			cfg := fakeConfig(t, "podman")
			cfg.KeepCluster = tt.keep
			log := captureLog(t)

			c, err := EnsureCluster(context.Background(), cfg)

			if tt.wantLog != "" && !strings.Contains(log.String(), tt.wantLog) {
				t.Errorf("log %q does not contain %q", log.String(), tt.wantLog)
			}

			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("expected an error")
				}
				if c != nil {
					t.Errorf("cluster = %+v, want nil on error", c)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err, want)
					}
				}
			}

			got := calls()
			if !slices.Equal(subcommands(got), tt.wantCalls) {
				t.Fatalf("calls = %v\nwant    %v", subcommands(got), tt.wantCalls)
			}
			// Every call uses the suite's own kubeconfig, never ~/.kube/config,
			// and the Ready wait covers all nodes with a timeout.
			wantKC := filepath.Join(cfg.ArtifactsDir, "kubeconfig")
			for _, call := range got {
				if field(call, "kubeconfig") != wantKC {
					t.Errorf("call %q used KUBECONFIG=%q, want %q", call, field(call, "kubeconfig"), wantKC)
				}
				if strings.HasPrefix(call, "kubectl ") && !strings.Contains(call, "nodes --all --timeout=") {
					t.Errorf("kubectl call %q should wait on all nodes with a timeout", call)
				}
			}
		})
	}
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

// A cancelled context while checking a reused cluster stops the run. It must
// not go on to delete and recreate the user's kept cluster.
func TestEnsureClusterDoesNotRecreateWhenCancelledDuringReuse(t *testing.T) {
	calls := fakeTools(t, fake{clusters: "e2e-fake", wait: `exec sleep 30`})
	cfg := fakeConfig(t, "podman")
	cfg.KeepCluster = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := EnsureCluster(ctx, cfg)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	got := subcommands(calls())
	if slices.Contains(got, "kind delete cluster") || slices.Contains(got, "kind create cluster") {
		t.Fatalf("calls = %v; a cancelled reuse must not delete or recreate", got)
	}
}

// When the context is cancelled mid-create (suite timeout, Ctrl-C), kind is
// killed and the delete still runs on a fresh context.
func TestEnsureClusterDeletesAfterCancelledCreate(t *testing.T) {
	// exec so the sleep itself is killed, not just the shell around it.
	calls := fakeTools(t, fake{create: `exec sleep 30`})

	// Long enough that `kind get clusters` finishes even on a loaded machine
	// with -race, so the deadline lands during create (which sleeps 30s).
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := EnsureCluster(ctx, fakeConfig(t, "podman"))

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("EnsureCluster took %s; kind create was not killed", elapsed)
	}
	if got := subcommands(calls()); !slices.Contains(got, "kind delete cluster") {
		t.Fatalf("calls = %v, want a delete after the cancelled create", got)
	}
}

// Every kind call gets the provider for cfg.ContainerRuntime, even when the
// user's shell exports a different KIND_EXPERIMENTAL_PROVIDER.
func TestEnsureClusterProviderOverridesShell(t *testing.T) {
	for _, tt := range []struct{ shell, runtime string }{
		{shell: "podman", runtime: "docker"},
		{shell: "docker", runtime: "podman"},
	} {
		t.Run(tt.runtime, func(t *testing.T) {
			t.Setenv("KIND_EXPERIMENTAL_PROVIDER", tt.shell)
			calls := fakeTools(t, fake{})

			if _, err := EnsureCluster(context.Background(), fakeConfig(t, tt.runtime)); err != nil {
				t.Fatal(err)
			}
			for _, c := range calls() {
				if strings.HasPrefix(c, "kind ") && field(c, "provider") != tt.runtime {
					t.Errorf("call %q used the wrong provider, want %s", c, tt.runtime)
				}
			}
		})
	}
}

// A cluster name without the harness prefix (e.g. the user's default "kind"
// cluster) is refused before any kind command runs, so it can't be deleted.
func TestEnsureClusterRefusesNonHarnessName(t *testing.T) {
	for _, name := range []string{"kind", "dev", "my-e2e-cluster"} {
		t.Run(name, func(t *testing.T) {
			calls := fakeTools(t, fake{clusters: name})
			cfg := fakeConfig(t, "podman")
			cfg.ClusterName = name

			_, err := EnsureCluster(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), ClusterNamePrefix) {
				t.Fatalf("err = %v, want a refusal mentioning %q", err, ClusterNamePrefix)
			}
			if got := calls(); got != nil {
				t.Fatalf("calls = %v, want none", subcommands(got))
			}
		})
	}
}
