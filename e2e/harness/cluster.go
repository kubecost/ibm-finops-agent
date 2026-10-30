package harness

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	// createWait is passed to `kind create cluster --wait`. kind only logs a
	// warning and exits 0 if this times out, so readiness is checked
	// separately with `kubectl wait` (see waitReady).
	createWait = "2m"

	// nodeReadyTimeout bounds the `kubectl wait` for Ready nodes. This is
	// what replaces e2e.sh's `sleep 2`.
	nodeReadyTimeout = "2m"

	// Image loading occasionally fails transiently; retry a few times, then fail.
	loadAttempts   = 3
	loadRetryDelay = 10 * time.Second

	// deleteTimeout bounds the cleanup after a failed create.
	deleteTimeout = 2 * time.Minute
)

// Cluster is the kind cluster the suite runs against.
type Cluster struct {
	Name string
	// Kubeconfig is an absolute path to a kubeconfig used only by the suite,
	// so ~/.kube/config and the current context are never touched.
	// With KEEP_CLUSTER=1: export KUBECONFIG=<this path> to inspect the cluster.
	Kubeconfig string

	runtime string // "docker" or "podman"
}

// Env returns the extra environment every kind/kubectl/helm call needs.
func (c *Cluster) Env() []string {
	return append([]string{"KUBECONFIG=" + c.Kubeconfig}, providerEnv(c.runtime)...)
}

// providerEnv tells kind which container runtime to use. It is always set, so
// a KIND_EXPERIMENTAL_PROVIDER exported in the user's shell can't override
// cfg.ContainerRuntime.
func providerEnv(runtime string) []string {
	return []string{"KIND_EXPERIMENTAL_PROVIDER=" + runtime}
}

// EnsureCluster returns a ready kind cluster named cfg.ClusterName.
//
// If the cluster exists and cfg.KeepCluster is set, it is reused: its
// kubeconfig is re-exported and its nodes must be Ready. If that fails, it is
// replaced. Otherwise any existing cluster with that name is deleted and a
// fresh one is created from kindest/node:<KubernetesVersion>.
func EnsureCluster(ctx context.Context, cfg Config) (*Cluster, error) {
	if _, err := exec.LookPath("kind"); err != nil {
		return nil, fmt.Errorf("kind not found on PATH (see https://kind.sigs.k8s.io/docs/user/quick-start/#installation): %w", err)
	}
	// The harness deletes clusters by name. Refuse names it doesn't own before
	// running any kind command (ConfigFromEnv checks this too).
	if !strings.HasPrefix(cfg.ClusterName, ClusterNamePrefix) {
		return nil, fmt.Errorf("refusing to manage kind cluster %q: the name must start with %q, because the harness deletes clusters by name", cfg.ClusterName, ClusterNamePrefix)
	}

	dir, err := filepath.Abs(cfg.ArtifactsDir)
	if err != nil {
		return nil, fmt.Errorf("resolve artifacts dir %q: %w", cfg.ArtifactsDir, err)
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create artifacts dir: %w", err)
	}

	c := &Cluster{
		Name:       cfg.ClusterName,
		Kubeconfig: filepath.Join(dir, "kubeconfig"),
		runtime:    cfg.ContainerRuntime,
	}

	out, err := run(ctx, c.Env(), "kind", "get", "clusters")
	if err != nil {
		return nil, fmt.Errorf("list kind clusters: %w", err)
	}
	exists := hasCluster(out, c.Name)

	if exists && cfg.KeepCluster {
		err := c.reuse(ctx)
		if err == nil {
			return c, nil
		}
		// Cancelled (timeout, Ctrl-C): stop, don't start replacing the cluster.
		if ctx.Err() != nil {
			return nil, err
		}
		// The kept cluster is broken (e.g. the podman machine restarted).
		_, _ = fmt.Fprintf(LogWriter, "reused kind cluster %s is not usable, recreating it: %v\n", c.Name, err)
	}
	if exists {
		if err := c.Delete(ctx); err != nil {
			return nil, err
		}
	}

	if _, err := run(ctx, c.Env(), "kind", "create", "cluster",
		"--name", c.Name,
		"--image", "kindest/node:"+cfg.KubernetesVersion,
		"--kubeconfig", c.Kubeconfig,
		"--wait", createWait); err != nil {
		return nil, c.cleanupFailedCreate(ctx, fmt.Errorf("create kind cluster %s: %w", c.Name, err))
	}
	if err := c.waitReady(ctx); err != nil {
		return nil, c.cleanupFailedCreate(ctx, err)
	}
	return c, nil
}

// reuse points the suite's kubeconfig at the existing cluster and checks that
// its nodes are Ready.
func (c *Cluster) reuse(ctx context.Context) error {
	if _, err := run(ctx, c.Env(), "kind", "export", "kubeconfig",
		"--name", c.Name, "--kubeconfig", c.Kubeconfig); err != nil {
		return fmt.Errorf("reuse kind cluster %s: %w", c.Name, err)
	}
	return c.waitReady(ctx)
}

// waitReady fails unless every node reports Ready within nodeReadyTimeout.
func (c *Cluster) waitReady(ctx context.Context) error {
	if _, err := run(ctx, c.Env(), "kubectl", "wait", "--for=condition=Ready", "nodes", "--all",
		"--timeout="+nodeReadyTimeout); err != nil {
		return fmt.Errorf("kind cluster %s nodes not Ready: %w", c.Name, err)
	}
	return nil
}

// cleanupFailedCreate deletes a cluster this run just created (or half-created)
// and returns err, joined with any delete error.
//
// If ctx was cancelled (timeout, Ctrl-C), kind was killed mid-create and may
// have left nodes behind. The delete runs on a fresh context so a later
// KEEP_CLUSTER=1 run can't reuse a half-built cluster.
func (c *Cluster) cleanupFailedCreate(ctx context.Context, err error) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deleteTimeout)
	defer cancel()
	if delErr := c.Delete(cleanupCtx); delErr != nil {
		return errors.Join(err, fmt.Errorf("cleanup after failed create: %w", delErr))
	}
	return err
}

// LoadImage copies image from the local container runtime onto the kind nodes.
// It runs on reused clusters too, so a rebuilt image is always picked up.
func (c *Cluster) LoadImage(ctx context.Context, image string) error {
	switch c.runtime {
	case "docker":
		return retry(ctx, loadAttempts, loadRetryDelay, func() error {
			_, err := run(ctx, c.Env(), "kind", "load", "docker-image", image, "--name", c.Name)
			return err
		})

	case "podman":
		// `kind load docker-image` fails for podman-built images; an archive works.
		tmp, err := os.MkdirTemp("", "e2e-image-")
		if err != nil {
			return fmt.Errorf("create temp dir for image archive: %w", err)
		}
		defer func() { _ = os.RemoveAll(tmp) }()

		archive := filepath.Join(tmp, "image.tar")
		if _, err := run(ctx, nil, "podman", "save", "-o", archive, image); err != nil {
			return fmt.Errorf("save image %s: %w", image, err)
		}
		return retry(ctx, loadAttempts, loadRetryDelay, func() error {
			_, err := run(ctx, c.Env(), "kind", "load", "image-archive", archive, "--name", c.Name)
			return err
		})

	default:
		return fmt.Errorf("unsupported container runtime %q", c.runtime)
	}
}

// Delete removes the kind cluster and its kubeconfig.
func (c *Cluster) Delete(ctx context.Context) error {
	if _, err := run(ctx, c.Env(), "kind", "delete", "cluster", "--name", c.Name); err != nil {
		return fmt.Errorf("delete kind cluster %s: %w", c.Name, err)
	}
	if err := os.Remove(c.Kubeconfig); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove kubeconfig: %w", err)
	}
	return nil
}

// hasCluster reports whether `kind get clusters` output lists name exactly.
func hasCluster(out, name string) bool {
	for line := range strings.SplitSeq(out, "\n") {
		if strings.TrimSpace(line) == name {
			return true
		}
	}
	return false
}

// retry calls fn up to attempts times, waiting delay between tries.
// It stops early if ctx is done and returns the last error.
func retry(ctx context.Context, attempts int, delay time.Duration, fn func() error) error {
	var err error
	for i := 1; i <= attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i == attempts {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("stopped after %d of %d attempts: %w", i, attempts, errors.Join(ctx.Err(), err))
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("failed after %d attempts: %w", attempts, err)
}
