package harness

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRunReturnsTrimmedStdout(t *testing.T) {
	t.Parallel()
	out, err := run(context.Background(), nil, "sh", "-c", "echo '  hello  '")
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Fatalf("out = %q, want %q", out, "hello")
	}
}

func TestRunErrorIncludesCommandAndStderr(t *testing.T) {
	t.Parallel()
	_, err := run(context.Background(), nil, "sh", "-c", "echo boom >&2; exit 3")
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"sh -c", "exit status 3", "boom"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 {
		t.Errorf("error does not wrap *exec.ExitError with code 3: %v", err)
	}
}

func TestRunAddsEnvAndKeepsProcessEnv(t *testing.T) {
	t.Parallel()
	// sh is found through the inherited PATH; E2E_RUN_TEST comes from env.
	out, err := run(context.Background(), []string{"E2E_RUN_TEST=yes"}, "sh", "-c", `echo "$E2E_RUN_TEST"`)
	if err != nil {
		t.Fatal(err)
	}
	if out != "yes" {
		t.Fatalf("out = %q, want %q", out, "yes")
	}
}

// When env and the process environment set the same key, the value passed to
// run wins (os/exec keeps the last duplicate in cmd.Env). Cluster.Env relies on
// this to override a KIND_EXPERIMENTAL_PROVIDER exported in the user's shell.
// Not parallel: t.Setenv changes the process environment.
func TestRunEnvOverridesProcessEnv(t *testing.T) {
	t.Setenv("E2E_RUN_OVERRIDE", "from-shell")
	out, err := run(context.Background(), []string{"E2E_RUN_OVERRIDE=from-harness"}, "sh", "-c", `echo "$E2E_RUN_OVERRIDE"`)
	if err != nil {
		t.Fatal(err)
	}
	if out != "from-harness" {
		t.Fatalf("out = %q, want %q", out, "from-harness")
	}
}

// A context that was cancelled before run is called stops the command at once.
// That's why EnsureCluster's cleanup after a failed create uses a fresh context.
func TestRunWithCancelledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := run(ctx, nil, "sh", "-c", "exit 0")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestRunHonorsContextDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := run(ctx, nil, "sleep", "10")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("run took %s after the deadline; process was not killed", elapsed)
	}
}

func TestRunMissingBinary(t *testing.T) {
	t.Parallel()
	_, err := run(context.Background(), nil, "definitely-not-a-real-binary-e2e")
	if !errors.Is(err, exec.ErrNotFound) {
		t.Fatalf("err = %v, want exec.ErrNotFound", err)
	}
}
