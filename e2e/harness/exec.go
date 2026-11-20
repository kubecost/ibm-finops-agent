package harness

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// LogWriter receives a line for every command run. It defaults to discarding
// output; the suite sets it to GinkgoWriter so CI logs show what ran.
var LogWriter io.Writer = io.Discard

// run executes name with args and returns its trimmed stdout. It is the single
// place the harness shells out (kind, podman, helm, kubectl), so a CLI call can
// later be swapped for a library call without touching callers.
//
// env entries ("KEY=value") are added on top of the current process
// environment, so PATH and HOME are kept. On failure the error includes the
// command line and trimmed stderr; if ctx expired, the error also wraps
// ctx.Err() so callers can check errors.Is(err, context.DeadlineExceeded).
func run(ctx context.Context, env []string, name string, args ...string) (string, error) {
	cmdline := strings.Join(append([]string{name}, args...), " ")
	_, _ = fmt.Fprintf(LogWriter, "+ %s\n", cmdline)

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	// If a child process keeps stdout/stderr open after being killed, stop
	// waiting for it instead of hanging the suite.
	cmd.WaitDelay = 5 * time.Second

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	out := strings.TrimSpace(stdout.String())
	if err == nil {
		return out, nil
	}

	msg := strings.TrimSpace(stderr.String())
	if ctxErr := ctx.Err(); ctxErr != nil {
		return out, fmt.Errorf("%s: %w (%w): %s", cmdline, ctxErr, err, msg)
	}
	return out, fmt.Errorf("%s: %w: %s", cmdline, err, msg)
}
