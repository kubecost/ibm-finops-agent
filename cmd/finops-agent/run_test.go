package main

// Graceful shutdown and startup errors (docs/reliability/FINDINGS.md chunk 10: F-16, F-24 server
// part, F-26 core part).

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/health"
	agenthttp "github.com/ibm/finops-agent/pkg/http"
	"github.com/julienschmidt/httprouter"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// events records the order of shutdown steps.
type events struct {
	mu   sync.Mutex
	list []string
}

func (e *events) add(s string) {
	e.mu.Lock()
	e.list = append(e.list, s)
	e.mu.Unlock()
}

func (e *events) get() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.list)
}

type fakeExporter struct {
	emitter.Exporter
	ev    *events
	block chan struct{} // Stop waits for it, if set
}

func (f *fakeExporter) Stop() {
	if f.block != nil {
		<-f.block
	}
	f.ev.add("exporter")
}

// stoppingEmitter is an emitter with a Stop(ctx) error, like the Cloudability emitter and (after
// chunk 07) the Kubecost one. during runs inside Stop.
type stoppingEmitter struct {
	id     emitter.EmitterID
	ev     *events
	block  chan struct{} // Stop waits for it, ignoring ctx, if set
	during func()
}

func (s *stoppingEmitter) ID() emitter.EmitterID                              { return s.id }
func (s *stoppingEmitter) Init(*emitter.ClusterSnapshot) error                { return nil }
func (s *stoppingEmitter) Emit(context.Context, *emitter.ClusterSnapshot) error { return nil }
func (s *stoppingEmitter) Stop(ctx context.Context) error {
	if s.during != nil {
		s.during()
	}
	if s.block != nil {
		<-s.block
	}
	s.ev.add(string(s.id))
	return nil
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(url)
	if err != nil {
		t.Errorf("GET %s: %v", url, err)
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// Shutdown stops the exporter first, then the emitters (Cloudability's last upload cycle, and
// Kubecost's Stop when it has one), and the HTTP server last: the probes keep answering while the
// emitters drain, with readiness failing.
func TestShutdownOrder(t *testing.T) {
	ev := &events{}
	router := httprouter.New()
	registry := health.NewRegistry()
	registry.SetPhase(health.PhaseRunning)
	registerHealthRoutes(router, registry)

	a := newAgent(registry)
	addr, err := a.listenAndServe(agenthttp.NewHttpServer(router, 0))
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + addr.String()
	if code := get(t, base+"/readyz"); code != http.StatusOK {
		t.Fatalf("/readyz = %d before shutdown, want 200", code)
	}

	probes := func() {
		if code := get(t, base+"/healthz"); code != http.StatusOK {
			t.Errorf("/healthz = %d while emitters drain, want 200", code)
		}
		if code := get(t, base+"/readyz"); code != http.StatusServiceUnavailable {
			t.Errorf("/readyz = %d while shutting down, want 503", code)
		}
	}
	a.exporter = &fakeExporter{ev: ev}
	a.emitters = []emitter.Emitter{
		&stoppingEmitter{id: emitter.KubecostEmitterID, ev: ev, during: probes},
		&stoppingEmitter{id: emitter.CldyEmitterID, ev: ev, during: probes},
	}

	if err := a.shutdown(10 * time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	got := ev.get()
	if len(got) != 3 || got[0] != "exporter" {
		t.Errorf("shutdown steps %v, want the exporter first, then both emitters", got)
	}
	if _, err := net.DialTimeout("tcp", addr.String(), time.Second); err == nil {
		t.Errorf("the HTTP server still accepts connections after shutdown")
	}
}

// A component that ignores the budget doesn't hold the process past it, and doesn't stop the
// other emitter from draining (I5).
func TestShutdownIsBoundedByTheBudget(t *testing.T) {
	ev := &events{}
	hung := make(chan struct{})
	defer close(hung)

	a := newAgent(health.NewRegistry())
	if _, err := a.listenAndServe(agenthttp.NewHttpServer(http.NotFoundHandler(), 0)); err != nil {
		t.Fatal(err)
	}
	a.exporter = &fakeExporter{ev: ev}
	a.emitters = []emitter.Emitter{
		&stoppingEmitter{id: emitter.KubecostEmitterID, ev: ev, block: hung},
		&stoppingEmitter{id: emitter.CldyEmitterID, ev: ev},
	}

	const budget = time.Second
	start := time.Now()
	err := a.shutdown(budget)
	if elapsed := time.Since(start); elapsed > budget+500*time.Millisecond {
		t.Errorf("F-16: shutdown took %s with a hung Kubecost Stop, over the %s budget", elapsed, budget)
	}
	if err == nil {
		t.Errorf("shutdown returned nil although the Kubecost emitter never stopped")
	}
	if got := ev.get(); !slices.Contains(got, string(emitter.CldyEmitterID)) {
		t.Errorf("shutdown steps %v: the Cloudability emitter wasn't drained while Kubecost hung", got)
	}
}

// A port that can't be bound is a startup error, returned before anything else starts.
func TestRunFailsOnBindError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = busy.Close() }()
	prev := listenAddr
	listenAddr = busy.Addr().String()
	defer func() { listenAddr = prev }()

	done := make(chan error, 1)
	go func() { done <- run(context.Background()) }()
	select {
	case err := <-done:
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Errorf("run = %v, want an address-in-use error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("run kept going after failing to bind %s", listenAddr)
	}
}

// agentGoroutines returns the stacks of goroutines started by the agent's own packages, other
// than this package's tests.
func agentGoroutines() []string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	var agent []string
	createdBy := regexp.MustCompile(`created by (github\.com/ibm/finops-agent/\S+)`)
	for _, g := range strings.Split(string(buf), "\n\n") {
		m := createdBy.FindStringSubmatch(g)
		if m == nil || strings.HasPrefix(m[1], "github.com/ibm/finops-agent/cmd/finops-agent.Test") {
			continue
		}
		agent = append(agent, g)
	}
	return agent
}

// runEnv starts an envtest control plane and points the agent's configuration at it: the
// Cloudability emitter on a scratch directory with no upload destination, background node-stats
// collection, and the given overrides.
func runEnv(t *testing.T, overrides map[string]string) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set; run with make test")
	}
	testEnv := &envtest.Environment{}
	if _, err := testEnv.Start(); err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() { _ = testEnv.Stop() })
	admin, err := testEnv.AddUser(envtest.User{Name: "agent", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig, err := admin.KubeConfig()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, kubeconfig, 0o600); err != nil {
		t.Fatal(err)
	}

	env := map[string]string{
		"KUBECONFIG":                       path,
		"OPENCOST_SOURCE_ENABLED":          "false",
		"KUBECOST_EMITTER_ENABLED":         "false",
		"TURBO_EMITTER_ENABLED":            "false",
		"CLOUDABILITY_SCRATCH_DIR":         t.TempDir(),
		"CLUSTER_NAME":                     "shutdown-test",
		"INSECURE":                         "true",
		"NODE_STATS_BG_COLLECTION_ENABLED": "true",
		"EXPORTER_EMISSION_INTERVAL":       "1s",
		"INFORMER_SYNC_TIMEOUT":            "30s",
	}
	for k, v := range overrides {
		env[k] = v
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	prev := listenAddr
	listenAddr = addr
	t.Cleanup(func() { listenAddr = prev })
}

// run serves the probes while running, returns nil within the shutdown budget when its context
// is cancelled, and leaves none of the agent's goroutines behind.
func TestRunShutsDownCleanly(t *testing.T) {
	runEnv(t, map[string]string{"SHUTDOWN_TIMEOUT": "10s"})
	before := len(agentGoroutines())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()

	base := "http://" + listenAddr
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.Get(base + "/startupz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case err := <-done:
			t.Fatalf("run returned during startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the agent did not finish starting within 60s")
		}
		time.Sleep(100 * time.Millisecond)
	}
	if code := get(t, base+"/healthz"); code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", code)
	}
	time.Sleep(2 * time.Second) // let the exporter run a few cycles

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v after SIGTERM, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("F-16: run did not return within 15s of SIGTERM (budget 10s)")
	}

	var leaked []string
	for range 50 {
		if leaked = agentGoroutines(); len(leaked) <= before {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("F-16: %d agent goroutines still running after run returned (%d before):\n%s",
		len(leaked), before, strings.Join(leaked, "\n\n"))
}

// A configuration error found after the data source has started is returned, not a panic, and
// what had started is stopped.
func TestRunReturnsStartupErrors(t *testing.T) {
	// The Kubecost emitter needs the OpenCost cloud provider, which runEnv disables.
	runEnv(t, map[string]string{"KUBECOST_EMITTER_ENABLED": "true"})
	before := len(agentGoroutines())

	done := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("F-26: run panicked: %v", r)
			}
		}()
		done <- run(context.Background())
	}()
	select {
	case err := <-done:
		if err == nil || strings.Contains(err.Error(), "panicked") {
			t.Fatalf("run = %v, want a returned configuration error", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("run did not return the configuration error within 60s")
	}
	var leaked []string
	for range 50 {
		if leaked = agentGoroutines(); len(leaked) <= before {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Errorf("%d agent goroutines still running after run failed:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
}
