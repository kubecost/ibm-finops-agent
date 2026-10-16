package health

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
)

func get(t *testing.T, h http.HandlerFunc) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(body)
}

// Before any component registers the agent is live and not ready, and /readyz names the phase.
// There is no startup grace on liveness any more: nothing registered means nothing to fail.
func TestStartupPhases(t *testing.T) {
	r := NewRegistry()
	if code, body := get(t, r.LivenessHandler()); code != http.StatusOK || body != "" {
		t.Errorf("/healthz before registration = %d %q, want 200 and an empty body", code, body)
	}
	if code, body := get(t, r.ReadinessHandler()); code != http.StatusServiceUnavailable || !strings.Contains(body, "startup: phase starting") {
		t.Errorf("/readyz before registration = %d %q, want 503 naming the startup phase", code, body)
	}
	if code, _ := get(t, r.StartupHandler()); code != http.StatusServiceUnavailable {
		t.Errorf("/startupz before startup finished = %d, want 503", code)
	}

	r.SetPhase(PhaseDataSource)
	if _, body := get(t, r.ReadinessHandler()); !strings.Contains(body, "startup: phase data_source") {
		t.Errorf("/readyz = %q, want the current phase", body)
	}

	// Startup finishing makes /startupz pass even while a component isn't ready, so an RBAC fault
	// can't turn the startupProbe into a restart loop (D9).
	r.RegisterConditions("informers", conditions{{Type: "informers_unsynced", Message: "pods"}})
	r.SetPhase(PhaseRunning)
	if code, _ := get(t, r.StartupHandler()); code != http.StatusOK {
		t.Errorf("/startupz after startup = %d, want 200", code)
	}
	if code, body := get(t, r.ReadinessHandler()); code != http.StatusServiceUnavailable || body != "informers: informers_unsynced: pods\n" {
		t.Errorf("/readyz = %d %q, want 503 with one line per condition", code, body)
	}
	if code, _ := get(t, r.LivenessHandler()); code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200: a condition is never a liveness failure", code)
	}
}

type conditions []condition.Condition

func (c conditions) Conditions() []condition.Condition { return c }

// fakeComponent returns report, after blocking on block if it is set.
type fakeComponent struct {
	report Report
	block  chan struct{}
	calls  atomic.Int32
}

func (f *fakeComponent) HealthCheck(context.Context, time.Time) Report {
	f.calls.Add(1)
	if f.block != nil {
		<-f.block
	}
	return f.report
}

// A deadlocked component makes /healthz return 503 within the check timeout, the handler never
// hangs, and later probes don't start a second check while the first is stuck.
func TestDeadlockedComponentFailsLiveness(t *testing.T) {
	r := NewRegistry()
	r.SetCheckTimeout(100 * time.Millisecond)
	stuck := &fakeComponent{block: make(chan struct{})}
	defer close(stuck.block)
	r.Register("stuck", stuck)
	r.Register("fine", &fakeComponent{report: Report{Live: true}})
	r.SetPhase(PhaseRunning)

	for probe := 1; probe <= 2; probe++ {
		start := time.Now()
		code, _ := get(t, r.LivenessHandler())
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("probe %d: /healthz took %s with a deadlocked component", probe, elapsed)
		}
		if code != http.StatusServiceUnavailable {
			t.Errorf("probe %d: /healthz = %d with a deadlocked component, want 503", probe, code)
		}
	}
	if n := stuck.calls.Load(); n != 1 {
		t.Errorf("the stuck check was started %d times, want 1", n)
	}
	res := r.Check(context.Background())
	for _, c := range res.Components {
		if c.Name == "fine" && !c.Live {
			t.Errorf("a healthy component was reported not live: %+v", c)
		}
		if c.Name == "stuck" && !strings.Contains(c.NotLiveReason, "did not return") {
			t.Errorf("stuck component reason = %q", c.NotLiveReason)
		}
	}
}

func TestStuckCheckRecovers(t *testing.T) {
	r := NewRegistry()
	r.SetCheckTimeout(50 * time.Millisecond)
	slow := &fakeComponent{report: Report{Live: true}, block: make(chan struct{})}
	r.Register("slow", slow)
	if r.Check(context.Background()).Live {
		t.Fatal("a check that doesn't return in time must count as not live")
	}
	close(slow.block)
	deadline := time.Now().Add(2 * time.Second)
	for !r.Check(context.Background()).Live {
		if time.Now().After(deadline) {
			t.Fatal("liveness did not recover once the check returned")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Probes that arrive together (liveness and readiness) share one check, and neither reports the
// component not live for the other's check being in flight.
func TestConcurrentProbesShareACheck(t *testing.T) {
	r := NewRegistry()
	slow := &fakeComponent{report: Report{Live: true}, block: make(chan struct{})}
	r.Register("slow", slow)
	results := make(chan Result, 2)
	for range 2 {
		go func() { results <- r.Check(context.Background()) }()
	}
	for slow.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	close(slow.block)
	for range 2 {
		if res := <-results; !res.Live {
			t.Errorf("a probe reported not live while sharing a check: %+v", res.Components)
		}
	}
	if n := slow.calls.Load(); n != 1 {
		t.Errorf("two concurrent probes ran the check %d times, want 1", n)
	}
}

type panicker struct{}

func (panicker) HealthCheck(context.Context, time.Time) Report { panic("boom") }

func TestPanickingCheckIsNotLive(t *testing.T) {
	r := NewRegistry()
	r.Register("p", panicker{})
	res := r.Check(context.Background())
	if res.Live || !strings.Contains(res.Components[0].NotLiveReason, "panicked") {
		t.Errorf("result = %+v, want not live with a panic reason", res)
	}
}

type both struct{ fakeComponent }

func (*both) Conditions() []condition.Condition {
	return []condition.Condition{{Type: "should_not_be_used"}}
}

// Any emitter that implements Component or ConditionReporter registers with one call; the
// Kubecost emitter's WAL and bucket conditions (chunk 07) make it not ready, never not live.
func TestRegisterAny(t *testing.T) {
	r := NewRegistry()
	r.SetPhase(PhaseRunning)
	if r.RegisterAny("plain", struct{}{}) {
		t.Error("registered a value with no health interface")
	}
	if !r.RegisterAny("kubecost-emitter", conditions{
		{Type: "wal_unavailable", Reason: "restart_required", Message: "the collector started without its WAL"},
		{Type: "bucket_unavailable", Reason: "write_failed"},
	}) {
		t.Fatal("did not register a ConditionReporter")
	}
	b := &both{fakeComponent{report: Report{Live: true}}}
	if !r.RegisterAny("both", b) {
		t.Fatal("did not register a Component")
	}

	res := r.Check(context.Background())
	if !res.Live || res.Ready {
		t.Errorf("live=%v ready=%v, want live and not ready", res.Live, res.Ready)
	}
	lines := strings.Join(res.NotReadyLines(), "\n")
	for _, want := range []string{"kubecost-emitter: wal_unavailable (restart_required): the collector started without its WAL", "kubecost-emitter: bucket_unavailable (write_failed)"} {
		if !strings.Contains(lines, want) {
			t.Errorf("readiness lines %q lack %q", lines, want)
		}
	}
	if strings.Contains(lines, "should_not_be_used") || b.calls.Load() != 1 {
		t.Errorf("a Component must be checked through HealthCheck, not Conditions: %q", lines)
	}
}

// A condition without Since gets the time it was first seen, kept while it stays active.
func TestSinceFilledAndKept(t *testing.T) {
	now := time.Unix(1000, 0)
	r := NewRegistry()
	r.SetClock(func() time.Time { return now })
	active := conditions{{Type: "node_stats_stale", Reason: "no_recent_collection"}}
	var current atomic.Pointer[conditions]
	current.Store(&active)
	r.Register("node_stats", ComponentFunc(func(context.Context, time.Time) Report {
		return Report{Live: true, Conditions: *current.Load()}
	}))

	first := r.Check(context.Background()).Components[0].Conditions[0].Since
	now = now.Add(time.Minute)
	second := r.Check(context.Background()).Components[0].Conditions[0].Since
	if !first.Equal(time.Unix(1000, 0)) || !second.Equal(first) {
		t.Errorf("Since = %v then %v, want both %v", first, second, time.Unix(1000, 0))
	}

	none := conditions{}
	current.Store(&none)
	r.Check(context.Background())
	current.Store(&active)
	if again := r.Check(context.Background()).Components[0].Conditions[0].Since; !again.Equal(now) {
		t.Errorf("a condition raised again should restart Since: got %v, want %v", again, now)
	}
}

// /status is JSON, redacts URL query strings in messages, and carries component status.
func TestStatusDocument(t *testing.T) {
	r := NewRegistry()
	r.SetPhase(PhaseRunning)
	r.Register("uploader", &fakeComponent{report: Report{
		Live: true,
		Conditions: []condition.Condition{{
			Type:    "upload_auth_failed",
			Message: "PUT https://bucket.s3.amazonaws.com/x.tgz?X-Amz-Signature=SECRETSIG: 403",
		}},
		Status: map[string]int{"backlogFiles": 3},
	}})
	code, body := get(t, r.StatusHandler())
	if code != http.StatusOK {
		t.Fatalf("/status = %d", code)
	}
	if strings.Contains(body, "SECRETSIG") {
		t.Errorf("/status exposes a presigned URL signature: %s", body)
	}
	var doc StatusDoc
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatalf("/status is not JSON: %v\n%s", err, body)
	}
	if doc.Phase != PhaseRunning || doc.Ready || !doc.Live || len(doc.Components) != 1 ||
		doc.Components[0].Conditions[0].Type != "upload_auth_failed" || !strings.Contains(body, `"backlogFiles": 3`) {
		t.Errorf("unexpected /status document: %s", body)
	}
	if _, body := get(t, r.ReadinessHandler()); strings.Contains(body, "SECRETSIG") {
		t.Errorf("/readyz exposes a presigned URL signature: %s", body)
	}
}
