package nodes

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// switchableClient returns one summary, or no stats and an error while failing is set.
type switchableClient struct {
	failing atomic.Bool
	calls   atomic.Int32
}

func (c *switchableClient) GetNodeData() ([]*stats.Summary, error) {
	c.calls.Add(1)
	if c.failing.Load() {
		return nil, errors.New("every kubelet unreachable")
	}
	return []*stats.Summary{{Node: stats.NodeStats{NodeName: "n1"}}}, nil
}

// F-21: in background mode, once every collection fails, the provider keeps serving the last
// stats with no error. It must say how old they are, and freshness must be measured where they are
// collected (D1).
func TestReproF21BackgroundStatsReportTheirAge(t *testing.T) {
	client := &switchableClient{}
	provider := NewNodeStatsSummaryProvider(client)
	if !provider.Start(5 * time.Millisecond) {
		t.Fatal("provider did not start")
	}
	defer provider.Stop()
	collected := provider.LastCollection()
	if collected.IsZero() {
		t.Fatal("the initial collection was not recorded")
	}

	client.failing.Store(true)
	for start := client.calls.Load(); client.calls.Load() < start+3; {
		time.Sleep(time.Millisecond)
	}
	data, at, err := provider.GetCachedNodeData()
	if err != nil || len(data) != 1 {
		t.Fatalf("GetCachedNodeData = %d stats, %v; want the last stats served", len(data), err)
	}
	if !at.Equal(collected) || !provider.LastCollection().Equal(collected) {
		t.Errorf("F-21: stale stats reported as collected at %v (last collection %v), want %v", at, provider.LastCollection(), collected)
	}

	maxAge := 30 * time.Minute
	component := FreshnessComponent(provider, maxAge, collected)
	if r := component.HealthCheck(context.Background(), collected.Add(maxAge-time.Second)); len(r.Conditions) != 0 || !r.Live {
		t.Errorf("stats within the threshold: %+v", r)
	}
	r := component.HealthCheck(context.Background(), collected.Add(maxAge+time.Second))
	if !r.Live || len(r.Conditions) != 1 || r.Conditions[0].Type != ConditionNodeStatsStale {
		t.Errorf("F-21: stats past the threshold should be %s and live (D1): %+v", ConditionNodeStatsStale, r)
	}
}

// On the foreground path the recorder counts only collections that returned stats.
func TestCollectionRecorder(t *testing.T) {
	client := &switchableClient{}
	now := time.Unix(1000, 0)
	recorder := NewCollectionRecorder(client)
	recorder.now = func() time.Time { return now }
	started := now

	component := FreshnessComponent(recorder, 30*time.Minute, started)
	if r := component.HealthCheck(context.Background(), started.Add(31*time.Minute)); len(r.Conditions) != 1 || r.Conditions[0].Reason != "never_collected" {
		t.Errorf("no collection 31 min after start: %+v", r.Conditions)
	}

	if _, err := recorder.GetNodeDataContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !recorder.LastCollection().Equal(now) {
		t.Errorf("LastCollection = %v, want %v", recorder.LastCollection(), now)
	}
	client.failing.Store(true)
	now = now.Add(time.Hour)
	_, _ = recorder.GetNodeData()
	if !recorder.LastCollection().Equal(time.Unix(1000, 0)) {
		t.Errorf("a failed collection moved LastCollection to %v", recorder.LastCollection())
	}
	if r := component.HealthCheck(context.Background(), now); len(r.Conditions) != 1 || r.Conditions[0].Reason != "no_recent_collection" || !r.Live {
		t.Errorf("an hour without stats: %+v", r)
	}
}
