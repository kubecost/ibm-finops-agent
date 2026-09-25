package emitter

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	clustercache "github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/nodes"
	"k8s.io/apimachinery/pkg/runtime/schema"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// fakeHeartbeat is a Heartbeat with a fixed status.
type fakeHeartbeat struct {
	stalled bool
	status  ExporterStatus
}

func (f fakeHeartbeat) Stalled(time.Time) bool { return f.stalled }
func (f fakeHeartbeat) Status() ExporterStatus { return f.status }

func readyEmitter(id EmitterID, failures int, err error) EmitterStatus {
	return EmitterStatus{ID: id, State: EmitterReady, ConsecutiveFailures: failures, LastEmitError: err}
}

// Every row of the exporter's health: only a stalled loop fails liveness. Snapshot and emitter
// failures, whatever their cause, only make it not ready (I4, F-38).
func TestExporterHealthComponent(t *testing.T) {
	remote := errors.New(`metrics source unavailable: Get "https://prom/api?token=SECRET": dial tcp: i/o timeout`)
	tests := map[string]struct {
		hb             fakeHeartbeat
		wantLive       bool
		wantConditions []string
	}{
		"healthy": {
			hb:       fakeHeartbeat{status: ExporterStatus{Running: true, Emitters: []EmitterStatus{readyEmitter(CldyEmitterID, 0, nil)}}},
			wantLive: true,
		},
		"stalled loop is not live": {
			hb:       fakeHeartbeat{stalled: true, status: ExporterStatus{Running: true}},
			wantLive: false,
		},
		"two snapshot failures are routine": {
			hb:       fakeHeartbeat{status: ExporterStatus{Running: true, ConsecutiveSnapshotFailures: 2, LastSnapshotError: remote}},
			wantLive: true,
		},
		"a remote snapshot outage is not ready, never not live": {
			hb:             fakeHeartbeat{status: ExporterStatus{Running: true, ConsecutiveSnapshotFailures: 45, LastSnapshotError: remote}},
			wantLive:       true,
			wantConditions: []string{ConditionSnapshotFailing},
		},
		"uninitialised emitter is not ready": {
			hb: fakeHeartbeat{status: ExporterStatus{Running: true, Emitters: []EmitterStatus{
				{ID: KubecostEmitterID, State: EmitterUninitialised, LastInitError: errors.New("failed to read bucket config file")},
				readyEmitter(CldyEmitterID, 0, nil),
			}}},
			wantLive:       true,
			wantConditions: []string{ConditionEmitterUninitialised},
		},
		"failing emitter is not ready": {
			hb: fakeHeartbeat{status: ExporterStatus{Running: true, Emitters: []EmitterStatus{
				readyEmitter(CldyEmitterID, 3, errors.New("no space left on device")),
			}}},
			wantLive:       true,
			wantConditions: []string{ConditionEmitterFailing},
		},
		"stopped exporter is not ready": {
			hb:             fakeHeartbeat{status: ExporterStatus{Running: false}},
			wantLive:       true,
			wantConditions: []string{ConditionExporterStopped},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			report := HealthComponent(tt.hb).HealthCheck(context.Background(), time.Now())
			if report.Live != tt.wantLive {
				t.Errorf("Live = %v, want %v (%s)", report.Live, tt.wantLive, report.NotLiveReason)
			}
			var got []string
			for _, c := range report.Conditions {
				got = append(got, c.Type)
				if strings.Contains(c.Message, "SECRET") {
					t.Errorf("condition message exposes a URL query string: %s", c.Message)
				}
			}
			if strings.Join(got, ",") != strings.Join(tt.wantConditions, ",") {
				t.Errorf("conditions = %v, want %v", got, tt.wantConditions)
			}
		})
	}
}

// fakeSyncCache is a cluster cache whose informers for some resources haven't synced.
type fakeSyncCache struct {
	*mocks.MockClusterCache
	unsynced []schema.GroupVersionResource
}

func (f *fakeSyncCache) StartWithTimeout(context.Context, time.Duration) []schema.GroupVersionResource {
	return f.unsynced
}
func (f *fakeSyncCache) UnsyncedResources() []schema.GroupVersionResource { return f.unsynced }

var _ clustercache.SyncReporter = (*fakeSyncCache)(nil)

// F-12, D9: a snapshot fails while an informer it needs is unsynced, since its list would be empty
// or partial, and doesn't drain short-lived pods; an unsynced informer no emitter needs doesn't
// block it.
func TestSnapshotWaitsForNeededInformers(t *testing.T) {
	pdbs := schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}
	deployments := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	kconfig := NewKubernetesSnapshotConfig()
	kconfig.Deployments = true
	config := &SnapshotConfig{KubernetesSnapshot: kconfig}

	cache := &fakeSyncCache{MockClusterCache: mocks.NewMockClusterCache(), unsynced: []schema.GroupVersionResource{pdbs}}
	if _, err := snapshotKubernetes(cache, config); err != nil {
		t.Errorf("an unsynced informer for a resource no emitter needs failed the snapshot: %v", err)
	}

	cache.unsynced = []schema.GroupVersionResource{deployments, pdbs}
	_, err := snapshotKubernetes(cache, config)
	if err == nil || !strings.Contains(err.Error(), "apps/deployments") || strings.Contains(err.Error(), "poddisruptionbudgets") {
		t.Errorf("snapshot with the deployments informer unsynced: err = %v, want one naming apps/deployments only", err)
	}
}

// cachedStats serves stats collected at a fixed time, as background collection does.
type cachedStats struct{ collected time.Time }

func (c cachedStats) GetNodeData() ([]*stats.Summary, error) {
	return []*stats.Summary{{}}, nil
}
func (c cachedStats) GetCachedNodeData() ([]*stats.Summary, time.Time, error) {
	return []*stats.Summary{{}}, c.collected, nil
}

var _ nodes.CachedStatSummaryClient = cachedStats{}

// F-21: background stats carry their collection time into the snapshot, so emitters don't take
// them for fresh.
func TestSnapshotNodeStatsCarryCollectionTime(t *testing.T) {
	collected := time.Now().Add(-40 * time.Minute)
	ns, err := snapshotNodeStats(context.Background(), cachedStats{collected: collected})
	if err != nil {
		t.Fatal(err)
	}
	if !ns.CollectedAt.Equal(collected) {
		t.Errorf("F-21: CollectedAt = %v, want the background collection time %v", ns.CollectedAt, collected)
	}
}
