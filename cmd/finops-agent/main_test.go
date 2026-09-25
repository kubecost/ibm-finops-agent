package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ibm/finops-agent/internal/mocks"
	"github.com/ibm/finops-agent/pkg/cluster"
	"github.com/ibm/finops-agent/pkg/health"
	"github.com/ibm/finops-agent/pkg/nodes"
	"github.com/julienschmidt/httprouter"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// The probes and /status are served, and before any component registers the agent is live and
// not ready (no startup grace on liveness).
func TestHealthRoutes(t *testing.T) {
	router := httprouter.New()
	registry := health.NewRegistry()
	registerHealthRoutes(router, registry)

	want := map[string]int{
		"/healthz":  http.StatusOK,
		"/readyz":   http.StatusServiceUnavailable,
		"/startupz": http.StatusServiceUnavailable,
		"/status":   http.StatusOK,
	}
	for path, code := range want {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != code {
			t.Errorf("GET %s = %d, want %d", path, rec.Code, code)
		}
	}
}

// syncingCache is a cluster cache with the pods informer unsynced.
type syncingCache struct{ *mocks.MockClusterCache }

func (syncingCache) StartWithTimeout(context.Context, time.Duration) []schema.GroupVersionResource {
	return nil
}
func (syncingCache) UnsyncedResources() []schema.GroupVersionResource {
	return []schema.GroupVersionResource{{Version: "v1", Resource: "pods"}}
}

type dataSource struct {
	*mocks.MockDataSource
	cache cluster.ClusterCache
	stats nodes.StatSummaryClient
}

func (d dataSource) Cluster() cluster.ClusterCache         { return d.cache }
func (d dataSource) StatsSummary() nodes.StatSummaryClient { return d.stats }

// The informers and node-stats freshness are registered from the data source, and both only
// affect readiness (D1, D9).
func TestRegisterDataSourceHealth(t *testing.T) {
	ds := dataSource{
		MockDataSource: mocks.NewMockDataSource(),
		cache:          syncingCache{mocks.NewMockClusterCache()},
		stats:          nodes.NewCollectionRecorder(mocks.NewMockStatsSummaryClient()),
	}
	registry := health.NewRegistry()
	registry.SetClock(func() time.Time { return time.Unix(0, 0).Add(2 * time.Hour) })
	registerDataSourceHealth(registry, ds, time.Unix(0, 0))
	registry.SetPhase(health.PhaseRunning)

	res := registry.Check(context.Background())
	if !res.Live || res.Ready {
		t.Fatalf("live=%v ready=%v, want live and not ready", res.Live, res.Ready)
	}
	got := map[string]string{}
	for _, c := range res.Components {
		for _, cond := range c.Conditions {
			got[c.Name] = cond.Type
		}
	}
	if got["informers"] != cluster.ConditionInformersUnsynced || got["node_stats"] != nodes.ConditionNodeStatsStale {
		t.Errorf("conditions by component = %v", got)
	}
}
