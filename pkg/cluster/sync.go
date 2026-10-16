package cluster

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/ibm/finops-agent/pkg/health"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ConditionInformersUnsynced: one or more informers have not synced, so their resources are
// missing from snapshots. It is a readiness condition (D9): the usual cause is a missing RBAC
// list or watch permission, which a restart can't fix, and the informers keep retrying.
const ConditionInformersUnsynced = "informers_unsynced"

// SyncReporter is implemented by cluster caches whose informers sync in the background.
type SyncReporter interface {
	// StartWithTimeout starts the informers, which run until ctx is done, and waits up to
	// timeout for them to sync. It returns the resources that haven't synced; those informers
	// keep retrying.
	StartWithTimeout(ctx context.Context, timeout time.Duration) []schema.GroupVersionResource
	// UnsyncedResources returns the resources whose informers haven't synced yet, sorted.
	UnsyncedResources() []schema.GroupVersionResource
}

// StartWithTimeout implements SyncReporter. Unlike Start it never blocks past timeout and never
// exits the process: an informer whose list is forbidden retries with backoff for as long as ctx
// lives, and becomes synced once the permission is granted (F-12).
func (dcc *DynamicClusterCache) StartWithTimeout(ctx context.Context, timeout time.Duration) []schema.GroupVersionResource {
	dcc.DynamicSharedInformerFactory.Start(ctx.Done())
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dcc.WaitForCacheSync(wctx.Done())
	return dcc.UnsyncedResources()
}

// UnsyncedResources implements SyncReporter.
func (dcc *DynamicClusterCache) UnsyncedResources() []schema.GroupVersionResource {
	var unsynced []schema.GroupVersionResource
	for _, gvr := range cacheResourceMap {
		if !dcc.ForResource(gvr).Informer().HasSynced() {
			unsynced = append(unsynced, gvr)
		}
	}
	slices.SortFunc(unsynced, func(a, b schema.GroupVersionResource) int {
		return strings.Compare(FormatResource(a), FormatResource(b))
	})
	return unsynced
}

// FormatResource returns resource or group/resource, as RBAC rules name it.
func FormatResource(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return gvr.Resource
	}
	return gvr.Group + "/" + gvr.Resource
}

// FormatResources joins FormatResource of each.
func FormatResources(gvrs []schema.GroupVersionResource) string {
	names := make([]string, 0, len(gvrs))
	for _, gvr := range gvrs {
		names = append(names, FormatResource(gvr))
	}
	return strings.Join(names, ", ")
}

// InformersUnsyncedMessage describes unsynced informers and their likely cause.
func InformersUnsyncedMessage(unsynced []schema.GroupVersionResource) string {
	return fmt.Sprintf("informers not synced for %s; the usual cause is a missing list or watch permission on these "+
		"resources in the agent's ClusterRole. They keep retrying, and snapshots leave these resources out until they sync", FormatResources(unsynced))
}

// SyncComponent returns the informers' health component: always live, and not ready
// (informers_unsynced, naming the resources) while any informer hasn't synced.
func SyncComponent(sr SyncReporter) health.Component {
	return health.ComponentFunc(func(context.Context, time.Time) health.Report {
		report := health.Report{Live: true}
		if unsynced := sr.UnsyncedResources(); len(unsynced) > 0 {
			report.Conditions = []condition.Condition{{
				Type:    ConditionInformersUnsynced,
				Reason:  "not_synced",
				Message: InformersUnsyncedMessage(unsynced),
			}}
			report.Status = map[string][]string{"unsynced": strings.Split(FormatResources(unsynced), ", ")}
		}
		return report
	})
}
