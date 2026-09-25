package emitter

import (
	"k8s.io/apimachinery/pkg/types"
)

// SnapshotComponent names one independently collected part of a ClusterSnapshot.
type SnapshotComponent string

const (
	ComponentClusterInfo SnapshotComponent = "cluster_info"
	ComponentKubernetes  SnapshotComponent = "kubernetes"
	ComponentNodeStats   SnapshotComponent = "node_stats"
	ComponentMetrics     SnapshotComponent = "metrics"
)

// AllComponents lists every snapshot component, in the order they are reported.
var AllComponents = []SnapshotComponent{ComponentClusterInfo, ComponentKubernetes, ComponentNodeStats, ComponentMetrics}

// ComponentRequirer is implemented by an emitter that needs only some snapshot components. The
// exporter calls its Init and Emit only when those components succeeded; a failure in any other
// component doesn't affect it. An emitter that doesn't implement it requires every component.
type ComponentRequirer interface {
	RequiredComponents() []SnapshotComponent
}

// requiredComponents returns the components e needs.
func requiredComponents(e Emitter) []SnapshotComponent {
	if r, ok := e.(ComponentRequirer); ok {
		return r.RequiredComponents()
	}
	return AllComponents
}

// requires reports whether e needs component c.
func requires(e Emitter, c SnapshotComponent) bool {
	for _, rc := range requiredComponents(e) {
		if rc == c {
			return true
		}
	}
	return false
}

// MissingComponent returns the first of required that failed in this snapshot, and its error.
// A component counts as failed only when ComponentErrors records an error for it.
func (cs *ClusterSnapshot) MissingComponent(required []SnapshotComponent) (SnapshotComponent, error) {
	for _, c := range required {
		if err := cs.ComponentErrors[c]; err != nil {
			return c, err
		}
	}
	return "", nil
}

// ShortLivedPodWriter is implemented by an emitter that writes the snapshot's short-lived pods
// to durable storage (Cloudability). Every snapshot carries the short-lived pods still
// buffered in the cluster cache; the exporter removes them from the buffer only once a writer
// reports them written. If no emitter is a ShortLivedPodWriter, the exporter commits each used
// snapshot's pods straight away, as nothing will write them.
type ShortLivedPodWriter interface {
	// WrittenShortLivedPods returns the UIDs of short-lived pods written into finalised samples
	// since the previous call, and forgets them. It is called from the exporter loop while Emit
	// may be running, so it must be safe for concurrent use.
	WrittenShortLivedPods() []types.UID
}

// WindowCommitter is implemented by a SnapshotProvider that tracks, per metrics resolution,
// which windows have been delivered. The exporter calls CommitWindows once every emitter that
// requires metrics has received the snapshot successfully; only then does the provider stop
// re-querying the snapshot's closed windows.
type WindowCommitter interface {
	CommitWindows(*ClusterSnapshot)
}
