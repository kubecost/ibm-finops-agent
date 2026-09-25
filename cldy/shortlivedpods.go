package cldy

import (
	"sync"

	"github.com/ibm/finops-agent/pkg/emitter"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// RequiredComponents implements emitter.ComponentRequirer. Cloudability samples are built from
// the Kubernetes objects and node stats only, so a metrics or cluster-info failure doesn't stop
// them.
func (ce *Emitter) RequiredComponents() []emitter.SnapshotComponent {
	return []emitter.SnapshotComponent{emitter.ComponentKubernetes, emitter.ComponentNodeStats}
}

// WrittenShortLivedPods implements emitter.ShortLivedPodWriter: the UIDs of short-lived pods
// written into samples queued for upload since the last call. The exporter keeps every other
// short-lived pod buffered, so pods seen on non-emitting ticks or by a failed Emit are written
// by a later sample.
func (ce *Emitter) WrittenShortLivedPods() []types.UID {
	return ce.writtenPods.take()
}

// writtenPods collects the UIDs of short-lived pods written into finalised samples. Emit adds to
// it on the exporter's call goroutine while the exporter loop takes from it.
type writtenPods struct {
	mu   sync.Mutex
	uids []types.UID
}

func (w *writtenPods) add(pods []*v1.Pod) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range pods {
		w.uids = append(w.uids, p.UID)
	}
}

func (w *writtenPods) take() []types.UID {
	w.mu.Lock()
	defer w.mu.Unlock()
	uids := w.uids
	w.uids = nil
	return uids
}
