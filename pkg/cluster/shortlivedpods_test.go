package cluster

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// F-25 / I7: the short-lived-pod buffer is capped; overflow drops the oldest and counts it.
func TestShortLivedPodBufferCap(t *testing.T) {
	dcc := &DynamicClusterCache{slpCap: 3}
	for i := range 5 {
		dcc.addShortLivedPod(&corev1.Pod{Namespace: "ns", Name: fmt.Sprintf("pod-%d", i)})
	}

	var names []string
	for _, p := range dcc.GetAllShortLivedPods() {
		names = append(names, p.Name)
	}
	if got := fmt.Sprint(names); got != "[pod-2 pod-3 pod-4]" {
		t.Errorf("buffer holds %s; want the 3 newest pods", got)
	}
	if got := dcc.ShortLivedPodsDropped(); got != 2 {
		t.Errorf("ShortLivedPodsDropped() = %d; want 2", got)
	}

	// A drained buffer accepts pods again without dropping.
	dcc.addShortLivedPod(&corev1.Pod{Name: "pod-5"})
	if got := len(dcc.GetAllShortLivedPods()); got != 1 {
		t.Errorf("got %d pods after drain; want 1", got)
	}
	if got := dcc.ShortLivedPodsDropped(); got != 2 {
		t.Errorf("ShortLivedPodsDropped() = %d after drain; want 2", got)
	}
}

// F-47: a pod deleted during a watch gap arrives as a DeletedFinalStateUnknown tombstone. It is
// still a short-lived pod and must be buffered.
func TestShortLivedPodTombstoneCaptured(t *testing.T) {
	dcc := &DynamicClusterCache{slpCap: DefaultShortLivedPodBufferCap, slpDuration: time.Hour}
	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": "gone", "namespace": "default", "uid": "uid-gone"},
		"status":     map[string]any{"startTime": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)},
	}}

	dcc.captureShortLivedPodFunc()(cache.DeletedFinalStateUnknown{Key: "default/gone", Obj: pod})

	var names []string
	for _, p := range dcc.GetAllShortLivedPods() {
		names = append(names, p.Name)
	}
	if fmt.Sprint(names) != "[gone]" {
		t.Errorf("F-47: a pod deleted during a watch gap (tombstone) was not buffered: got %v, want [gone]", names)
	}
}

// F-07: peeking leaves pods buffered; committing removes exactly the given UIDs, including pods
// added after the peek being kept.
func TestShortLivedPodPeekCommit(t *testing.T) {
	dcc := &DynamicClusterCache{slpCap: DefaultShortLivedPodBufferCap}
	pod := func(name string) *corev1.Pod {
		return &corev1.Pod{Name: name, UID: types.UID("uid-" + name)}
	}
	dcc.addShortLivedPod(pod("a"))
	dcc.addShortLivedPod(pod("b"))

	peeked := dcc.PeekShortLivedPods()
	if len(peeked) != 2 || len(dcc.PeekShortLivedPods()) != 2 {
		t.Fatalf("peek returned %d pods and must not drain", len(peeked))
	}
	dcc.addShortLivedPod(pod("c")) // deleted after the snapshot

	if n := dcc.CommitShortLivedPods([]types.UID{"uid-a", "uid-b", "uid-unknown"}); n != 2 {
		t.Errorf("CommitShortLivedPods removed %d; want 2", n)
	}
	var names []string
	for _, p := range dcc.PeekShortLivedPods() {
		names = append(names, p.Name)
	}
	if fmt.Sprint(names) != "[c]" {
		t.Errorf("buffer after commit = %v; want [c]", names)
	}
}
