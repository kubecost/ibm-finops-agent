package cluster

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
