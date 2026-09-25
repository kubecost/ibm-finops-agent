package cluster

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// F-25 / I7: the short-lived-pod buffer is capped; overflow drops the oldest and counts it.
func TestShortLivedPodBufferCap(t *testing.T) {
	dcc := &DynamicClusterCache{slpCap: 3}
	for i := range 5 {
		dcc.addShortLivedPod(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprintf("pod-%d", i)}})
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
	dcc.addShortLivedPod(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-5"}})
	if got := len(dcc.GetAllShortLivedPods()); got != 1 {
		t.Errorf("got %d pods after drain; want 1", got)
	}
	if got := dcc.ShortLivedPodsDropped(); got != 2 {
		t.Errorf("ShortLivedPodsDropped() = %d after drain; want 2", got)
	}
}
