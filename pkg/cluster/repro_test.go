package cluster

// Reliability reproductions for the cluster-cache findings in docs/reliability/FINDINGS.md.

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func unstructuredPod(name string, containers any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": name, "namespace": "default"},
		"spec":       map[string]any{"containers": containers},
	}}
}

// F-40: one object that fails typed conversion empties its whole resource type.
func TestReproF40OneBadObjectEmptiesResourceType(t *testing.T) {
	before := ConversionFailures()["pods"]
	good := []any{map[string]any{"name": "c", "image": "busybox"}}
	pods := ConvertUnstructuredArrayToTypedArray[corev1.Pod]([]*unstructured.Unstructured{
		unstructuredPod("good-1", good),
		unstructuredPod("bad", "not-a-list"), // does not convert to []Container
		unstructuredPod("good-2", good),
	})

	var names []string
	for _, p := range pods {
		names = append(names, p.Name)
	}
	if len(pods) != 2 {
		t.Fatalf("F-40: one pod that fails conversion dropped the whole pod list: got %d pods %v, want the 2 convertible pods [good-1 good-2]", len(pods), names)
	}
	if got := ConversionFailures()["pods"] - before; got != 1 {
		t.Errorf("ConversionFailures()[pods] rose by %d; want 1", got)
	}
}
