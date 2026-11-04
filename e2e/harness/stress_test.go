package harness

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// readyOnCreate makes the fake mark created pods Ready, standing in for the
// kubelet starting them.
func readyOnCreate(client *k8sfake.Clientset) {
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		p := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		return false, nil, nil // let the fake store the updated pod
	})
}

// recordConsume returns a consumeFunc that records each load request, failing
// the first failFirst calls (e.g. the HTTP server not listening yet).
func recordConsume(failFirst int) (consumeFunc, func() []string) {
	var mu sync.Mutex
	var got []string
	calls := 0
	f := func(_ context.Context, endpoint string, params url.Values) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls <= failFirst {
			return errors.New("connection refused")
		}
		got = append(got, endpoint+"?"+params.Encode())
		return nil
	}
	return f, func() []string { mu.Lock(); defer mu.Unlock(); return slices.Clone(got) }
}

func TestDeployStressCreatesPodAndStartsLoad(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, sent := recordConsume(0)

	if err := deployStress(context.Background(), client, consume, 5*time.Second, testInterval); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CoreV1().Namespaces().Get(context.Background(), StressNamespace, metav1.GetOptions{}); err != nil {
		t.Errorf("namespace %s not created: %v", StressNamespace, err)
	}
	p, err := client.CoreV1().Pods(StressNamespace).Get(context.Background(), "stress", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("stress pod not created: %v", err)
	}
	// The ported e2e.sh checks look for this name, namespace and label.
	if p.Labels["app"] != "stress" {
		t.Errorf("labels = %v, want app=stress", p.Labels)
	}
	c := p.Spec.Containers[0]
	if c.Image != stressImage || len(c.Ports) != 1 || c.Ports[0].ContainerPort != stressPort {
		t.Errorf("container = %s ports %v", c.Image, c.Ports)
	}
	// Limits must leave headroom above the load, or the pod is throttled or OOM-killed.
	if c.Resources.Limits.Cpu().MilliValue() <= StressCPUMillicores ||
		c.Resources.Limits.Memory().Value() <= int64(StressMemoryMegabytes)*1_000_000*11/10 {
		t.Errorf("limits %v leave no headroom above the load", c.Resources.Limits)
	}

	want := []string{
		"ConsumeCPU?durationSec=7200&millicores=100",
		"ConsumeMem?durationSec=7200&megabytes=128",
	}
	if got := sent(); !slices.Equal(got, want) {
		t.Fatalf("load requests = %v, want %v", got, want)
	}
}

// A pod from an earlier run is always replaced, even if it looks fine: its
// load would add to the new one (resource-consumer stacks requests).
func TestDeployStressReplacesPodFromEarlierRun(t *testing.T) {
	t.Parallel()
	old := stressPod()
	old.Status.Phase = corev1.PodRunning
	old.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	old.Annotations = map[string]string{"run": "earlier"}
	client := k8sfake.NewClientset(&corev1.Namespace{Name: StressNamespace}, old)
	readyOnCreate(client)
	consume, sent := recordConsume(0)

	if err := deployStress(context.Background(), client, consume, 5*time.Second, testInterval); err != nil {
		t.Fatal(err)
	}
	p, err := client.CoreV1().Pods(StressNamespace).Get(context.Background(), "stress", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if p.Annotations["run"] == "earlier" {
		t.Fatal("the earlier run's stress pod was kept")
	}
	if len(sent()) != 2 {
		t.Fatalf("load requests = %v, want one CPU and one memory request", sent())
	}
}

// The load requests are retried until resource-consumer's HTTP server answers.
func TestDeployStressRetriesLoadRequests(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, sent := recordConsume(3)

	if err := deployStress(context.Background(), client, consume, 5*time.Second, testInterval); err != nil {
		t.Fatal(err)
	}
	if got := sent(); len(got) != 2 {
		t.Fatalf("load requests = %v, want both sent after the retries", got)
	}
}

func TestDeployStressLoadFailureIsReported(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, _ := recordConsume(1 << 30) // never succeeds

	err := deployStress(context.Background(), client, consume, 100*time.Millisecond, testInterval)
	for _, want := range []string{"start stress load ConsumeCPU", "connection refused"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to contain %q", err, want)
		}
	}
}

func TestDeployStressReportsNotReady(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset() // pods stay without a Ready condition
	consume, sent := recordConsume(0)

	err := deployStress(context.Background(), client, consume, 100*time.Millisecond, testInterval)
	if err == nil || !strings.Contains(err.Error(), "stress pod") || !strings.Contains(err.Error(), "not Ready") {
		t.Fatalf("err = %v, want a not-Ready error for the stress pod", err)
	}
	if got := sent(); got != nil {
		t.Fatalf("load requests = %v, want none before the pod is Ready", got)
	}
}

func TestDeployStressCreateError(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	client.PrependReactor("create", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})
	consume, _ := recordConsume(0)

	err := deployStress(context.Background(), client, consume, time.Second, testInterval)
	if err == nil || !strings.Contains(err.Error(), "create namespace stress: forbidden") {
		t.Fatalf("err = %v", err)
	}
}
