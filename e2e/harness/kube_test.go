package harness

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
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

// Short so tests run fast; waits that should succeed finish on the first or
// second poll, and waits that should time out use a short timeout.
const testInterval = 10 * time.Millisecond

func pod(name string, labels map[string]string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		Name: name, Namespace: "ns", Labels: labels,
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}},
		},
	}
}

var agentLabels = map[string]string{"app.kubernetes.io/name": "finops-agent"}

const agentSelector = "app.kubernetes.io/name=finops-agent"

func TestWaitForPodsReady(t *testing.T) {
	t.Parallel()
	crashing := pod("agent-b", agentLabels, false)
	crashing.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "agent",
		RestartCount: 3,
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
			Reason: "CrashLoopBackOff", Message: "back-off 40s restarting failed container",
		}},
	}}
	pulling := pod("agent-c", agentLabels, false)
	pulling.Status.Phase = corev1.PodPending
	pulling.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "agent",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
	}}
	unschedulable := pod("agent-d", agentLabels, false)
	unschedulable.Status.Phase = corev1.PodPending
	unschedulable.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Message: "0/1 nodes are available: 1 Insufficient cpu.",
	}}
	evicted := pod("agent-evicted", agentLabels, false)
	evicted.Status.Phase = corev1.PodFailed
	evicted.Status.Reason = "Evicted"
	evicted.Status.Message = "The node was low on resource: ephemeral-storage."
	completed := pod("agent-done", agentLabels, false)
	completed.Status.Phase = corev1.PodSucceeded
	terminatingOld := pod("agent-old", agentLabels, true)
	terminatingOld.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	terminatingOld.Finalizers = []string{"test"} // the fake requires one for a deletion timestamp

	tests := []struct {
		name    string
		pods    []runtime.Object
		wantErr []string // substrings; nil means Ready
	}{
		{name: "all ready", pods: []runtime.Object{pod("agent-a", agentLabels, true)}},
		{name: "other labels are ignored", pods: []runtime.Object{pod("agent-a", agentLabels, true), pod("other", map[string]string{"app": "x"}, false)}},
		{name: "no matching pods", pods: []runtime.Object{pod("other", map[string]string{"app": "x"}, true)}, wantErr: []string{"no matching pods"}},
		{name: "one of two not ready", pods: []runtime.Object{pod("agent-a", agentLabels, true), pod("agent-b", agentLabels, false)}, wantErr: []string{"agent-b: Running, not Ready"}},
		{name: "crash loop reason", pods: []runtime.Object{crashing}, wantErr: []string{"CrashLoopBackOff", "back-off 40s", "restarts=3"}},
		{name: "image pull reason", pods: []runtime.Object{pulling}, wantErr: []string{"agent-c: Pending", "ImagePullBackOff"}},
		{name: "unschedulable reason", pods: []runtime.Object{unschedulable}, wantErr: []string{"unschedulable: 0/1 nodes are available"}},
		// After a rollout restart the old pod can still be Ready while it shuts down.
		{name: "terminating ready pod does not count", pods: []runtime.Object{terminatingOld}, wantErr: []string{"no matching pods"}},
		{name: "terminating pod ignored next to a ready one", pods: []runtime.Object{terminatingOld, pod("agent-new", agentLabels, true)}},
		// Evicted and completed pods stay around until pod GC but will never be Ready.
		{name: "evicted pod ignored next to a ready one", pods: []runtime.Object{evicted, pod("agent-new", agentLabels, true)}},
		{name: "completed pod ignored next to a ready one", pods: []runtime.Object{completed, pod("agent-new", agentLabels, true)}},
		{name: "only an evicted pod reports the eviction", pods: []runtime.Object{evicted}, wantErr: []string{"no running pods", "agent-evicted: Failed", "Evicted", "low on resource"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := k8sfake.NewClientset(tt.pods...)
			err := waitForPodsReady(context.Background(), client, "ns", agentSelector, 100*time.Millisecond, testInterval)
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected a timeout error")
			}
			if !isTimeout(err) {
				t.Errorf("error should be a timeout: %v", err)
			}
			for _, want := range append(tt.wantErr, `pods "`+agentSelector+`" in namespace ns not Ready`) {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

// The wait keeps polling until the pod becomes Ready, rather than checking once.
func TestWaitForPodsReadyAfterUpdate(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset(pod("agent-a", agentLabels, false))

	go func() {
		time.Sleep(50 * time.Millisecond)
		p := pod("agent-a", agentLabels, true)
		_, _ = client.CoreV1().Pods("ns").UpdateStatus(context.Background(), p, metav1.UpdateOptions{})
	}()

	if err := waitForPodsReady(context.Background(), client, "ns", agentSelector, 5*time.Second, testInterval); err != nil {
		t.Fatal(err)
	}
}

// API errors are retried until the timeout, and the last one is reported.
func TestWaitForPodsReadyRetriesListErrors(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset(pod("agent-a", agentLabels, true))
	calls := 0
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls <= 2 {
			return true, nil, errors.New("connection refused")
		}
		return false, nil, nil // fall through to the real fake
	})

	if err := waitForPodsReady(context.Background(), client, "ns", agentSelector, 5*time.Second, testInterval); err != nil {
		t.Fatalf("should succeed after transient errors: %v", err)
	}

	broken := k8sfake.NewClientset()
	broken.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})
	err := waitForPodsReady(context.Background(), broken, "ns", agentSelector, 50*time.Millisecond, testInterval)
	if err == nil || !strings.Contains(err.Error(), "list pods: connection refused") {
		t.Fatalf("err = %v, want the last list error", err)
	}
}

// When the deadline hits during a list call, the error still reports the last
// pod state seen, not the list call's "context deadline exceeded".
func TestWaitForPodsReadyKeepsPodStateAtDeadline(t *testing.T) {
	t.Parallel()
	pulling := pod("stress", agentLabels, false)
	pulling.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  "stress",
		State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}},
	}}
	client := k8sfake.NewClientset(pulling)
	calls := 0
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		calls++
		if calls == 1 {
			return false, nil, nil // first poll sees the real pod
		}
		// later polls fail the way client-go does once the deadline is near
		time.Sleep(150 * time.Millisecond)
		return true, nil, context.DeadlineExceeded
	})

	err := waitForPodsReady(context.Background(), client, "ns", agentSelector, 100*time.Millisecond, testInterval)
	if err == nil || !strings.Contains(err.Error(), "ImagePullBackOff") {
		t.Fatalf("err = %v, want the last pod state (ImagePullBackOff)", err)
	}
}

func TestWaitForPodsGone(t *testing.T) {
	t.Parallel()

	t.Run("no pods", func(t *testing.T) {
		t.Parallel()
		client := k8sfake.NewClientset(pod("other", map[string]string{"app": "x"}, true))
		if err := waitForPodsGone(context.Background(), client, "ns", agentSelector, time.Second, testInterval); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("waits until the pod is deleted", func(t *testing.T) {
		t.Parallel()
		client := k8sfake.NewClientset(pod("agent-a", agentLabels, true))
		go func() {
			time.Sleep(50 * time.Millisecond)
			_ = client.CoreV1().Pods("ns").Delete(context.Background(), "agent-a", metav1.DeleteOptions{})
		}()
		if err := waitForPodsGone(context.Background(), client, "ns", agentSelector, 5*time.Second, testInterval); err != nil {
			t.Fatal(err)
		}
	})

	// A terminating pod can still write, so it counts until it is gone.
	t.Run("terminating pod still counts", func(t *testing.T) {
		t.Parallel()
		p := pod("agent-a", agentLabels, true)
		p.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		p.Finalizers = []string{"test"}
		client := k8sfake.NewClientset(p)
		err := waitForPodsGone(context.Background(), client, "ns", agentSelector, 100*time.Millisecond, testInterval)
		if err == nil || !strings.Contains(err.Error(), "still present") || !strings.Contains(err.Error(), "agent-a") {
			t.Fatalf("err = %v, want the terminating pod reported", err)
		}
	})
}

// A cancelled context (suite timeout, Ctrl-C) stops the wait immediately.
func TestWaitForPodsReadyStopsOnCancel(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset(pod("agent-a", agentLabels, false))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := waitForPodsReady(ctx, client, "ns", agentSelector, time.Hour, testInterval)
	if err == nil {
		t.Fatal("expected an error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("wait did not stop on a cancelled context")
	}
}

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

func TestNewClient(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("expected an error for a missing kubeconfig")
	}

	kc := filepath.Join(t.TempDir(), "kubeconfig")
	const cfg = `apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: "https://127.0.0.1:6443"}}]
users: [{name: u, user: {token: t}}]
contexts: [{name: x, context: {cluster: c, user: u}}]
current-context: x
`
	if err := os.WriteFile(kc, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(kc); err != nil {
		t.Fatalf("valid kubeconfig: %v", err)
	}
}
