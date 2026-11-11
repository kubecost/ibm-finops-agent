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
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
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

// steadyUsage reports the stress pod already at its steady load.
func steadyUsage(context.Context) (uint64, uint64, error) {
	return 100 * 1_000_000, 136 * 1024 * 1024, nil
}

// rampingUsage returns a usageFunc that replays readings in order and then
// repeats the last one, counting the calls.
func rampingUsage(readings ...usageReading) (usageFunc, func() int) {
	var mu sync.Mutex
	calls := 0
	f := func(context.Context) (uint64, uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		r := readings[min(calls, len(readings)-1)]
		calls++
		return r.cpu, r.mem, r.err
	}
	return f, func() int { mu.Lock(); defer mu.Unlock(); return calls }
}

type usageReading struct {
	cpu, mem uint64
	err      error
}

// The load ramps up after it is started; DeployStress returns only once the
// kubelet reports both CPU and memory at their steady values.
func TestDeployStressWaitsForSteadyLoad(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, _ := recordConsume(0)
	usage, calls := rampingUsage(
		usageReading{err: errors.New("stress pod not in the kubelet stats yet")},
		usageReading{cpu: 35 * 1_000_000, mem: 136 * 1024 * 1024}, // the ramp seen in CI
		usageReading{cpu: 99 * 1_000_000, mem: 60 * 1024 * 1024},  // memory still filling
		usageReading{cpu: 99 * 1_000_000, mem: 136 * 1024 * 1024},
	)

	if err := deployStress(context.Background(), client, consume, usage, 5*time.Second, testInterval); err != nil {
		t.Fatal(err)
	}
	if got := calls(); got != 4 {
		t.Fatalf("usage read %d times, want 4 (returned before the load was steady?)", got)
	}
}

func TestDeployStressReportsLoadThatNeverSettles(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, _ := recordConsume(0)
	usage, _ := rampingUsage(usageReading{cpu: 35 * 1_000_000, mem: 136 * 1024 * 1024})

	err := deployStress(context.Background(), client, consume, usage, 100*time.Millisecond, testInterval)
	for _, want := range []string{"did not reach 50m cpu and 100 MiB memory", "last reading: cpu 35m, memory 136 MiB"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to contain %q", err, want)
		}
	}
}

func TestDeployStressReportsUsageReadError(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, _ := recordConsume(0)
	usage, _ := rampingUsage(usageReading{err: errors.New("kubelet stats for node n1: forbidden")})

	err := deployStress(context.Background(), client, consume, usage, 100*time.Millisecond, testInterval)
	if err == nil || !strings.Contains(err.Error(), "last reading: kubelet stats for node n1: forbidden") {
		t.Fatalf("err = %v, want the usage read error", err)
	}
}

func TestStressUsageFindsStressPod(t *testing.T) {
	t.Parallel()
	cpu, mem := uint64(99_000_000), uint64(142_000_000)
	other := uint64(1)
	summary := stats.Summary{Pods: []stats.PodStats{
		{PodRef: stats.PodReference{Namespace: "kube-system", Name: "stress"},
			CPU: &stats.CPUStats{UsageNanoCores: &other}, Memory: &stats.MemoryStats{WorkingSetBytes: &other}},
		{PodRef: stats.PodReference{Namespace: StressNamespace, Name: "stress"},
			CPU: &stats.CPUStats{UsageNanoCores: &cpu}, Memory: &stats.MemoryStats{WorkingSetBytes: &mem}},
	}}
	gotCPU, gotMem, err := stressUsage(summary)
	if err != nil || gotCPU != cpu || gotMem != mem {
		t.Fatalf("stressUsage = %d, %d, %v; want %d, %d", gotCPU, gotMem, err, cpu, mem)
	}

	// Before the kubelet has a reading, the pod is listed without CPU stats.
	summary.Pods[1].CPU = nil
	if _, _, err := stressUsage(summary); err == nil || !strings.Contains(err.Error(), "no cpu or memory usage") {
		t.Errorf("missing cpu: err = %v", err)
	}
	if _, _, err := stressUsage(stats.Summary{}); err == nil || !strings.Contains(err.Error(), "not in the kubelet stats") {
		t.Errorf("missing pod: err = %v", err)
	}
}

func TestDeployStressCreatesPodAndStartsLoad(t *testing.T) {
	t.Parallel()
	client := k8sfake.NewClientset()
	readyOnCreate(client)
	consume, sent := recordConsume(0)

	if err := deployStress(context.Background(), client, consume, steadyUsage, 5*time.Second, testInterval); err != nil {
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

	if err := deployStress(context.Background(), client, consume, steadyUsage, 5*time.Second, testInterval); err != nil {
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

	if err := deployStress(context.Background(), client, consume, steadyUsage, 5*time.Second, testInterval); err != nil {
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

	err := deployStress(context.Background(), client, consume, steadyUsage, 100*time.Millisecond, testInterval)
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

	err := deployStress(context.Background(), client, consume, steadyUsage, 100*time.Millisecond, testInterval)
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

	err := deployStress(context.Background(), client, consume, steadyUsage, time.Second, testInterval)
	if err == nil || !strings.Contains(err.Error(), "create namespace stress: forbidden") {
		t.Fatalf("err = %v", err)
	}
}
