package harness

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// pollInterval is how often waits re-check the cluster.
	pollInterval = 2 * time.Second

	// StressNamespace and StressSelector identify the stress pod. The sample
	// checks look for this namespace and pod in the agent's output.
	StressNamespace = "stress"
	StressSelector  = "app=stress"
	stressPodName   = "stress"
	// stressImage is Kubernetes' own e2e load generator. It idles until asked
	// over HTTP (port 8080) to consume a set amount of CPU or memory, which
	// gives the value checks a known workload. It replaces jfusterm/stress,
	// whose Docker v1 manifest recent kind nodes can't pull.
	stressImage = "registry.k8s.io/e2e-test-images/resource-consumer:1.15.0"
	stressPort  = 8080

	// StressCPUMillicores and StressMemoryMegabytes are the load DeployStress
	// starts on the stress pod. Checks compare the agent's data against them.
	StressCPUMillicores   = 100
	StressMemoryMegabytes = 128
	// stressLoadDuration outlasts the 45m suite timeout, so the load never
	// stops mid-run. The pod is recreated each run, so it doesn't linger.
	stressLoadDuration = 2 * time.Hour
)

// NewClient returns a Kubernetes client for the cluster in kubeconfig.
func NewClient(kubeconfig string) (kubernetes.Interface, error) {
	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %s: %w", kubeconfig, err)
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return client, nil
}

// WaitForPodsReady waits until at least one pod in namespace matches selector
// and every matching pod is Ready. Pods being deleted are ignored, so pods
// from an old ReplicaSet don't count.
//
// On timeout the error describes each pod (phase and container reasons such as
// CrashLoopBackOff or ImagePullBackOff), so the failure says why.
func WaitForPodsReady(ctx context.Context, client kubernetes.Interface, namespace, selector string, timeout time.Duration) error {
	return waitForPodsReady(ctx, client, namespace, selector, timeout, pollInterval)
}

func waitForPodsReady(ctx context.Context, client kubernetes.Interface, namespace, selector string, timeout, interval time.Duration) error {
	var last string // description of what was seen on the last poll
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			// A list that failed because the wait ran out of time says nothing
			// about the pods; keep the last state seen so the error explains why.
			if ctx.Err() != nil && last != "" {
				return false, nil
			}
			// API hiccups are retried until the timeout.
			last = "list pods: " + err.Error()
			return false, nil
		}
		ready, desc := podsReady(pods.Items)
		last = desc
		return ready, nil
	})
	if err != nil {
		return fmt.Errorf("pods %q in namespace %s not Ready after %s: %s: %w", selector, namespace, timeout, last, err)
	}
	return nil
}

// podsReady reports whether there is at least one live pod and all live pods
// are Ready, plus a description of their state for error messages.
//
// Terminating pods are ignored, and so are finished ones (Failed or Succeeded,
// e.g. evicted): they stay around until pod GC but will never be Ready.
// Finished pods are still described, so an eviction shows up in the error.
func podsReady(pods []corev1.Pod) (bool, string) {
	var descs, finished []string
	live, ready := 0, 0
	for i := range pods {
		p := &pods[i]
		switch {
		case p.DeletionTimestamp != nil:
			continue
		case isFinished(p):
			finished = append(finished, describePod(p))
			continue
		}
		live++
		if isPodReady(p) {
			ready++
		}
		descs = append(descs, describePod(p))
	}
	slices.Sort(descs)
	slices.Sort(finished)
	if live == 0 {
		if len(finished) > 0 {
			return false, "no running pods; finished: " + strings.Join(finished, "; ")
		}
		return false, "no matching pods"
	}
	return ready == live, strings.Join(descs, "; ")
}

// isFinished reports whether p has stopped for good (Failed or Succeeded).
func isFinished(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded
}

func isPodReady(p *corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// describePod summarises a pod, e.g.
// "agent-123: Running, not Ready, container agent: CrashLoopBackOff (back-off 10s ...), restarts=3".
func describePod(p *corev1.Pod) string {
	parts := []string{fmt.Sprintf("%s: %s", p.Name, p.Status.Phase)}
	// Pod-level reason, e.g. "Evicted (The node was low on resource: ephemeral-storage.)".
	if p.Status.Reason != "" {
		r := p.Status.Reason
		if p.Status.Message != "" {
			r += " (" + p.Status.Message + ")"
		}
		parts = append(parts, r)
	}
	if isPodReady(p) {
		parts = append(parts, "Ready")
	} else {
		parts = append(parts, "not Ready")
	}
	statuses := append(slices.Clone(p.Status.InitContainerStatuses), p.Status.ContainerStatuses...)
	for _, cs := range statuses {
		var reason, msg string
		switch {
		case cs.State.Waiting != nil:
			reason, msg = cs.State.Waiting.Reason, cs.State.Waiting.Message
		case cs.State.Terminated != nil:
			reason, msg = cs.State.Terminated.Reason, cs.State.Terminated.Message
		}
		if reason == "" && cs.RestartCount == 0 {
			continue
		}
		s := "container " + cs.Name + ":"
		if reason != "" {
			s += " " + reason
		}
		if msg != "" {
			s += " (" + msg + ")"
		}
		if cs.RestartCount > 0 {
			s += fmt.Sprintf(", restarts=%d", cs.RestartCount)
		}
		parts = append(parts, s)
	}
	// Unschedulable pods have no container statuses; the condition says why.
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse && c.Message != "" {
			parts = append(parts, "unschedulable: "+c.Message)
		}
	}
	return strings.Join(parts, ", ")
}

// DeployStress recreates the stress namespace and pod (as e2e.sh did), waits
// for the pod to be Ready, and starts its load: StressCPUMillicores of CPU and
// StressMemoryMegabytes of memory. Run it before the agent is installed, so
// every sample the agent takes sees the same, known load.
//
// The pod is recreated on every run rather than reused. resource-consumer adds
// each request to the running load, so re-sending the load to a kept pod would
// stack it, and doubled memory gets the pod OOM-killed (which drops all load).
func DeployStress(ctx context.Context, client kubernetes.Interface, timeout time.Duration) error {
	return deployStress(ctx, client, proxyConsume(client), timeout, pollInterval)
}

// consumeFunc sends one load request (e.g. "ConsumeCPU") to the stress pod.
type consumeFunc func(ctx context.Context, endpoint string, params url.Values) error

// proxyConsume sends load requests through the API server's pod proxy, so no
// port-forward is needed.
func proxyConsume(client kubernetes.Interface) consumeFunc {
	return func(ctx context.Context, endpoint string, params url.Values) error {
		req := client.CoreV1().RESTClient().Post().
			Namespace(StressNamespace).
			Resource("pods").
			Name(fmt.Sprintf("%s:%d", stressPodName, stressPort)).
			SubResource("proxy").
			Suffix(endpoint)
		for k, vs := range params {
			for _, v := range vs {
				req = req.Param(k, v)
			}
		}
		return req.Do(ctx).Error()
	}
}

func deployStress(ctx context.Context, client kubernetes.Interface, consume consumeFunc, timeout, interval time.Duration) error {
	ns := &corev1.Namespace{Name: StressNamespace}
	if _, err := client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", StressNamespace, err)
	}
	if err := recreateStressPod(ctx, client, timeout, interval); err != nil {
		return err
	}
	if err := waitForPodsReady(ctx, client, StressNamespace, StressSelector, timeout, interval); err != nil {
		return fmt.Errorf("stress pod: %w", err)
	}
	return startStressLoad(ctx, consume, timeout, interval)
}

// recreateStressPod deletes any stress pod left by an earlier run and creates
// a fresh one, so the pod never carries load from before this run.
func recreateStressPod(ctx context.Context, client kubernetes.Interface, timeout, interval time.Duration) error {
	pods := client.CoreV1().Pods(StressNamespace)
	switch err := pods.Delete(ctx, stressPodName, metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); {
	case err == nil:
		_, _ = fmt.Fprintln(LogWriter, "replacing stress pod from an earlier run")
	case apierrors.IsNotFound(err):
	default:
		return fmt.Errorf("delete old stress pod: %w", err)
	}

	// Create fails with AlreadyExists until the old pod is fully gone.
	want := stressPod()
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := pods.Create(ctx, want, metav1.CreateOptions{})
		switch {
		case err == nil:
			return true, nil
		case apierrors.IsAlreadyExists(err):
			return false, nil
		default:
			return false, err
		}
	})
	if err != nil {
		return fmt.Errorf("create stress pod: %w", err)
	}
	return nil
}

// startStressLoad asks the stress pod for its fixed CPU and memory load. Each
// request is sent once it succeeds; failures (e.g. the HTTP server not
// listening yet) are retried until timeout.
func startStressLoad(ctx context.Context, consume consumeFunc, timeout, interval time.Duration) error {
	dur := strconv.Itoa(int(stressLoadDuration.Seconds()))
	loads := []struct {
		endpoint string
		params   url.Values
	}{
		{"ConsumeCPU", url.Values{"millicores": {strconv.Itoa(StressCPUMillicores)}, "durationSec": {dur}}},
		{"ConsumeMem", url.Values{"megabytes": {strconv.Itoa(StressMemoryMegabytes)}, "durationSec": {dur}}},
	}
	for _, l := range loads {
		var last error
		err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
			last = consume(ctx, l.endpoint, l.params)
			return last == nil, nil
		})
		if err != nil {
			return fmt.Errorf("start stress load %s: %w", l.endpoint, errors.Join(err, last))
		}
	}
	return nil
}

// stressPod is the pod e2e.sh created (name and namespace "stress", label
// app=stress), now running resource-consumer. The limits leave headroom above
// the load: 128 MB of memory shows up as about 136 MiB working set.
func stressPod() *corev1.Pod {
	return &corev1.Pod{
		Name:      stressPodName,
		Namespace: StressNamespace,
		Labels:    map[string]string{"app": "stress"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "stress",
				Image: stressImage,
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: stressPort}},
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			}},
		},
	}
}

// isTimeout reports whether err came from a wait that ran out of time
// (as opposed to failing for another reason).
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || wait.Interrupted(err)
}
