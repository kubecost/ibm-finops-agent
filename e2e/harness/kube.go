package harness

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// pollInterval is how often waits re-check the cluster.
	pollInterval = 2 * time.Second
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

// waitForPodsGone waits until no pod in namespace matches selector, including
// terminating ones: a pod still shutting down can still write data.
func waitForPodsGone(ctx context.Context, client kubernetes.Interface, namespace, selector string, timeout, interval time.Duration) error {
	var last []string
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
		if err != nil {
			return false, nil // retried until the timeout
		}
		last = last[:0]
		for i := range pods.Items {
			last = append(last, describePod(&pods.Items[i]))
		}
		return len(pods.Items) == 0, nil
	})
	if err != nil {
		return fmt.Errorf("pods %q in namespace %s still present after %s: %s: %w", selector, namespace, timeout, strings.Join(last, "; "), err)
	}
	return nil
}

// isTimeout reports whether err came from a wait that ran out of time
// (as opposed to failing for another reason).
func isTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || wait.Interrupted(err)
}
