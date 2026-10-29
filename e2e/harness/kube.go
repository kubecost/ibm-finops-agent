package harness

import (
	"context"
	"fmt"
	"slices"
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
func podsReady(pods []corev1.Pod) (bool, string) {
	var descs []string
	live, ready := 0, 0
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		live++
		if isPodReady(p) {
			ready++
		}
		descs = append(descs, describePod(p))
	}
	if live == 0 {
		return false, "no matching pods"
	}
	slices.Sort(descs)
	return ready == live, strings.Join(descs, "; ")
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

// DeployStress creates the stress namespace and pod (as e2e.sh did) and waits
// for the pod to be Ready. Existing ones are reused when current, so this is
// safe on a kept cluster. The stress pod gives data validation a consistent workload.
func DeployStress(ctx context.Context, client kubernetes.Interface, timeout time.Duration) error {
	return deployStress(ctx, client, timeout, pollInterval)
}

func deployStress(ctx context.Context, client kubernetes.Interface, timeout, interval time.Duration) error {
	ns := &corev1.Namespace{Name: StressNamespace}
	if _, err := client.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create namespace %s: %w", StressNamespace, err)
	}
	if err := ensureStressPod(ctx, client, timeout, interval); err != nil {
		return err
	}
	if err := waitForPodsReady(ctx, client, StressNamespace, StressSelector, timeout, interval); err != nil {
		return fmt.Errorf("stress pod: %w", err)
	}
	return nil
}

// ensureStressPod creates the stress pod. An existing pod is kept if it runs
// stressImage and replaced otherwise: a pod's image can't be changed in place,
// and a kept cluster may still have a pod from an older harness version.
func ensureStressPod(ctx context.Context, client kubernetes.Interface, timeout, interval time.Duration) error {
	pods := client.CoreV1().Pods(StressNamespace)
	want := stressPod()

	_, err := pods.Create(ctx, want, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create stress pod: %w", err)
	}

	existing, err := pods.Get(ctx, stressPodName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get existing stress pod: %w", err)
	}
	if len(existing.Spec.Containers) == 1 && existing.Spec.Containers[0].Image == stressImage {
		return nil
	}

	_, _ = fmt.Fprintf(LogWriter, "replacing stress pod: it runs %q, want %q\n", existing.Spec.Containers[0].Image, stressImage)
	if err := pods.Delete(ctx, stressPodName, metav1.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete old stress pod: %w", err)
	}
	// Create fails with AlreadyExists until the old pod is fully gone.
	err = wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
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
		return fmt.Errorf("recreate stress pod: %w", err)
	}
	return nil
}

// stressPod is the pod e2e.sh created (name and namespace "stress", label
// app=stress), now running resource-consumer. It idles until a check asks it to
// consume CPU or memory; the limits leave room above what those checks request.
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
