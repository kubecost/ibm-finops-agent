package harness

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// The stress pod is a known workload for the agent to report on. Checks compare
// the agent's data against the load started here.

const (
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
