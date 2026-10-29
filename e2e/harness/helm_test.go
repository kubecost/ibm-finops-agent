package harness

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const (
	testNS      = "ibm-finops-agent"
	testRelease = "unified-agent"
	notFound    = `echo "Error: release: not found" >&2; exit 1`
)

func testInstallConfig() Config {
	return Config{
		Image:        "localhost/e2e/ibm-finops-agent:e2e",
		ImageRef:     ImageRef{Registry: "localhost", Repository: "e2e/ibm-finops-agent", Tag: "e2e"},
		ChartVersion: "1.0.32",
		Namespace:    testNS,
		ReleaseName:  testRelease,
	}
}

func agentDeploymentObj() *appsv1.Deployment {
	return &appsv1.Deployment{
		Name:      testRelease + "-finops-agent",
		Namespace: testNS,
		Labels:    map[string]string{"app.kubernetes.io/name": "finops-agent", "app.kubernetes.io/instance": testRelease},
	}
}

func readyPodIn(name string, labels map[string]string, ready bool) *corev1.Pod {
	p := pod(name, labels, ready)
	p.Namespace = testNS
	return p
}

var (
	localstackLabels = map[string]string{"app.kubernetes.io/name": "localstack"}
	metav1Now        = metav1.Now()
)

// newInstaller returns an Installer with fast waits, a fake client holding
// objs, and the fake CLIs from f. Not parallel (fakeTools sets PATH).
func newInstaller(t *testing.T, f fake, objs ...runtime.Object) (*Installer, func() []string) {
	t.Helper()
	calls := fakeTools(t, f)
	in := &Installer{
		Cluster:  &Cluster{Name: "e2e-fake", Kubeconfig: "/tmp/e2e-fake/kubeconfig", runtime: "podman"},
		Client:   k8sfake.NewClientset(objs...),
		Config:   testInstallConfig(),
		Timeout:  200 * time.Millisecond,
		interval: testInterval,
	}
	return in, calls
}

// assertKubeconfig checks every helm/kubectl call used the suite's kubeconfig.
func assertKubeconfig(t *testing.T, in *Installer, calls []string) {
	t.Helper()
	for _, c := range calls {
		if field(c, "kubeconfig") != in.Cluster.Kubeconfig {
			t.Errorf("call %q used KUBECONFIG=%q, want %q", c, field(c, "kubeconfig"), in.Cluster.Kubeconfig)
		}
	}
}

func TestAgentInstallArgs(t *testing.T) {
	t.Parallel()
	got := agentInstallArgs(testInstallConfig(), AgentInstall{
		ValuesFiles: []string{"testdata/fast.yaml", "testdata/limits.yaml"},
		Set:         map[string]string{"z.last": "1", "agent.cloudability.emissionInterval": "5s", "agent.kubecost.enabled": "false"},
	})
	want := []string{
		"upgrade", "--install", testRelease, "finops-agent",
		"--repo", AgentChartRepo,
		"--version", "1.0.32",
		"--namespace", testNS, "--create-namespace",
		// Base values first, then overlays in the given order.
		"--values", AgentValuesFile,
		"--values", "testdata/fast.yaml",
		"--values", "testdata/limits.yaml",
		// Image under test, split by ParseImage. Always strings, so an
		// all-digit tag isn't turned into a number.
		"--set-string", "image.registry=localhost",
		"--set-string", "image.repository=e2e/ibm-finops-agent",
		"--set-string", "image.tag=e2e",
		// Extra values sorted, so the command is the same every run. They use
		// --set, so a spec can pass booleans and numbers. (They can't override
		// image.*: helm applies --set-string after --set. See AgentInstall.Set.)
		"--set", "agent.cloudability.emissionInterval=5s",
		"--set", "agent.kubecost.enabled=false",
		"--set", "z.last=1",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("args =\n  %v\nwant\n  %v", got, want)
	}
}

// Tags that look like numbers (short SHAs, x.y versions) stay strings.
func TestAgentInstallArgsNumericTag(t *testing.T) {
	t.Parallel()
	for _, tag := range []string{"12345678", "1.10", "0123"} {
		cfg := testInstallConfig()
		cfg.ImageRef.Tag = tag
		args := agentInstallArgs(cfg, AgentInstall{})
		i := slices.Index(args, "image.tag="+tag)
		if i < 1 || args[i-1] != "--set-string" {
			t.Errorf("tag %q: want --set-string image.tag=%s in %v", tag, tag, args)
		}
	}
}

func TestInstallLocalstack(t *testing.T) {
	t.Run("installs and waits for Ready", func(t *testing.T) {
		in, calls := newInstaller(t, fake{}, readyPodIn("localstack-0", localstackLabels, true))
		if err := in.InstallLocalstack(context.Background()); err != nil {
			t.Fatal(err)
		}
		got := calls()
		want := "upgrade --install localstack localstack --repo " + LocalstackChartRepo +
			" --version " + LocalstackChartVersion +
			" --namespace " + testNS + " --create-namespace --values " + LocalstackValuesFile
		if len(got) != 1 || argsOf(got[0]) != want {
			t.Fatalf("calls = %v\nwant one helm call: %s", got, want)
		}
		assertKubeconfig(t, in, got)
	})

	t.Run("not Ready reports the pod state", func(t *testing.T) {
		in, _ := newInstaller(t, fake{}, readyPodIn("localstack-0", localstackLabels, false))
		err := in.InstallLocalstack(context.Background())
		if err == nil || !strings.Contains(err.Error(), "localstack:") || !strings.Contains(err.Error(), "localstack-0: Running, not Ready") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("helm failure", func(t *testing.T) {
		in, _ := newInstaller(t, fake{on: map[string]string{"helm upgrade": `echo "repo unreachable" >&2; exit 1`}})
		err := in.InstallLocalstack(context.Background())
		if err == nil || !strings.Contains(err.Error(), "install localstack") || !strings.Contains(err.Error(), "repo unreachable") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestEnsureBucket(t *testing.T) {
	const (
		head = "kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3api head-bucket --bucket " + Bucket
		mb   = "kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3 mb s3://" + Bucket
	)
	tests := []struct {
		name      string
		on        map[string]string
		wantErr   []string
		wantCalls []string
	}{
		{
			name:      "existing bucket is left alone",
			wantCalls: []string{head},
		},
		{
			name:      "missing bucket is created",
			on:        map[string]string{"kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3api": `exit 254`},
			wantCalls: []string{head, mb},
		},
		{
			name: "both failing reports both errors",
			on: map[string]string{
				"kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3api": `echo "could not connect" >&2; exit 255`,
				"kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3 mb": `echo "mb failed" >&2; exit 1`,
			},
			wantErr:   []string{"create bucket " + Bucket, "mb failed", "head-bucket", "could not connect"},
			wantCalls: []string{head, mb},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A terminating pod must not be picked for exec.
			old := readyPodIn("localstack-old", localstackLabels, true)
			old.DeletionTimestamp = &metav1Now
			old.Finalizers = []string{"test"}
			in, calls := newInstaller(t, fake{on: tt.on}, old, readyPodIn("localstack-0", localstackLabels, true))

			err := in.EnsureBucket(context.Background())
			if tt.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for _, want := range tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want it to contain %q", err, want)
				}
			}
			var got []string
			for _, c := range calls() {
				got = append(got, "kubectl "+argsOf(c))
			}
			if !slices.Equal(got, tt.wantCalls) {
				t.Fatalf("calls =\n  %v\nwant\n  %v", got, tt.wantCalls)
			}
		})
	}

	// A hung awslocal fails that step quickly with a clear error, instead of
	// using up the whole setup timeout.
	t.Run("hung exec times out and names the step", func(t *testing.T) {
		in, _ := newInstaller(t, fake{on: map[string]string{
			"kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3api": `exec sleep 30`,
		}}, readyPodIn("localstack-0", localstackLabels, true))
		in.execTimeout = 200 * time.Millisecond

		start := time.Now()
		err := in.EnsureBucket(context.Background())
		if time.Since(start) > 10*time.Second {
			t.Fatalf("EnsureBucket took %s; the hung exec wasn't stopped", time.Since(start))
		}
		for _, want := range []string{"head-bucket", "context deadline exceeded"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("err = %v, want it to contain %q", err, want)
			}
		}
	})

	t.Run("no Ready localstack pod", func(t *testing.T) {
		in, calls := newInstaller(t, fake{}, readyPodIn("localstack-0", localstackLabels, false))
		err := in.EnsureBucket(context.Background())
		if err == nil || !strings.Contains(err.Error(), "find localstack pod") {
			t.Fatalf("err = %v", err)
		}
		if got := calls(); got != nil {
			t.Fatalf("calls = %v, want none", got)
		}
	})
}

// TestInstallAgentCalls checks the exact helm/kubectl sequence of InstallAgent.
// The rules the rows enforce:
//   - A fresh install is not restarted (its pod already runs the new image).
//   - An existing release is restarted, because the "e2e" tag doesn't change
//     between builds and helm would otherwise leave the old pod running.
//   - Success always means `kubectl rollout status` passed.
func TestInstallAgentCalls(t *testing.T) {
	const (
		status   = "helm status " + testRelease
		upgrade  = "helm upgrade --install"
		restart  = "kubectl rollout restart"
		rollout  = "kubectl rollout status"
		deployID = "deployment/" + testRelease + "-finops-agent"
	)
	tests := []struct {
		name      string
		on        map[string]string
		objs      []runtime.Object
		wantErr   []string
		wantCalls []string
	}{
		{
			name:      "fresh install waits for rollout without restart",
			on:        map[string]string{"helm status": notFound},
			wantCalls: []string{status, upgrade, rollout},
		},
		{
			name:      "existing release is restarted so the new build runs",
			wantCalls: []string{status, upgrade, restart, rollout},
		},
		{
			name: "failed rollout reports why the pods aren't Ready",
			on:   map[string]string{"helm status": notFound, "kubectl rollout status": `echo "timed out waiting for the condition" >&2; exit 1`},
			objs: []runtime.Object{crashingAgentPod()},
			wantErr: []string{"agent rollout", "timed out waiting",
				"unified-agent-abc: Running, not Ready", "CrashLoopBackOff", "restarts=4"},
			wantCalls: []string{status, upgrade, rollout},
		},
		{
			name:      "helm status error other than not found stops before installing",
			on:        map[string]string{"helm status": `echo "Kubernetes cluster unreachable" >&2; exit 1`},
			wantErr:   []string{"check helm release " + testRelease, "cluster unreachable"},
			wantCalls: []string{status},
		},
		{
			name:      "helm upgrade failure is reported with helm's stderr",
			on:        map[string]string{"helm status": notFound, "helm upgrade": `echo "chart version 9.9.9 not found" >&2; exit 1`},
			wantErr:   []string{"install agent", "chart version 9.9.9 not found"},
			wantCalls: []string{status, upgrade},
		},
		{
			name:      "restart failure is reported",
			on:        map[string]string{"kubectl rollout restart": `exit 1`},
			wantErr:   []string{"restart agent"},
			wantCalls: []string{status, upgrade, restart},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := append([]runtime.Object{agentDeploymentObj()}, tt.objs...)
			in, calls := newInstaller(t, fake{on: tt.on}, objs...)

			err := in.InstallAgent(context.Background(), AgentInstall{})
			if tt.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantErr != nil && err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}

			got := calls()
			var gotCalls []string
			for _, c := range got {
				tool, _, _ := strings.Cut(c, " ")
				a := argsOf(c)
				switch {
				case strings.HasPrefix(a, "status "):
					gotCalls = append(gotCalls, "helm status "+strings.Fields(a)[1])
				case strings.HasPrefix(a, "upgrade --install"):
					gotCalls = append(gotCalls, upgrade)
				default:
					gotCalls = append(gotCalls, strings.Join(append([]string{tool}, strings.Fields(a)[:2]...), " "))
				}
				// restart and status act on the agent Deployment found by label, in the namespace.
				if strings.HasPrefix(a, "rollout ") && !strings.Contains(a, deployID+" --namespace "+testNS) {
					t.Errorf("rollout call %q should target %s in %s", a, deployID, testNS)
				}
				if strings.HasPrefix(a, "rollout status") && !strings.Contains(a, "--timeout 200ms") {
					t.Errorf("rollout status %q should use the installer timeout", a)
				}
			}
			if !slices.Equal(gotCalls, tt.wantCalls) {
				t.Fatalf("calls =\n  %v\nwant\n  %v", gotCalls, tt.wantCalls)
			}
			assertKubeconfig(t, in, got)
		})
	}
}

func crashingAgentPod() *corev1.Pod {
	p := readyPodIn("unified-agent-abc", map[string]string{"app.kubernetes.io/name": "finops-agent", "app.kubernetes.io/instance": testRelease}, false)
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "finops-agent",
		RestartCount: 4,
		State:        corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
	}}
	return p
}

// The Deployment is found by label, not by guessing the chart's name: the
// chart names it "<release>-finops-agent", but just "finops-agent" when the
// release is called finops-agent.
func TestInstallAgentFindsDeploymentByLabel(t *testing.T) {
	t.Run("custom name", func(t *testing.T) {
		d := agentDeploymentObj()
		d.Name = "finops-agent"
		in, calls := newInstaller(t, fake{}, d)
		if err := in.InstallAgent(context.Background(), AgentInstall{}); err != nil {
			t.Fatal(err)
		}
		if got := calls(); !strings.Contains(argsOf(got[len(got)-1]), "rollout status deployment/finops-agent ") {
			t.Fatalf("last call = %q", got[len(got)-1])
		}
	})

	t.Run("other release's deployment is ignored", func(t *testing.T) {
		other := agentDeploymentObj()
		other.Name = "someone-else-finops-agent"
		other.Labels = map[string]string{"app.kubernetes.io/name": "finops-agent", "app.kubernetes.io/instance": "someone-else"}
		in, _ := newInstaller(t, fake{}, other)
		err := in.InstallAgent(context.Background(), AgentInstall{})
		if err == nil || !strings.Contains(err.Error(), "found 0") {
			t.Fatalf("err = %v, want no deployment found for this release", err)
		}
	})
}
