package harness

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// AgentChartRepo and agentChart identify the finops-agent chart. Installs use
	// --repo, so they don't depend on `helm repo add` or local repo names.
	AgentChartRepo = "https://kubecost.github.io/finops-agent-chart"
	agentChart     = "finops-agent"

	// LocalstackChartRepo serves the localstack chart (fake S3 for the agent's
	// federated storage).
	LocalstackChartRepo = "https://helm.localstack.cloud"
	// LocalstackChartVersion pins the chart, so a new localstack chart release
	// can't break every run at once. The image is pinned separately (4.12) in
	// testdata/localstack-values.yaml.
	LocalstackChartVersion = "0.7.1"
	localstackChart        = "localstack"
	localstackRelease      = "localstack"
	LocalstackSelector     = "app.kubernetes.io/name=localstack"

	// Bucket is the S3 bucket testdata/values.yaml points the agent at.
	Bucket = "kubecost-store"

	// Values files, relative to e2e/ (go test's working directory).
	AgentValuesFile      = "testdata/values.yaml"
	LocalstackValuesFile = "testdata/localstack-values.yaml"

	defaultInstallTimeout = 5 * time.Minute
	// defaultExecTimeout bounds each awslocal call in EnsureBucket. They
	// normally take about a second.
	defaultExecTimeout = 30 * time.Second
)

// AgentSelector selects the agent pods of release.
func AgentSelector(release string) string {
	return "app.kubernetes.io/name=finops-agent,app.kubernetes.io/instance=" + release
}

// AgentInstall holds per-install agent settings, so specs can reinstall the
// agent with different values (e.g. a shorter emission interval).
type AgentInstall struct {
	// ValuesFiles are applied after AgentValuesFile, in order; later files win.
	ValuesFiles []string
	// Set holds extra --set values, e.g. "agent.cloudability.emissionInterval": "5s".
	// They go through --set, so specs can pass booleans and numbers.
	//
	// The image.registry, image.repository and image.tag keys can't be
	// overridden here: helm applies every --set before every --set-string,
	// whatever the order on the command line, so the image flags always win.
	// That's intended; the suite always tests the IMAGE it was given.
	Set map[string]string
}

// Installer installs the suite's Helm releases into cfg.Namespace on Cluster.
type Installer struct {
	Cluster *Cluster
	Client  kubernetes.Interface
	Config  Config

	// Timeout bounds each wait (localstack Ready, agent rollout). Zero means 5m.
	Timeout time.Duration

	interval    time.Duration // poll interval; tests shorten it
	execTimeout time.Duration // per kubectl exec; zero means defaultExecTimeout; tests shorten it
}

func (in *Installer) execTimeoutOrDefault() time.Duration {
	if in.execTimeout > 0 {
		return in.execTimeout
	}
	return defaultExecTimeout
}

func (in *Installer) timeout() time.Duration {
	if in.Timeout > 0 {
		return in.Timeout
	}
	return defaultInstallTimeout
}

func (in *Installer) pollEvery() time.Duration {
	if in.interval > 0 {
		return in.interval
	}
	return pollInterval
}

// run runs a helm or kubectl command against the suite's cluster.
func (in *Installer) run(ctx context.Context, name string, args ...string) (string, error) {
	return run(ctx, in.Cluster.Env(), name, args...)
}

// InstallLocalstack installs or upgrades localstack and waits for it to be Ready.
func (in *Installer) InstallLocalstack(ctx context.Context) error {
	if _, err := in.run(ctx, "helm", "upgrade", "--install", localstackRelease, localstackChart,
		"--repo", LocalstackChartRepo,
		"--version", LocalstackChartVersion,
		"--namespace", in.Config.Namespace, "--create-namespace",
		"--values", LocalstackValuesFile); err != nil {
		return fmt.Errorf("install localstack: %w", err)
	}
	if err := waitForPodsReady(ctx, in.Client, in.Config.Namespace, LocalstackSelector, in.timeout(), in.pollEvery()); err != nil {
		return fmt.Errorf("localstack: %w", err)
	}
	return nil
}

// RemoveAgent uninstalls the agent release if it exists (a kept cluster) and
// waits until its pods are gone. Call it before EnsureBucket: the old agent
// keeps writing to the bucket until it stops, so emptying the bucket while it
// runs would let its data back in.
func (in *Installer) RemoveAgent(ctx context.Context) error {
	release, ns := in.Config.ReleaseName, in.Config.Namespace
	existed, err := in.releaseExists(ctx, release)
	if err != nil {
		return err
	}
	if !existed {
		return nil
	}
	_, _ = fmt.Fprintf(LogWriter, "removing agent release %s from an earlier run\n", release)
	if _, err := in.run(ctx, "helm", "uninstall", release, "--namespace", ns); err != nil {
		return fmt.Errorf("uninstall agent: %w", err)
	}
	if err := waitForPodsGone(ctx, in.Client, ns, AgentSelector(release), in.timeout(), in.pollEvery()); err != nil {
		return fmt.Errorf("old agent: %w", err)
	}
	return nil
}

// EnsureBucket leaves the agent's S3 bucket in localstack existing and empty.
// On a kept cluster the bucket survives from the previous run, so it is
// emptied: otherwise old federated windows, heartbeats and uploads would mix
// with this run's (the stress pod keeps its name across runs, so its usage
// would be combined in the longer windows).
//
// No agent may be running when this is called (see RemoveAgent), or it will
// write new objects right after the bucket is emptied.
func (in *Installer) EnsureBucket(ctx context.Context) error {
	pod, err := in.readyPod(ctx, LocalstackSelector)
	if err != nil {
		return fmt.Errorf("find localstack pod: %w", err)
	}
	// Each exec gets its own timeout, so a hung awslocal fails this step with
	// a clear error instead of using up the whole setup timeout.
	exec := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(ctx, in.execTimeoutOrDefault())
		defer cancel()
		return in.run(ctx, "kubectl", append([]string{"exec", "--namespace", in.Config.Namespace, pod, "--"}, args...)...)
	}

	_, headErr := exec("awslocal", "s3api", "head-bucket", "--bucket", Bucket)
	switch {
	case headErr == nil:
		// Left from an earlier run: empty it.
		if _, err := exec("awslocal", "s3", "rm", "s3://"+Bucket, "--recursive"); err != nil {
			return fmt.Errorf("empty bucket %s: %w", Bucket, err)
		}
		return nil
	case isTimeout(headErr):
		// Hung, not "bucket missing"; creating it would likely hang too.
		return fmt.Errorf("head-bucket %s: %w", Bucket, headErr)
	}
	if _, err := exec("awslocal", "s3", "mb", "s3://"+Bucket); err != nil {
		// Report both: if head-bucket failed for another reason, its error says why.
		return fmt.Errorf("create bucket %s: %w", Bucket, errors.Join(err, fmt.Errorf("head-bucket: %w", headErr)))
	}
	return nil
}

// InstallAgent installs or upgrades the agent from cfg.ChartVersion with the
// image under test, then waits for the rollout to finish.
//
// If the release already existed (KEEP_CLUSTER=1), the Deployment is restarted:
// the image tag is usually unchanged ("e2e"), so without a restart the old pod
// would keep running the previous build.
func (in *Installer) InstallAgent(ctx context.Context, opts AgentInstall) error {
	cfg := in.Config
	existed, err := in.releaseExists(ctx, cfg.ReleaseName)
	if err != nil {
		return err
	}

	if _, err := in.run(ctx, "helm", agentInstallArgs(cfg, opts)...); err != nil {
		return fmt.Errorf("install agent: %w", err)
	}

	deploy, err := in.agentDeployment(ctx)
	if err != nil {
		return err
	}
	if existed {
		if _, err := in.run(ctx, "kubectl", "rollout", "restart", deploy, "--namespace", cfg.Namespace); err != nil {
			return fmt.Errorf("restart agent: %w", err)
		}
	}

	// rollout status waits for the new ReplicaSet and for old pods to go away,
	// so a still-Ready old pod can't make this pass.
	if _, err := in.run(ctx, "kubectl", "rollout", "status", deploy, "--namespace", cfg.Namespace,
		"--timeout", in.timeout().String()); err != nil {
		return fmt.Errorf("agent rollout: %w; pods: %s", err, in.podReport(ctx, AgentSelector(cfg.ReleaseName)))
	}
	return nil
}

// agentInstallArgs builds the `helm upgrade --install` args for the agent.
func agentInstallArgs(cfg Config, opts AgentInstall) []string {
	args := []string{"upgrade", "--install", cfg.ReleaseName, agentChart,
		"--repo", AgentChartRepo,
		"--version", cfg.ChartVersion,
		"--namespace", cfg.Namespace, "--create-namespace",
		"--values", AgentValuesFile,
	}
	for _, f := range opts.ValuesFiles {
		args = append(args, "--values", f)
	}
	// --set-string, because --set parses values: an all-digit tag such as a
	// short SHA (12345678) would become a number and fail the chart, and a tag
	// like 1.10 would become the float 1.1.
	args = append(args,
		"--set-string", "image.registry="+cfg.ImageRef.Registry,
		"--set-string", "image.repository="+cfg.ImageRef.Repository,
		"--set-string", "image.tag="+cfg.ImageRef.Tag,
	)
	// Sorted so the command (and its log line) is the same every run.
	for _, k := range slices.Sorted(maps.Keys(opts.Set)) {
		args = append(args, "--set", k+"="+opts.Set[k])
	}
	return args
}

// releaseExists reports whether release is installed in cfg.Namespace.
func (in *Installer) releaseExists(ctx context.Context, release string) (bool, error) {
	_, err := in.run(ctx, "helm", "status", release, "--namespace", in.Config.Namespace)
	switch {
	case err == nil:
		return true, nil
	case strings.Contains(err.Error(), "release: not found"):
		return false, nil
	default:
		return false, fmt.Errorf("check helm release %s: %w", release, err)
	}
}

// agentDeployment returns "deployment/<name>" for the agent. It is found by
// label because the chart's naming depends on the release name.
func (in *Installer) agentDeployment(ctx context.Context) (string, error) {
	sel := AgentSelector(in.Config.ReleaseName)
	list, err := in.Client.AppsV1().Deployments(in.Config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return "", fmt.Errorf("find agent deployment: %w", err)
	}
	if len(list.Items) != 1 {
		return "", fmt.Errorf("find agent deployment: want 1 deployment matching %q in %s, found %d", sel, in.Config.Namespace, len(list.Items))
	}
	return "deployment/" + list.Items[0].Name, nil
}

// readyPod returns the name of a Ready, non-terminating pod matching selector.
func (in *Installer) readyPod(ctx context.Context, selector string) (string, error) {
	pods, err := in.Client.CoreV1().Pods(in.Config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "", err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp == nil && isPodReady(p) {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("no Ready pod matching %q in %s", selector, in.Config.Namespace)
}

// podReport describes the pods matching selector for an error message.
func (in *Installer) podReport(ctx context.Context, selector string) string {
	// The rollout may have failed because ctx expired; still try to report.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	pods, err := in.Client.CoreV1().Pods(in.Config.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return "list pods: " + err.Error()
	}
	_, desc := podsReady(pods.Items)
	return desc
}
