# E2E Go harness: context and decisions

State as of PR #280 (merged to `develop`). Steps 1 to 4 of 8 are done. Steps 5 to 8 are designed but not started.
The old shell suite (`e2e/e2e.sh` plus `e2e/e2e_test.go`) still runs in CI and is untouched.

Use this file to understand why the harness is built the way it is, what was measured, and what is deliberately left for later.

## 1. What this is

A Go/Ginkgo test harness that replaces `e2e/e2e.sh` one stage at a time (strangler approach). It creates a kind cluster, loads the agent image, installs localstack and the agent, deploys a known workload, and checks the running agent. The goal is a "confidence loop": every PR runs end-to-end tests against fake backends.

What exists today:

- Setup: config, kind cluster, image load, localstack, bucket, stress pod, agent install. All with real waits instead of sleeps.
- Checks: agent pod Ready with no restarts, `/healthz`, `/version`, `/metrics`.

What does not exist yet: data checks (what the agent exports), CI job, failure artifacts. See section 8.

## 2. Layout

| Path | Purpose |
| --- | --- |
| `e2e/suite_test.go` | `//go:build e2e`. Ginkgo bootstrap. `BeforeSuite` does all setup, `AfterSuite` deletes the cluster unless `KEEP_CLUSTER` |
| `e2e/install_test.go` | `//go:build e2e`. The `Agent install` specs (label `install`) |
| `e2e/e2e_suite_test.go`, `e2e/e2e_test.go` | Old specs, now `//go:build !e2e`. Read files that `e2e.sh` copies out of the pod |
| `e2e/e2e.sh`, `e2e/values.yaml`, `e2e/localstack-values.yaml`, `e2e/config.yaml` | Old shell flow. `config.yaml` is an unused leftover |
| `e2e/testdata/` | Values files for the Go suite. Copies, not moves, because `e2e.sh` still reads the originals |
| `e2e/harness/config.go` | Reads and validates every setting once (`ConfigFromEnv`), `ParseImage` |
| `e2e/harness/exec.go` | `run`: the single place that shells out. `LogWriter` hook |
| `e2e/harness/cluster.go` | kind create, reuse, delete, image load, readiness |
| `e2e/harness/helm.go` | `Installer`: localstack, bucket, agent install and removal |
| `e2e/harness/kube.go` | client-go client, pod-readiness waits, pod descriptions for errors |
| `e2e/harness/stress.go` | The stress pod: a workload with known CPU and memory use |
| `e2e/harness/agent.go` | HTTP access to the agent through the API server's service proxy, `/metrics` parser |
| `e2e/harness/*_test.go` | Unit tests. No build tag, so `make test` runs them on every PR |
| `e2e/harness/fakes_test.go` | Fake `kind`/`kubectl`/`helm` scripts and helpers used by the unit tests |

## 3. Running it

```sh
# Unit tests: seconds, no cluster
go test ./e2e/harness/ -race
go vet -tags e2e ./e2e/...        # compiles the tagged suite too (lint does not)

# Real suite: about 2 minutes. Needs podman running, kind, helm, kubectl and the image
make e2e
make e2e KUBERNETES_VERSION=v1.31.0 IMAGE=ghcr.io/org/agent:tag
KEEP_CLUSTER=1 make e2e           # leave the cluster up, reuse it next time

# Subset of specs (make does not pass extra flags)
cd e2e && IMAGE=localhost/e2e/ibm-finops-agent:e2e KUBERNETES_VERSION=v1.34.0 \
  go test -tags e2e . -v -timeout 0 -ginkgo.v -ginkgo.label-filter=install

# Old shell suite
make add-e2e-chart && make test-k8s-1.33.0     # or: make e2e-test (4 versions)
```

Build the image under test (the Dockerfile is now distroless, see section 6):

```sh
podman build --layers=false --platform linux/arm64 --build-arg version=dev \
  -t localhost/e2e/ibm-finops-agent:e2e .
```

With `KEEP_CLUSTER=1`, inspect with `export KUBECONFIG=e2e/_artifacts/e2e-v1.34.0/kubeconfig`. Delete with
`KIND_EXPERIMENTAL_PROVIDER=podman kind delete cluster --name e2e-v1.34.0`.

## 4. Settings (all read in `config.go`)

| Variable | Default | Notes |
| --- | --- | --- |
| `IMAGE` | none, required | `make e2e` falls back to `IMAGE_TAG` (`localhost/e2e/ibm-finops-agent:e2e`) |
| `KUBERNETES_VERSION` | none, required | Must look like `v1.34.0` (a kindest/node tag). `make e2e` defaults to `v1.34.0` |
| `CHART_VERSION` | `1.0.32` | No `v` prefix |
| `CLUSTER_NAME` | `e2e-<version>` | Must match `^e2e-[a-z0-9][a-z0-9.-]*$` |
| `NAMESPACE` | `ibm-finops-agent` | |
| `RELEASE_NAME` | `unified-agent` | |
| `CONTAINER_RUNTIME` | `docker` if `CI=true`, else `podman` | Only those two |
| `KEEP_CLUSTER` | false | Parsed with `strconv.ParseBool` |
| `ARTIFACTS_DIR` | `_artifacts/<cluster>` | Relative paths resolve from `e2e/` (go test's working directory) |

Every problem is reported at once, before any cluster work.

## 5. Decisions and the reasons

### Scope and approach

- **Tests live in this repo under `e2e/`**, not in the CI/CD repo. They change in the same PR as the code and run against that PR's image.
- **Ginkgo/Gomega plus shelling out to `kind`/`helm`/`kubectl`, with client-go for waits.** Rejected: the Helm and kind Go SDKs (heavy, more code) and `sigs.k8s.io/e2e-framework` (new style to learn). One `run` helper means a CLI call can be swapped for a library call later without touching tests.
- **Build tags separate the suites.** New: `//go:build e2e`. Old: `//go:build !e2e`. `go test ./e2e` without tags still compiles the old suite, so the old CI job is unaffected. The `harness` package has no tag so its unit tests run in `make test`.
- **Test-only change.** No agent code was touched. Branch was cut from `develop`, not the reliability branch.
- **Agent settings are not env vars.** They are per-test inputs: `AgentInstall{ValuesFiles, Set}`, so one run can test several configurations. `Set` goes through `--set` so specs can pass booleans and numbers. The `image.*` keys cannot be overridden through `Set`: helm applies every `--set` before every `--set-string`, whatever the flag order. That is intended, the suite always tests `IMAGE`.
- **`IMAGE` and `KUBERNETES_VERSION` have no defaults in Go**, so a run cannot pass against the wrong build. The Makefile supplies the local fallback.
- **`ParseImage` is strict** (needs registry host and tag, rejects digests). The old bash split broke on `localhost:5000/agent` and registry-less names.
- **Charts are pinned**: agent `1.0.32`, localstack chart `0.7.1`, localstack image `4.12` (in `testdata/localstack-values.yaml`). An unpinned chart could break every PR at once.
- **Installs use `--repo <url>`**, not `helm repo add`. Local repo names differ from CI's (`finops-agent` vs `ibm-finops`).

### Cluster (`cluster.go`, `exec.go`)

- **`run` is the only place that shells out.** It returns stderr in errors, kills the child when the context ends (`WaitDelay` stops a stuck pipe), and layers env on top of `os.Environ()` so `PATH`/`HOME` survive.
- **Own kubeconfig** at `<ArtifactsDir>/kubeconfig`. `~/.kube/config` is never touched. It holds credentials, so artifact uploads (step 7) must exclude it.
- **`KIND_EXPERIMENTAL_PROVIDER` is always set** from the runtime (docker or podman). A value exported in the user's shell cannot override `CONTAINER_RUNTIME`.
- **Only `e2e-` clusters are ever deleted.** Protects a user's own kind cluster (for example the default `kind`) and keeps the name safe as a path. It does not protect a hand-made `e2e-*` cluster or two concurrent runs using one name. Ownership labels were considered and judged not worth it yet.
- **Readiness uses `kubectl wait` on all nodes**, after create and after reuse. `kind create --wait` logs a warning and still exits 0 when it times out, so it is not a guarantee.
- **A failed or cancelled create is cleaned up** on a fresh context (`context.WithoutCancel`), so a later `KEEP_CLUSTER=1` run cannot reuse a half-built cluster.
- **A broken kept cluster is recreated automatically** (for example after a podman restart) with a logged reason. Chosen over failing every `KEEP_CLUSTER=1` run until someone deletes it. The cost is that a broken cluster cannot be inspected. A cancelled context does not trigger recreation.
- **Image load**: docker uses `kind load docker-image`. podman saves a temp archive and uses `kind load image-archive` (`docker-image` fails for podman-built images). It loads on reused clusters too so a rebuilt image is picked up. Three attempts, 10s apart, then an error. `e2e.sh` carried on after failures.
- **`KEEP_CLUSTER` is kept although CI never uses it.** Setup is about 100s fresh and 23s reused, and it allows inspecting a failure. CI always starts on a fresh runner.

### Installing (`helm.go`)

Setup order in `BeforeSuite`: config, cluster, image, client, localstack, remove old agent, empty bucket, stress pod, agent.

- **The old agent is removed before the bucket is emptied.** On a kept cluster the old agent keeps writing (a collector file every minute), so emptying first let its data back in. Reproduced 2 of 2 times with the old order, 0 of 3 after the fix. The chart's `Recreate` strategy shortens the window to a few seconds, which made it intermittent. Every run is now a fresh agent install.
- **The bucket is checked first** (`head-bucket`), emptied with `s3 rm --recursive` if it exists, created with `s3 mb` if not. Each `kubectl exec` has a 30s timeout so a hung `awslocal` cannot use up the 15 minute setup budget.
- **`rollout restart` only if the release already existed** (detected by `helm status` printing `release: not found`). The image tag `e2e` does not change between builds and the pull policy is `IfNotPresent`, so helm alone leaves the old pod running. Decided on release existence, not "cluster reused": a spec that reinstalls the agent mid-run needs the restart too. Chosen over an image-ID pod annotation, which needs chart support.
- **Deployment and Service are found by label** (`app.kubernetes.io/name=finops-agent` plus `app.kubernetes.io/instance=<release>`). The chart names the Deployment `<release>-finops-agent`, or just the release name if it already contains `finops-agent`. `RELEASE_NAME` stays configurable because the old specs silently depended on the name `unified-agent`.
- **`--set-string` for `image.*`**. An all-digit tag such as a short SHA becomes a number under `--set` and breaks the chart. `1.10` would become `1.1`.
- **The stress pod is created before the agent**, so the agent's first samples include it. `e2e.sh` hid this because `helm install` did not wait.

### Waiting (`kube.go`)

- **No fixed sleeps.** Waits poll the real condition with their own timeout.
- **Errors say why.** They describe each pod (phase, `CrashLoopBackOff`, `ImagePullBackOff`, `Evicted` with its message, unschedulable).
- **Terminating pods and finished pods (Failed or Succeeded) are ignored** when judging readiness, but listed in the error. An evicted bare pod otherwise blocks the wait until the timeout.
- **A list call that fails because the deadline passed does not overwrite the last pod state seen**, so the timeout error still explains the cause.
- **`waitForPodsGone` counts terminating pods**, because a pod shutting down can still write data.
- **The agent rollout uses `kubectl rollout status`**, which also waits for old pods to go away.
- **Timeouts are Ginkgo's, not `go test`'s.** `-timeout 0 -ginkgo.timeout 45m`. `go test`'s timer kills the process without running `AfterSuite`, which leaks the cluster. `BeforeSuite` has 15m plus a 3m grace period so cleanup (up to 2m) is not cut off.

### Stress workload (`stress.go`)

- **Name, namespace and label stay `stress`/`stress`/`app=stress`**, because the old checks look for them.
- **Image is `registry.k8s.io/e2e-test-images/resource-consumer:1.15.0`**, replacing `jfusterm/stress`. That image uses a Docker v1 manifest, which current kind nodes cannot pull (`media type ...manifest.v1+prettyjws`). The old suite never noticed because it only checks that the pod object exists, not that it runs. resource-consumer is Kubernetes' own load generator, multi-arch, and idles until asked.
- **Load: 100m CPU and 128 MB memory**, requested over HTTP through the API server's pod proxy once the pod is Ready, for 2 hours. Limits are 500m and 256Mi. Constants `StressCPUMillicores` and `StressMemoryMegabytes` are exported so checks compare against them.
- **The pod is recreated every run.** Load requests add up (two 100m requests gave 193m), and a doubled memory request got the pod OOM-killed, which drops all load.
- **Measured**: the kubelet reports 100m CPU and about 136.5 MiB working set (the 128 MB request plus roughly 8 MiB of process overhead). The first partial allocation window averaged 0.090 cores and 135.1 MiB.
- **Deferred**: a generic `Workload` type and a `--vm-keep` style variant. Add them in step 5 only when value checks need more than one workload, as separate pods, not as a switch on this one.
- The code lives in its own file so `kube.go` stays general-purpose and step 5 can grow it.

### Reaching the agent (`agent.go`)

- **API server service proxy, not port-forward.** No background process, no local port, nothing to clean up if the suite is killed.
- **`Get` returns non-2xx as a `Response`.** client-go reports a proxied 404 or 503 as an error and `.StatusCode()` reads 0 for a 404, so the status code is extracted from the error. `err` only means "could not reach the agent".
- **`/version` is only checked for non-empty.** It returns the `version` build arg (`dev` for local builds), not the image tag. An `EXPECTED_VERSION` setting for CI is deferred to step 7.
- **`/metrics` is parsed with `expfmt`** and must contain `go_goroutines`. `cldy_agent_status` is not a Prometheus metric (see section 6).
- **The install specs** check exactly one non-terminating, non-finished agent pod, Ready, with zero restarts (a restart this early means a startup crash), then `/healthz` (polled up to 1 minute), `/version`, `/metrics`.

### Testing the harness itself

- Unit tests run the real harness code against **fake `kind`/`kubectl`/`helm` shell scripts** put first on `PATH` (they log every call) and client-go's fake clientset. This covers failure paths a real cluster cannot produce on demand: nodes that never get Ready, a cancelled create, a failed delete, a hung exec, an evicted pod. About 0.3s per case, so they run in `make test`.
- Tests that use the fakes cannot be parallel (`t.Setenv("PATH")`).
- Table tests assert the exact command sequence per scenario. New tests were checked by running them against the old code to confirm they fail.
- The fakes only encode assumptions about the tools. Real runs have already corrected two: `kind --wait` exits 0 on timeout, and a proxied 404 reads as status 0.

### Deliberately not done

| Item | Reason / when |
| --- | --- |
| Recover a helm release stuck in `pending-*`, and use `helm list -o json` instead of matching `release: not found` | CI starts fresh, so neither can affect a PR. Step 8, when a shared cluster is reused |
| Preload third-party images (localstack, resource-consumer, busybox) into kind | Step 7, together with image caching in CI. Preloading alone does not remove the registry dependency on a fresh runner. One 5+ minute pull was seen |
| Hard-code the release name | Rejected, kept configurable and found by label |
| A `SetString` field on `AgentInstall` | Not needed, the image is fixed by `IMAGE` |
| Remove `KEEP_CLUSTER` | Kept for iteration speed and step 8 |
| A `REUSE_AGENT` dev knob (skip the reinstall) | Rejected |
| Reading `upload/*.tgz` for a packaging check | Skipped. The upload tick is a hard-coded 10 minutes on `develop` (see section 6) |
| Decode KubeModel | Later. Legacy allocations first |

## 6. Facts about the agent and environment (measured unless marked)

### The agent

- HTTP server on container port 9003, exposed as Service port 80 (`http`). Probes use `/healthz`.
- `/healthz` returns 200 with an empty body, or 503 if an emitter reports unhealthy. Failed Cloudability uploads still count as upload attempts, so fake keys do not make it unhealthy.
- The Deployment uses `strategy: Recreate`.
- Helm applies all `--set` before all `--set-string`.

### What the agent writes to the localstack bucket (`kubecost-store`), path-style, readable unsigned through the service proxy

| Output | Key |
| --- | --- |
| Legacy allocations, assets | `federated/e2e/etl/bingen/{allocations,assets}/{10m,1h,1d}/<startEpoch>-<endEpoch>` |
| Network insights | same prefix, but only `1h` and `1d` (no 10m) |
| KubeModel | `finops-agent/<kube-system UID>/kubemodel/<res>/YYYY/MM/DD/<ts>` (not under `federated/`) |
| Heartbeat | `finops-agent/e2e/heartbeat/<ts>.json` |
| Diagnostics | `finops-agent/e2e/diagnostics/<ts>.json.gz` |
| Collector | `finops-agent/e2e/collector/<ts>.bingen`, one per minute regardless of export intervals |

- Chart default export intervals: allocation, asset, network insight and KubeModel 10m, heartbeat 5m, diagnostics 10m. The chart exposes all of them except KubeModel (use `extraEnvVars` `KUBEMODEL_EXPORT_INTERVAL`). Each run rewrites the current window's object until the window closes.
- Allocation files decode with `opencost.AllocationSet.UnmarshalBinary` (no new dependency). Keys look like `cluster/node/namespace/pod/container`. Fields: `CPUCoreUsageAverage` (cores), `RAMBytesUsageAverage` (bytes, working set). A pod with only limits shows requests equal to limits.
- Heartbeat JSON: `id`, `timestamp`, `uptime` (in minutes, despite the doc comment saying seconds), `application` (`finops-agent`), `version` (`dev (HEAD)`, the friendly version), `metadata` (`id`/`name` `e2e`, `provider` `custom`, `version` `1.34`). It has no health fields: presence and freshness are the signal.
- Diagnostics JSON: `application`, `startTime`, `results[]`, each with `name`, `category`, `details.passed`, `details.errors`. A real health signal. Which results pass in kind is not yet checked.
- KubeModel containers carry usage (`CPUCoreUsageAvg`, `RAMBytesUsageAvg`), pods do not. Pods and containers are only filled in when the collector reports `kubemodel_version`. Unverified in kind.

### Cloudability (`cldy/`)

- Scratch samples live in `/opt/finops-agent/scratch/<default-namespace UID>/<unixMillis>_<n>/`, one per snapshot (`EXPORTER_EMISSION_INTERVAL`) once 90% of `CLOUDABILITY_EMISSION_INTERVAL` has passed.
- A complete sample has 15 files: `agent-measurement.json`, `baseline-summary-<node>.json`, `stats-summary-<node>.json`, and `nodes`, `pods`, `deployments`, `replicasets`, `daemonsets`, `namespaces`, `services`, `replicationcontrollers`, `persistentvolumes`, `persistentvolumeclaims`, `statefulsets`, `jobs` as `.jsonl`.
- The first folder never has a `baseline-summary` file (14 files). The newest folder holds only its baseline file until the next emit completes the previous one. **A folder is complete when it contains `agent-measurement.json`**, which is written last. The old suite only passed because it read the first folder.
- `cldy_agent_status` is the `name` field inside `agent-measurement.json`, with `metrics.nodes_failed`, `metrics.uptime` (seconds), `values.*` (including `emission_interval`, `upload_region`, `cluster_version_git`) and `errors[]`.
- Stress pod values in the files: `stats-summary` shows 100m CPU and about 136.5 MiB working set. `pods.jsonl` shows limits 500m/256Mi and label `app=stress`.
- **The upload tick is a hard-coded 10 minutes** (`UPLOAD_FREQUENCY = 10`). PR #270 would add `CLOUDABILITY_UPLOAD_INTERVAL`; it had not landed on `develop` when checked. On each tick the agent tars all queued sample folders into `/opt/finops-agent/upload/<uid>_<time>.tgz` unchanged, then deletes the scratch folders before attempting the upload. The tgz is kept when there is no upload service or the upload fails. Scratch folders are therefore the exact bytes that get sent.
- **Cloudability cannot upload to localstack as the agent is written.** The custom S3 uploader uses aws-sdk-go v1 (1.55.8) with only a region set. That SDK ignores `AWS_ENDPOINT_URL`, so it would reach real AWS. Options: read the output from the agent pod (chosen), DNS rewrite plus a test CA, or change `cldy` (out of scope). Uploading to a bucket waits for the fake Cloudability backend work.
- **The fake-key startup login behaves differently depending on the network.** When egress to `frontdoor-stage.apptio.com` timed out, startup stalled about 3 minutes (60s timeout, three tries) and the federated exporters started only afterwards. When the host was reachable, the login failed in about 8 seconds with `400 Bad Request` (which also proves outbound TLS works with the new base image's CA bundle). Removing the fake keys skips the login entirely (agent ready 1.7s after pod start, logs `No complete upload configurations were detected`) but loses that TLS signal. Undecided, see section 9.

### The image (PR #283)

- The agent image is now distroless (`registry.access.redhat.com/hi/static`): no shell, no libc, binary built with `CGO_ENABLED=0`. **`kubectl exec` into the agent fails** (confirmed). The base ships CA certs.
- `e2e.sh` reads samples through a busybox sidecar named `e2e-reader` (`sidecars` in `e2e/values.yaml`) that mounts the agent's `data` volume read-only. Chart 1.0.32 renders it. `testdata/values.yaml` does not have it yet because the current harness never execs into the agent.
- Verified: with `develop` merged, the harness unit tests pass and `make e2e` passes 4 of 4 specs on the distroless image.

### kind, helm, localstack

- `localstack` chart `0.7.1` has `appVersion: latest`, so the image tag in the values file is what pins it. Its Service port `edge` (4566) is what the service proxy uses. Unsigned path-style `ListObjectsV2` and `GET` work through `/api/v1/namespaces/<ns>/services/localstack:edge/proxy/<bucket>/...`.
- Chart 1.0.32 exposes `agent.exporter.emissionInterval`, `agent.cloudability.emissionInterval`, `agent.exportIntervals.*`, `agent.cloudability.httpsClientTimeout`, `uploadRetryCount` and `sidecars`.

## 7. Gotchas for local development

- **macOS SDK linker error**: `make test` and `make ci-lint` fail locally for packages that link cgo (`cldy`, `pkg/emitter`, ...) with a `tapi error: malformed file`. This happens on clean `develop` too and does not affect CI. The harness package builds and tests fine.
- **podman VM disk**: the default VM disk is 23 GB. Untagged build leftovers (two 5.74 GB images were seen) made a build fail with `no space left on device`. Use `--layers=false`, and `podman image prune` removes untagged images. Give the machine enough CPU and memory: 4 CPUs and 8 GiB worked, 1 CPU and 1.9 GiB was too small.
- **podman build flags**: `--platform linux/arm64` (podman may build `amd64` otherwise) and `--build-arg version=dev` (the Red Hat labels need it, and `/version` then returns `dev`).
- `podman untag <image>` with no tag argument removes **all** tags on the image.
- **gopls errors on `&corev1.Pod{Name: ...}`**: Go 1.27 allows setting promoted fields of embedded structs in literals, and `go fix` rewrites code to that form. An older gopls shows "unknown field". Fix with `go install golang.org/x/tools/gopls@latest`. Do not revert the code, `make go-fix-check` in CI enforces it.
- `.golangci.yaml` excludes `_test.go`, so lint never sees the tagged suite. Run `go vet -tags e2e ./e2e/...`.
- `make e2e` output often appears only when the run finishes.
- Cluster name collisions: the Go suite and `e2e.sh` both default to `e2e-<version>`, so running one deletes the other's cluster.

## 8. What is left

### Step 5: data checks (design agreed, not built)

Each output gets its own `Ordered` container. Ginkgo shuffles top-level containers, so no container may depend on another having run. Each waits for its own data with `Eventually` (about 8 minutes budget). Ignore any object older than the agent pod's `status.startTime`, taken from the cluster (compare cluster clock to localstack clock, not the host's).

**5a, Cloudability container (also gives parity with the old suite).** Read the newest complete scratch folder through the `e2e-reader` sidecar with `kubectl exec -c e2e-reader`.

- Full file set present (`baseline-summary` optional), no unknown files.
- One node, named `<cluster>-control-plane`, no finalizers.
- Namespaces `stress`, `cfg.Namespace`, `kube-system`, no finalizers.
- Pods: the stress pod (label `app=stress`, limits 500m/256Mi) and the agent pod (name contains `cfg.ReleaseName`).
- `stats-summary`: stress pod 90 to 110m CPU, 128 to 145 MiB working set.
- `agent-measurement.json`: `nodes_failed=0`, no errors, `emission_interval=30s`, `upload_region=staging`, `cluster_version_git == cfg.KubernetesVersion`.
- Files: `harness/pod.go`, `e2e/cloudability_test.go`. The sidecar block must be added to `testdata/values.yaml`.

**5b, Federated container.** `harness/bucket.go` (list with pagination, get, through the service proxy), `harness/decode.go`, `e2e/federated_test.go`.

- Wait for a 10m allocation object written after the agent started in which the stress container has several minutes of data.
- Stress container: CPU 0.08 to 0.12 cores, RAM 128 to 145 MiB, CPU request about 0.5.
- Namespaces present: `stress`, agent namespace, `kube-system`, `local-path-storage`. Every container running in the live cluster (client-go) appears.
- Asset objects exist (decoding unverified). KubeModel objects exist (presence only).

**5c, Heartbeat container.** At least 2 heartbeats after start, about the interval apart, newest within 2x the interval, `uptime` increasing, `application`/`version`/`metadata.version` as expected, `version` starts with the `/version` body. Newest diagnostics report decodes and every result passes (allow-list any that legitimately fail in kind, with a comment).

**5d, background recorder (optional).** Polls `/healthz`, restarts and heartbeats for the whole run. Its assertion belongs in `AfterSuite` because container order is shuffled. Reusable for step 6.

**Values for the Go suite** (proposed, with measured effect: scratch folder with a baseline at about +60s, bucket allocation/KubeModel/heartbeat/diagnostics by about +69s after pod start, with the keys removed):

```yaml
agent:
  exporter:
    emissionInterval: 30s
  exportIntervals:
    allocation: 1m
    asset: 1m
    networkInsight: 1m
    heartbeat: 1m
    diagnostics: 1m
  kubecost:
    enabled: true
  cloudability:
    enabled: true
    uploadRegion: staging
    emissionInterval: 30s
    # secret: cloudabilityAccessKey/SecretKey/EnvId "XXX" is the open decision below
extraEnvVars:
  - name: KUBEMODEL_EXPORT_INTERVAL
    value: 1m
sidecars:
  - name: e2e-reader
    image: busybox:1.37
    command: ["sleep", "infinity"]
    volumeMounts:
      - name: data
        mountPath: /opt/finops-agent
        readOnly: true
```

### Steps 6 to 8 and after

- **Step 6, artifacts**: on failure save pod logs, `describe`, events and a `/metrics` snapshot to `ARTIFACTS_DIR`. Exclude the kubeconfig from any upload.
- **Step 7, CI**: non-blocking `e2e-go` job with the 4-version matrix and artifact upload. Fix version drift (`make e2e-test` runs 1.30 to 1.33, `pr.yaml` runs 1.31 to 1.34). Add `go vet -tags e2e ./e2e/...`. Pass an absolute `ARTIFACTS_DIR`. Preload and cache third-party images. Add `EXPECTED_VERSION` for `/version`. Measure real startup timings.
- **Step 8**: a `Backend` interface for a fake Cloudability, and `CLUSTER_MODE=kind|existing` so the suite can target a shared cluster. Handle `pending-*` releases and switch to `helm list -o json` then.
- **After parity**: delete `e2e.sh`, `e2e/config.yaml`, the original values files and the old specs, make the Go job blocking, add upload tests once #270 lands.

## 9. Open questions

1. **Keep or drop the fake Apptio keys in `testdata/values.yaml`?** Keeping them costs about 8 seconds on a reachable network (up to about 3 minutes if egress to the Apptio host times out) and preserves a check that outbound TLS works with the new base image's CA bundle (a `x509` error in the agent log would show it; no spec reads it yet). Dropping them starts the agent in under 2 seconds. Shortening `httpsClientTimeout` and `uploadRetryCount` bounds the cost if the keys stay.
2. Does KubeModel contain pods and containers in kind? Needed before decoding it.
3. Which diagnostics results pass in kind? Needed for the allow-list in 5c.
4. Does `AssetSet` decode cleanly? Unverified.
5. Tune the usage ranges in 5a and 5b after a few runs. They come from a small number of observations.
6. What does `finops-agent/e2e/collector/<ts>.bingen` hold? It appears right after start and every minute.
