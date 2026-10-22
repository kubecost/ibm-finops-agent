# E2E Go test harness: context and plan

Working notes for the sprint 1 task "Go test runner skeleton/framework" (Trevor Walker, Alex Southard as available, Chris Tolbert helping).
Written 2026-10-21. Source: Warwick's pitch "Ship the FinOps Agent with Confidence" (Confluence, Kubecost space) and a read of this repo.

## Background

The pitch replaces manual testing of the FinOps agent with a closed "confidence loop": every PR runs automated end-to-end tests against fake backends, and every two weeks `develop` becomes a release candidate that soaks in the internal clusters. It says Go tests in `e2e/` replace the shell script.

Sprint 1 items around this task:

| Item | Owner | How it relates to the harness |
| --- | --- | --- |
| `CLOUDABILITY_UPLOAD_INTERVAL` (PR #270) | Alex | Needed before upload tests make sense (upload is hard-coded at 10m on develop) |
| Fake Cloudability behind DNS + test CA | Alex | Plugs into the harness through a `Backend` interface |
| Instana scraping + Smart Alerts as code (terraform) | Chris | Separate; uses the agent's `/metrics` |
| Integration build in one internal cluster | Chris | Lives in `kubecost/integration-ci-cd`; the harness should be able to target it later |
| Go test runner skeleton | Trevor / Alex | This doc |

## Decisions

- **Repo:** the harness lives in this repo under `e2e/`, not in `kubecost/integration-ci-cd`. That repo has no test code; its finops-agent workflows build/push release images (`finops-agent-build-release.yaml`), helm-install a PR build into a namespace on a shared EKS cluster (`finops-agent-push-pr-to-eks.yaml`), and bump Go weekly (`finops-agent-update-go-version.yaml`). Tests belong next to the code so they run on every PR against that PR's build.
- **Framework:** Ginkgo/Gomega (already in `go.mod` and used by `e2e/`) plus a small Go `harness` package that shells out to the `kind`, `helm` and `kubectl` CLIs, with client-go for waits and exec. Rejected: Helm/kind Go SDKs (heavy deps, more code), `sigs.k8s.io/e2e-framework` (new style to learn in a 2-week sprint), putting tests in the CI/CD repo (drifts from the code). A CLI call can be swapped for a library call inside `harness/` later without touching tests.
- **Base branch:** branch from `develop`, **not** `reliability/develop`. The reliability stack changes agent function code, which is out of scope; this project is test-only and changes no agent code.
- **Gating:** suite files carry `//go:build e2e`, so plain `go test ./...` never runs them. This replaces today's skip on `WORKING_DIR`.

## What exists in `e2e/` today (on develop)

| File | Role |
| --- | --- |
| `e2e.sh` | Does all the setup: kind cluster, image load (podman archive locally, docker in CI), namespace, localstack + `kubecost-store` bucket, agent chart from the remote `ibm-finops/finops-agent` repo (unpinned), a `stress` pod, polls for Ready, copies one sample out of `/opt/finops-agent/scratch/` with `kubectl exec`, then runs `go test ./e2e` |
| `e2e_suite_test.go` | Ginkgo bootstrap |
| `e2e_test.go` | 4 specs that read the copied files: known file list, node metadata, namespace metadata (`stress`, `ibm-finops-agent`), pod metadata (`stress`, `unified-agent`) |
| `values.yaml` | Agent values: localstack store, Cloudability `staging` with fake keys, 10s emission |
| `localstack-values.yaml` | Localstack 4.12, Pro off |
| `config.yaml` | Not referenced anywhere; leftover |

Known weaknesses: fixed `sleep`s and retry loops that continue on failure, no logs on failure, unpinned chart version, Makefile tests k8s 1.30-1.33 while `pr.yaml` tests 1.31-1.34, and the `unified-agent` pod check depends on the helm release name.

What develop does **not** have (only on reliability/develop): `/readyz`, `/startupz`, `/status`, drop metrics. Develop exposes `/healthz` (`cmd/finops-agent/main.go`), `/version` and `/metrics` (`pkg/http/server.go`).

## Target layout

```
e2e/
  harness/
    config.go      # settings from env, validated once (replaces top of e2e.sh)
    cluster.go     # kind create/reuse/delete, image load (setup_kind, cleanup)
    helm.go        # localstack, bucket, agent at pinned chart (setup_env, setup_localstack, deploy)
    kube.go        # client-go waits, exec, read file from pod (wait_for_metrics, get_sample_data)
    agent.go       # port-forward; GET /healthz /version /metrics
    backend.go     # interface for fakes (Alex's fake Cloudability)
    artifacts.go   # on failure: pod logs, describe, events, /metrics snapshot
  testdata/        # values.yaml, localstack-values.yaml
  suite_test.go    # BeforeSuite: cluster + install; AfterSuite: teardown; ReportAfterSuite: artifacts
  install_test.go  # pod Ready, /healthz 200, /version matches image, /metrics scrapes
  sample_test.go   # port of today's 4 checks, reading from the pod
```

Plus a `make e2e` target and an `e2e-go` job in `.github/workflows/pr.yaml`.

## PR sequence

Strangler approach: move one stage of `e2e.sh` into Go per PR, keep the old job running until the Go one matches it.

1. `harness/config.go` + `config_test.go`, build tag, `make e2e` running an empty suite. Done when `make e2e` passes.
2. `cluster.go`: create, reuse with `KEEP_CLUSTER=1`, delete, load image.
3. `helm.go` + client-go waits: localstack, bucket, agent at pinned chart. No fixed sleeps.
4. `agent.go` + `install_test.go`: Ready, `/healthz`, `/version`, `/metrics`.
5. `kube.go` + `sample_test.go`: the 4 existing checks; release name is a setting.
6. `artifacts.go`: logs, describe, events, `/metrics` saved to `ARTIFACTS_DIR` on failure.
7. Non-blocking `e2e-go` CI job (4-version matrix, uploads artifacts); fix Makefile version drift.
8. `backend.go` interface + `CLUSTER_MODE=kind|existing` so the suite can later run against Chris's EKS cluster.

Sprint 2: delete `e2e.sh` and `config.yaml`, make `e2e-go` blocking, add upload tests once PR #270 lands.

Split: PR 1 first (everything depends on it). Then one person takes 2-4 (setup), the other 5-6 (checks, artifacts) against a cluster kept up with `KEEP_CLUSTER=1`.

## Step 1 detail: `harness/config.go`

Replaces the top of `e2e.sh` (required `IMAGE` / `KUBERNETES_VERSION`, podman vs docker on `CI`, splitting the image into registry/repo/tag). The whole `harness/` package replaces the script; this file is only the settings part.

Purpose: one place that reads every env var, applies defaults, validates, and returns a `Config` struct the rest of the harness uses. Fails fast with all problems listed before any cluster is created. Image parsing is unit-tested (the bash split breaks on `localhost:5000/agent:e2e`).

```go
type Config struct {
	Image             string // required
	KubernetesVersion string // required
	ChartVersion      string // pinned agent chart version
	ClusterName       string // default "e2e-<KubernetesVersion>"
	Namespace         string // default "ibm-finops-agent"
	ReleaseName       string // default "unified-agent"
	ContainerRuntime  string // docker in CI, podman locally; CONTAINER_RUNTIME overrides
	KeepCluster       bool   // KEEP_CLUSTER=1
	ArtifactsDir      string
}

func ConfigFromEnv() (Config, error)
```

## Open items

- Agree option A, `e2e/` location, build tag and env var names with Trevor.
- Agree the `Backend` interface with Alex early (limited availability this sprint).
- Agree with Chris which repo owns the cluster deploy (integration-ci-cd) vs the tests (this repo).
- Pick the pinned chart version.
- Upload/ledger-style checks wait for PR #270 and the fake Cloudability; without drop metrics on develop, compare what the fake receives against what the agent emitted.
