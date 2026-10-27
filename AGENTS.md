# AGENTS.md

Base branch is `develop`.

## The image

The base is `registry.access.redhat.com/hi/static`, a distroless Red Hat Hardened Image:

- No shell or libc: the binary must be built with `CGO_ENABLED=0`, and `kubectl exec` into the agent won't work.
- CA certs come from the base image.
- When changing the base, review `vex/ibm-finops-agent.openvex.json`.

## Local testing

Needs `podman`, `kind`, `helm`, `kubectl`, `yq`. Commands assume Apple Silicon; use `amd64` elsewhere.

```sh
export KIND_EXPERIMENTAL_PROVIDER=podman
export KUBECONFIG=$PWD/.kubeconfig   # gitignored; keeps kind out of your default kubeconfig
make test                            # unit tests
```

### Build

Pass `--platform` (podman may otherwise build `amd64`) and `--build-arg version` (Red Hat certification needs the `version`/`release` labels it sets).

```sh
# BEFORE from develop's Dockerfile, AFTER from your change
git show origin/develop:Dockerfile > Dockerfile.before
podman build --platform linux/arm64 -f Dockerfile.before -t localhost/agent:before --build-arg version=dev .
podman build --platform linux/arm64 -t localhost/agent:after --build-arg version=dev .
rm Dockerfile.before
```

### E2E (same as CI)

Creates a kind cluster per Kubernetes version, installs localstack and the published chart with `e2e/values.yaml`, then runs `go test ./e2e`.

```sh
podman tag localhost/agent:after localhost/e2e/ibm-finops-agent:e2e   # the tag e2e.sh loads
make add-e2e-chart
make test-k8s-1.33.0   # or `make e2e-test` for every version
```

### Chart on kind

```sh
kind create cluster --name agent-test
for t in before after; do podman save -o agent.tar localhost/agent:$t && kind load image-archive agent.tar --name agent-test; done; rm agent.tar
helm repo add finops-agent https://kubecost.github.io/finops-agent-chart/

# Fake Cloudability keys, so the agent starts and tries to upload. Also adds
# e2e's busybox sidecar, which mounts the agent's data volume read-only.
yq '{"sidecars": .sidecars}' e2e/values.yaml > test-values.yaml
cat >> test-values.yaml <<'EOF'
clusterId: agent-test
image: {registry: localhost, repository: agent, pullPolicy: Never}
resources: {requests: {cpu: 10m}}   # fits several releases on one kind node
agent:
  cloudability:
    enabled: true
    emissionInterval: 30s
    secret: {cloudabilityAccessKey: fake, cloudabilitySecretKey: fake, cloudabilityEnvId: fake, cloudabilityApiKey: fake}
EOF

helm install agent finops-agent/finops-agent -f test-values.yaml --set image.tag=after
kubectl get pods   # expect Running, 0 restarts
kubectl exec deploy/agent-finops-agent -c e2e-reader -- ls /opt/finops-agent/scratch/
```

Repeat with `--set image.tag=before` to compare against develop.

### Outbound TLS

With fake keys, `403 Forbidden` means TLS worked and `x509: certificate signed by unknown authority` means it didn't. To test a proxy with a private CA, put `mitmproxy` behind `agent.cloudability.outboundProxy` and enable `global.updateCaTrust` (see [#283](https://github.com/kubecost/ibm-finops-agent/pull/283)).

```sh
kubectl logs deploy/agent-finops-agent -c finops-agent | grep -E 'status code|x509'
```

### Full Kubecost chart on kind

Checks the agent's data reaches the Kubecost API.

```sh
helm repo add kubecost https://kubecost.github.io/kubecost
helm install kubecost kubecost/kubecost -n kubecost --create-namespace --wait --timeout 15m \
  --set finopsagent.fullImageName=localhost/agent:after
kubectl -n kubecost get pods    # all Running, 0 restarts
helm test kubecost -n kubecost  # expect Phase: Succeeded
kubectl -n kubecost port-forward svc/kubecost-frontend 9090:9090 &
curl -s 'localhost:9090/model/allocation?window=1d&aggregate=namespace&accumulate=true' | jq '.code, (.data[0] | keys)'
```

Clean up with `kind delete cluster --name agent-test && rm test-values.yaml`.

## CI

PR builds are pushed to `icr.io/kubecost/agent:pr-<PR>-<sha>` (amd64, arm64, s390x); a bot comments the tag on the PR.
