# AGENTS.md

Base branch is `develop`.

## Image gotchas

The runtime base is `registry.access.redhat.com/hi/static:latest`, a distroless Red Hat Hardened Image:

- No shell, no libc. The binary must stay `CGO_ENABLED=0`, or the container dies with `exec /go/bin/app: no such file or directory`. `kubectl exec` into the agent fails, so read its files through a sidecar (see below).
- CA certs come from the base, at `/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem`.
- The base sets `WORKDIR /tmp` and `HOME=/tmp`; the Dockerfile resets `WORKDIR /`.
- `USER 1001` matches the Helm chart's `runAsUser`.
- When changing the base, review `vex/ibm-finops-agent.openvex.json` (see `vex/README.md`).

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

### Outbound TLS

With fake keys, `403 Forbidden` in the agent log means TLS worked; `x509: certificate signed by unknown authority` means it failed.

```sh
kubectl logs deploy/agent-finops-agent -c finops-agent | grep -E 'status code|x509'
```

To test a proxy with a private CA (what the chart's `updateCaTrust` is for), run `mitmproxy` signing with a throwaway CA:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 2 -subj '/CN=test CA'
kubectl create secret generic mitm-ca --from-file=mitmproxy-ca.pem=<(cat ca.key ca.crt)
kubectl create secret generic ca-certs-secret --from-file=test-ca.crt=ca.crt   # chart's default updateCaTrust secret
rm ca.key ca.crt

kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata: {name: mitm}
spec:
  selector: {matchLabels: {app: mitm}}
  template:
    metadata: {labels: {app: mitm}}
    spec:
      containers:
        - name: mitm
          image: docker.io/mitmproxy/mitmproxy:12.3.1
          # mitmproxy writes into its confdir, so copy the CA out of the read-only secret
          command: ["sh", "-c", "cp /ca/mitmproxy-ca.pem /conf/ && exec mitmdump --set confdir=/conf --listen-port 8080 --flow-detail 1"]
          volumeMounts: [{name: ca, mountPath: /ca}, {name: conf, mountPath: /conf}]
      volumes: [{name: ca, secret: {secretName: mitm-ca}}, {name: conf, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata: {name: mitm}
spec: {selector: {app: mitm}, ports: [{port: 8080}]}
EOF
kubectl rollout status deploy/mitm

PROXY="--set agent.cloudability.outboundProxy=http://mitm:8080"
helm install proxy-noca finops-agent/finops-agent -f test-values.yaml --set image.tag=after $PROXY   # expect x509
helm install proxy-ca   finops-agent/finops-agent -f test-values.yaml --set image.tag=after $PROXY \
  --set global.updateCaTrust.enabled=true                                                          # expect 403
kubectl logs deploy/mitm | grep -E 'POST|<<|TLS handshake'   # requests went through the proxy
```

Repeat any install with `--set image.tag=before` to compare against develop.

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
