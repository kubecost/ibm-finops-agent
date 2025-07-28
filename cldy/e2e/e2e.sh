#!/bin/bash

set -e

: ${IMAGE:?Need to set metrics-agent IMAGE variable to test}
: ${KUBERNETES_VERSION:?Need to set KUBERNETES_VERSION to test}

# Maybe this should be CI = true instead of OS = darwin
OS=$(uname)
if [ "$OS" = "Darwin" ]; then
  export WORKINGDIR=/private${TEMP_DIR}/testdata/e2e/e2e-${KUBERNETES_VERSION}
  export KUBECTL="kubectl"
else
  export WORKINGDIR=${TEMP_DIR}/testdata/e2e/e2e-${KUBERNETES_VERSION}
  export KUBECTL="docker exec -i e2e-${KUBERNETES_VERSION}-control-plane kubectl --server=https://127.0.0.1:6443"
fi

cleanup() {
  kind delete cluster --name=e2e-${KUBERNETES_VERSION} &> /dev/null || true
  if [ -d $WORKINGDIR ]; then
    echo "Cleaning up temp directory: ${WORKINGDIR}"
    rm -rf $WORKINGDIR
  fi
}

setup_kind() {
  export PATH=$(go env GOPATH)/bin:$PATH

  cleanup
  if ! (kind create cluster --name=e2e-${KUBERNETES_VERSION} --image=kindest/node:${KUBERNETES_VERSION}) ; then
    echo "Could not create kind cluster"
    exit 1
  fi

  sleep 2
  kubectl version

  if [ "${CI}" != "true" ]; then
    podman save -o e2e_image_archive.tar localhost/e2e/ibm-finops-agent:e2e
  fi

  i=0
  until [ $i -ge 5 ]
  do
    if [ "${CI}" = "true" ]; then
      kind load docker-image ${IMAGE} --name e2e-${KUBERNETES_VERSION} && echo "${IMAGE} image added to cluster" && break
    else
      # Errors when trying to load docker-image if it is built with podman
      kind load image-archive e2e_image_archive.tar --name e2e-${KUBERNETES_VERSION} && echo "${IMAGE} image added to cluster" && break
    fi
    i=$[$i+1]
    sleep 15
  done
}

deploy(){
  mkdir -p -m 0777 ${WORKINGDIR}

  if [ ! -d $WORKINGDIR ]; then
    >&2 echo "Failed to create temp directory ${WORKINGDIR}"
    exit 1
  fi

  if [ "${CI}" = "true" ]; then
    docker cp ~/.kube/config e2e-${KUBERNETES_VERSION}-control-plane:/root/.kube/config
    ${KUBECTL} apply -f -  < e2e/e2e_deployment.yaml
  else
    ${KUBECTL} apply -f e2e/e2e_deployment.yaml
    kubectl -n ibm-finops-agent patch deployment unified-agent --patch \
    "{\"spec\": {\"template\": {\"spec\": {\"containers\": [{\"name\": \"unified-agent\", \"image\": \"localhost/e2e/ibm-finops-agent:e2e\" }]}}}}"
  fi

  sleep 10
  ${KUBECTL} create ns stress
  ${KUBECTL} -n stress run stress --labels=app=stress --image=jfusterm/stress -- --cpu 50 --vm 1 --vm-bytes 127m
}

wait_for_metrics() {
  # Wait for metrics-agent pod ready
  while [[ $(${KUBECTL} get pods -n ibm-finops-agent -l app=unified-agent -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') != "True" ]]; do
    echo "waiting for pod ready" && sleep 5;
  done

}

get_sample_data(){
  echo "Waiting for agent data collection check: docker cp e2e-${KUBERNETES_VERSION}-control-plane:/tmp ${WORKINGDIR}"
  sleep 30
  POD=$(${KUBECTL} get pod -n ibm-finops-agent -l app=unified-agent -o jsonpath="{.items[0].metadata.name}")
  FLDR=$(${KUBECTL} exec -n ibm-finops-agent $POD -- ls tmp/scratch/)
  SMPL=$(${KUBECTL} exec -n ibm-finops-agent $POD -- ls tmp/scratch/${FLDR})
  sleep 60
  ${KUBECTL} exec -n ibm-finops-agent $POD -- ls tmp/scratch/${FLDR}/${SMPL} >> ${WORKINGDIR}/file_list.txt
  ${KUBECTL} exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/nodes.jsonl > ${WORKINGDIR}/nodes.jsonl
  ${KUBECTL} exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/namespaces.jsonl > ${WORKINGDIR}/namespaces.jsonl
  ${KUBECTL} exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/pods.jsonl > ${WORKINGDIR}/pods.jsonl
}

run_tests() {
  echo "running tests: WORKING_DIR=${WORKINGDIR} KUBERNETES_VERSION=${KUBERNETES_VERSION} go test ./e2e -v"
  WORKING_DIR=${WORKINGDIR} KUBERNETES_VERSION=${KUBERNETES_VERSION} go test ./e2e -v
}

trap cleanup EXIT
setup_kind
deploy
wait_for_metrics
get_sample_data
run_tests