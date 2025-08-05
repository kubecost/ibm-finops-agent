#!/bin/bash

set -e

: ${IMAGE:?Need to set metrics-agent IMAGE variable to test}
: ${KUBERNETES_VERSION:?Need to set KUBERNETES_VERSION to test}

if [ "${CI}" != "true" ]; then
  export WORKINGDIR=/private${TEMP_DIR}/testdata/e2e/e2e-${KUBERNETES_VERSION}
  export DOCKER_EXEC=""
else
  export WORKINGDIR=${TEMP_DIR}/testdata/e2e/e2e-${KUBERNETES_VERSION}
  export DOCKER_EXEC="docker exec -i e2e-${KUBERNETES_VERSION}-control-plane"
fi

IMAGE_TAG="${IMAGE##*:}"
IMAGE_NO_TAG="${IMAGE%:*}"
IMAGE_REG="${IMAGE_NO_TAG%%/*}"
IMAGE_REPO="${IMAGE_NO_TAG#*/}"

cleanup() {
  kind delete cluster --name=e2e-${KUBERNETES_VERSION} &> /dev/null || true
  if [ -d $WORKINGDIR ]; then
    echo "Cleaning up temp directory: ${WORKINGDIR}"
    rm -rf $WORKINGDIR
  fi
}

setup_kind() {
  export PATH=$PATH:$(go env GOPATH)/bin

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
      # Errors when trying to load docker-image if it is built with podman but is fine with an image-archive
      kind load image-archive e2e_image_archive.tar --name e2e-${KUBERNETES_VERSION} && echo "${IMAGE} image added to cluster" && break
    fi
    i=$[$i+1]
    sleep 15
  done
}

HELM_INSTALL="helm install unified-agent e2e-test/finops-agent \
--set agent.kubecost.enabled=false \
--set agent.cloudability.uploadRegion="staging" \
--set agent.cloudability.secret.create=true \
--set agent.cloudability.secret.cloudabilityAccessKey="XXX" \
--set agent.cloudability.secret.cloudabilitySecretKey="XXX" \
--set agent.cloudability.secret.cloudabilityEnvId="dfa07190-0acb-4758-8d1b-a76fb6c6730e" \
--set agent.cloudability.emissionInterval="10s" \
--set image.registry="${IMAGE_REG}" \
--set image.repository="${IMAGE_REPO}" \
--set image.tag="${IMAGE_TAG}" \
--set clusterId="e2e" \
--create-namespace -n ibm-finops-agent"

deploy(){
  mkdir -p -m 0777 ${WORKINGDIR}

  if [ ! -d $WORKINGDIR ]; then
    >&2 echo "Failed to create temp directory ${WORKINGDIR}"
    exit 1
  fi

  if [ "${CI}" = "true" ]; then
    docker cp ~/.kube/config e2e-${KUBERNETES_VERSION}-control-plane:/root/.kube/config
    ${DOCKER_EXEC} ${HELM_INSTALL}
  else
    ${DOCKER_EXEC} ${HELM_INSTALL}
  fi

  sleep 10
  ${DOCKER_EXEC} kubectl create ns stress
  ${DOCKER_EXEC} kubectl -n stress run stress --labels=app=stress --image=jfusterm/stress -- --cpu 50 --vm 1 --vm-bytes 127m
}

wait_for_metrics() {
  i=0
  until [ $i -ge 10 ]
  do 
    if [[ $(${DOCKER_EXEC} kubectl get pods -n ibm-finops-agent -l app.kubernetes.io/name=finops-agent -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') = "True" ]]; then
      echo "Agent pod is ready!" && break
    fi
    echo "waiting for agent pod to be ready"
    i=$[$i+1]
    sleep 5
  done
}

get_sample_data(){
  POD=$(${DOCKER_EXEC} kubectl get pod -n ibm-finops-agent -l app.kubernetes.io/name=finops-agent -o jsonpath="{.items[0].metadata.name}")
  i=0
  until [ $i -ge 5 ]
  do
    if [[ -n $(${DOCkER_EXEC} kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/) ]]; then
      echo "Scratch directory exists!"
      break
    fi
    
    echo "Waiting for scratch directory to initialize"
    sleep 30
    i=$[$i+1]
  done

  FLDR=$(${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/)
  SMPL=$(${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/${FLDR})

  i=0
  until [ $i -ge 5 ]
  do
    if [[ $(${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/$FLDR | wc -l) -gt 1 ]]; then
      echo "Sample is populated!"
      break
    fi
    
    echo "Waiting for sample to populate"
    sleep 30
    i=$[$i+1]
  done

  echo "Copying agent sample to ${WORKINGDIR}"
  ${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/${FLDR}/${SMPL} >> ${WORKINGDIR}/file_list.txt
  ${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/nodes.jsonl > ${WORKINGDIR}/nodes.jsonl
  ${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/namespaces.jsonl > ${WORKINGDIR}/namespaces.jsonl
  ${DOCKER_EXEC} kubectl exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/pods.jsonl > ${WORKINGDIR}/pods.jsonl
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