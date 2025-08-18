#!/bin/bash

set -e

: ${IMAGE:?Need to set metrics-agent IMAGE variable to test}
: ${KUBERNETES_VERSION:?Need to set KUBERNETES_VERSION to test}

# Assumes that you're running podman and macOS locally.
# This could be handled by some params if we want to handle alternative dev envs.
if [ "${CI}" != "true" ]; then
  export WORKINGDIR=/private${TEMP_DIR}/testdata/e2e/e2e-${KUBERNETES_VERSION}
  DOCKER=podman
else
  export WORKINGDIR=${TEMP_DIR}/testdata/e2e/e2e-${KUBERNETES_VERSION}
  DOCKER=docker
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
    ${DOCKER} save -o e2e_image_archive.tar localhost/e2e/ibm-finops-agent:e2e
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

deploy(){
  mkdir -p -m 0777 ${WORKINGDIR}

  if [ ! -d $WORKINGDIR ]; then
    >&2 echo "Failed to create temp directory ${WORKINGDIR}"
    exit 1
  fi

  kubectl create namespace ibm-finops-agent
  
  # Localstack bucket setup
  helm install localstack localstack/localstack -n ibm-finops-agent
  sleep 15
  LOCALSTACK_POD=$(kubectl get pods -n ibm-finops-agent -l "app.kubernetes.io/name=localstack" -o jsonpath="{.items[0].metadata.name}")
  i=0
  until [ $i -ge 5 ]
  do 
    if [[ $(kubectl get pods -n ibm-finops-agent -l "app.kubernetes.io/name=localstack" -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') = "True" ]]; then
      echo "Localstack pod is ready!" && break
    fi
    echo "Waiting for localstack pod to be ready..."
    i=$[$i+1]
    sleep 20
  done

  # Create kubecost-store bucket
  kubectl exec -n ibm-finops-agent $LOCALSTACK_POD -- awslocal s3 mb s3://kubecost-store

  # Install unified-agent
  helm install unified-agent ibm-finops/finops-agent -n ibm-finops-agent -f e2e/values.yaml

  # Create stress namespace & pod
  kubectl create ns stress
  kubectl -n stress run stress --labels=app=stress --image=jfusterm/stress -- --cpu 50 --vm 1 --vm-bytes 127m
}

wait_for_metrics() {
  i=0
  until [ $i -ge 10 ]
  do 
    if [[ $(kubectl get pods -n ibm-finops-agent -l app.kubernetes.io/name=finops-agent -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') = "True" ]]; then
      echo "Agent pod is ready!" && break
    fi
    echo "Waiting for agent pod to be ready..."
    i=$[$i+1]
    sleep 5
  done
}

get_sample_data(){
  POD=$(kubectl get pod -n ibm-finops-agent -l app.kubernetes.io/name=finops-agent -o jsonpath="{.items[0].metadata.name}")
  i=0
  until [ $i -ge 5 ]
  do
    if [[ -n $(kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/) ]]; then
      echo "Scratch directory exists!"
      break
    fi
    
    echo "Waiting for scratch directory to initialize..."
    sleep 30
    i=$[$i+1]
  done

  # Retrieve sample name
  FLDR=$(kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/)
  SMPL=$(kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/${FLDR})

  i=0
  until [ $i -ge 5 ]
  do
    if [[ $(kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/$FLDR | wc -l) -gt 1 ]]; then
      echo "Sample is populated!"
      break
    fi
    
    echo "Waiting for sample to populate..."
    sleep 30
    i=$[$i+1]
  done

  echo "Copying agent sample to ${WORKINGDIR}"
  # Copy all file names into file_list.txt
  kubectl exec -n ibm-finops-agent $POD -- ls tmp/scratch/${FLDR}/${SMPL} >> ${WORKINGDIR}/file_list.txt
  # Copy notable files to working dir
  kubectl exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/nodes.jsonl > ${WORKINGDIR}/nodes.jsonl
  kubectl exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/namespaces.jsonl > ${WORKINGDIR}/namespaces.jsonl
  kubectl exec -n ibm-finops-agent $POD -- cat tmp/scratch/${FLDR}/${SMPL}/pods.jsonl > ${WORKINGDIR}/pods.jsonl
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