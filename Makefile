LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

ENVTEST_K8S_VERSION = 1.25.0
ENVTEST ?= $(LOCALBIN)/setup-envtest

ifndef TEMP_DIR
TEMP_DIR:=$(shell mktemp -d /tmp/ibm-finops-agent.XXXXXX)
endif

ifndef IMAGE_TAG
IMAGE_TAG:=localhost/e2e/ibm-finops-agent:e2e
endif

test: envtest
	@KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./... -coverprofile coverage.out

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	@test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

ci-lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.2
	golangci-lint run

# go-fix-check runs the Go fix tool to update deprecated or outdated API usage
# to current equivalents (e.g. old error patterns, renamed stdlib identifiers).
# -diff prints a unified diff instead of rewriting files, and exits non-zero
# if the diff is non-empty, which causes CI to fail.
go-fix-check:
	go fix -diff ./...

# $(call TEST_KUBERNETES, image_tag, prefix, git_commit)
define TEST_KUBERNETES
	KUBERNETES_VERSION=$(1) IMAGE=$(IMAGE_TAG) TEMP_DIR=$(TEMP_DIR) e2e/e2e.sh; \
		if [ $$? != 0 ]; then \
			exit 1; \
		fi;
endef

# To run the e2e tests locally, have an image built off the total UA docker file tagged 'localhost/e2e/ibm-finops-agent:e2e'
# and export it to IMAGE_TAG. This could be manually configured by editing the HELM_INSTALL variable in the e2e.sh files
e2e-test: add-e2e-chart test-k8s-1.33.0 test-k8s-1.32.0 test-k8s-1.31.0 test-k8s-1.30.0 

add-e2e-chart:
	helm repo add ibm-finops https://kubecost.github.io/finops-agent-chart
	helm repo add localstack https://helm.localstack.cloud

test-k8s-1.33.0:
	$(call TEST_KUBERNETES,v1.33.0)

test-k8s-1.32.0:
	$(call TEST_KUBERNETES,v1.32.0)

test-k8s-1.31.0:
	$(call TEST_KUBERNETES,v1.31.0)

test-k8s-1.30.0:
	$(call TEST_KUBERNETES,v1.30.0)

.PHONY: podman-build-push
podman-build-push:
	podman manifest rm $(IMAGETAG) > /dev/null 2>&1 || true
	podman manifest create $(IMAGETAG)
	podman build --rm --platform "linux/arm64" -f ./ibm-finops-agent/Dockerfile --manifest $(IMAGETAG) -t $(IMAGETAG)-arm64 ./ibm-finops-agent
	podman build --rm --platform "linux/amd64" -f ./ibm-finops-agent/Dockerfile --manifest $(IMAGETAG) -t $(IMAGETAG)-amd64 ./ibm-finops-agent
	podman manifest push $(IMAGETAG)
