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
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.2.1
	golangci-lint run

# $(call TEST_KUBERNETES, image_tag, prefix, git_commit)
define TEST_KUBERNETES
	KUBERNETES_VERSION=$(1) IMAGE=$(IMAGE_TAG) TEMP_DIR=$(TEMP_DIR) e2e/e2e.sh; \
		if [ $$? != 0 ]; then \
			exit 1; \
		fi;
endef

# To run the e2e tests locally, have an image built off the total UA docker file tagged 'localhost/e2e/ibm-finops-agent:e2e'
# and export it to IMAGE_TAG. This could be manually configured by editing it in the e2e_deployment.yaml and e2e.sh files
e2e-test:test-k8s-1.33.0 test-k8s-1.32.0 test-k8s-1.31.0 test-k8s-1.30.0 

test-k8s-1.33.0:
	$(call TEST_KUBERNETES,v1.33.0)

test-k8s-1.32.0:
	$(call TEST_KUBERNETES,v1.32.0)

test-k8s-1.31.0:
	$(call TEST_KUBERNETES,v1.31.0)

test-k8s-1.30.0:
	$(call TEST_KUBERNETES,v1.30.0)