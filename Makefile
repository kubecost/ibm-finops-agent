LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

ENVTEST_K8S_VERSION = 1.25.0
ENVTEST ?= $(LOCALBIN)/setup-envtest

ifndef TEMP_DIR
TEMP_DIR:=$(shell mktemp -d /tmp/ibm-finops-agent.XXXXXX)
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
	KUBERNETES_VERSION=$(1) IMAGE=ibm-finops-agent:$(1) TEMP_DIR=$(TEMP_DIR) e2e/e2e.sh; \
		if [ $$? != 0 ]; then \
			exit 1; \
		fi;
endef

e2e-test: test-k8s-1.32.0

test-k8s-1.32.0:
	$(call TEST_KUBERNETES,v1.32.0,$(PREFIX))

test-k8s-1.31.0:
	@KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(1.31.0) --bin-dir $(LOCALBIN) -p path)" go test ./...

test-k8s-1.30.0:
	@KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(1.30.0) --bin-dir $(LOCALBIN) -p path)" go test ./...

test-k8s-1.29.0:
	@KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(1.29.0) --bin-dir $(LOCALBIN) -p path)" go test ./...