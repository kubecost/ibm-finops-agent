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