FROM golang:1.25.7-alpine AS build-env

RUN mkdir /app
WORKDIR /app

ARG version=dev
ARG commit=HEAD


# Opencost-Core
COPY ./opencost/core/go.mod ./opencost/core/go.mod
COPY ./opencost/core/go.sum ./opencost/core/go.sum
RUN cd ./opencost/core && go mod download
COPY ./opencost/core ./opencost/core

# Opencost-Modules 
COPY ./opencost/modules/prometheus-source/go.mod ./opencost/modules/prometheus-source/go.mod
COPY ./opencost/modules/prometheus-source/go.sum ./opencost/modules/prometheus-source/go.sum
RUN cd ./opencost/modules/prometheus-source && go mod download
COPY ./opencost/modules/prometheus-source ./opencost/modules/prometheus-source

COPY ./opencost/modules/collector-source/go.mod ./opencost/modules/collector-source/go.mod
COPY ./opencost/modules/collector-source/go.sum ./opencost/modules/collector-source/go.sum
RUN cd ./opencost/modules/collector-source && go mod download
COPY ./opencost/modules/collector-source ./opencost/modules/collector-source

# Opencost 
COPY ./opencost/go.mod ./opencost/go.mod
COPY ./opencost/go.sum ./opencost/go.sum
RUN cd ./opencost && go mod download
COPY ./opencost ./opencost

# Copy Finops Agent Source 
COPY ./ibm-finops-agent/go.mod ./ibm-finops-agent/go.mod
COPY ./ibm-finops-agent/go.sum ./ibm-finops-agent/go.sum
COPY ./ibm-finops-agent ./ibm-finops-agent
RUN cd ./ibm-finops-agent && go mod download

# Build the binary
RUN cd ./ibm-finops-agent/cmd/finops-agent && set -e ;\
    go build -a -installsuffix cgo \
    -ldflags \
    "-X github.com/ibm/finops-agent/pkg/version.Version=${version} \
    -X github.com/ibm/finops-agent/pkg/version.GitCommit=${commit}" \
    -o /go/bin/app


FROM redhat/ubi9-minimal:latest

ARG commit
ARG version
ARG occommit

LABEL com.ibm-finops-agent.base.version="${version}"
LABEL com.ibm-finops-agent.base.commit="${commit}" 

# REQUIRED FOR RED HAT OPENSHIFT OPERATOR
LABEL name="ibm-finops-agent" \
    summary="IBM FinOps Agent" \
    description="IBM FinOps Agent" \
    vendor="IBM" \
    maintainer="kubecost-image-support@wwpdl.vnet.ibm.com" \
    version="${version}" \
    release="${version}"

ENV CONTAINERIZED="true"

# Add timezone data and set timezone to GMT
ENV TZ=UTC

ADD ./opencost/configs/default.json /models/default.json
ADD ./opencost/configs/azure.json /models/azure.json
ADD ./opencost/configs/aws.json /models/aws.json
ADD ./opencost/configs/gcp.json /models/gcp.json
ADD ./opencost/configs/awsreservationofferings.json /static/awsreservationofferings.json
ADD ./opencost/configs/alibaba.json /models/alibaba.json
COPY ./ibm-finops-agent/LICENSE /licenses/LICENSE

COPY --from=build-env /go/bin/app /go/bin/app
USER 1001

ENTRYPOINT ["/go/bin/app"]