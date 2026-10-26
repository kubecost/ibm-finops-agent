FROM golang:1.27.3-alpine AS build-env

RUN mkdir /app
WORKDIR /app

ARG version=dev
ARG commit=HEAD

# Copy Go module files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY . .

# Build a static binary; vex/ibm-finops-agent.openvex.json relies on it
RUN cd ./cmd/finops-agent && set -e ;\
    CGO_ENABLED=0 go build -a -installsuffix cgo \
    -ldflags \
    "-X github.com/ibm/finops-agent/pkg/version.Version=${version} \
    -X github.com/ibm/finops-agent/pkg/version.GitCommit=${commit}" \
    -o /go/bin/app


# vex/ibm-finops-agent.openvex.json describes this base image; review it when changing the base
# Red Hat Hardened Image (distroless, ships CA certs + tzdata)
FROM registry.access.redhat.com/hi/static:latest

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
COPY LICENSE /licenses/LICENSE

COPY --from=build-env /go/bin/app /go/bin/app
# 1001, not the base's 65532: the Helm chart runs every pod as runAsUser/fsGroup 1001
USER 1001

ENTRYPOINT ["/go/bin/app"]
