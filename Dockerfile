FROM golang:1.26.2-alpine AS build-env

RUN mkdir /app
WORKDIR /app

ARG version=dev
ARG commit=HEAD

RUN go mod download

# Build the binary
RUN cd ./cmd/finops-agent && set -e ;\
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