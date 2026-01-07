FROM redhat/ubi9:latest AS build-env
ARG TARGETPLATFORM

RUN yum install -y unzip wget

RUN echo "TARGETPLATFORM: $TARGETPLATFORM"
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
        wget https://go.dev/dl/go1.25.5.linux-amd64.tar.gz && \
        tar -C /usr/local -xzf go1.25.5.linux-amd64.tar.gz && \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        wget https://go.dev/dl/go1.25.5.linux-arm64.tar.gz && \
        tar -C /usr/local -xzf go1.25.5.linux-arm64.tar.gz && \
    else \
        echo "unsupported target platform: $TARGETPLATFORM" && \
        exit 1; \
    fi

ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOPROXY=https://proxy.golang.org,direct
ENV GO111MODULE=on
#ENV GOSUMDB=off

RUN go version

RUN mkdir /app
WORKDIR /app

ARG version=dev
ARG commit=HEAD

# Copy Opencost Pricing Configs 
COPY ./ibm-finops-agent/opencost/configs ./opencost/configs

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
