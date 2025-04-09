FROM redhat/ubi9:latest AS build-env
ARG TARGETPLATFORM
ARG DUCK_VERSION
RUN yum install -y unzip \
                   wget \
                   ca-certificates \
                   yum-utils

# Install Go 1.24.0
RUN wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz && \
    rm go1.24.0.linux-amd64.tar.gz

ENV PATH="${PATH}:/usr/local/go/bin"
ENV GOPROXY=https://proxy.golang.org,direct
ENV GO111MODULE=on
ENV GOSUMDB=off

RUN go version

RUN mkdir /app
WORKDIR /app

ARG version=dev
ARG commit=HEAD

# First copy all go.mod and go.sum files
COPY ./opencost/core/go.mod ./opencost/core/go.mod
COPY ./opencost/core/go.sum ./opencost/core/go.sum

COPY ./opencost/modules/prometheus-source/go.mod ./opencost/modules/prometheus-source/go.mod
COPY ./opencost/modules/prometheus-source/go.sum ./opencost/modules/prometheus-source/go.sum

COPY ./opencost/go.mod ./opencost/go.mod
COPY ./opencost/go.sum ./opencost/go.sum

# Then copy the source code
COPY ./opencost/core ./opencost/core
COPY ./opencost/modules/prometheus-source ./opencost/modules/prometheus-source
COPY ./opencost ./opencost

# Now run go mod download for each module
RUN cd ./opencost/core && go mod download
RUN cd ./opencost/modules/prometheus-source && go mod download
RUN cd ./opencost && go mod download

# ibm-finops-agent
COPY ./ibm-finops-agent/go.mod ./ibm-finops-agent/go.mod
COPY ./ibm-finops-agent/go.sum ./ibm-finops-agent/go.sum
COPY ./ibm-finops-agent ./ibm-finops-agent
RUN cd ./ibm-finops-agent && go mod download

# Build the binary
RUN cd ./ibm-finops-agent/cmd/finops-agent && set -e ;\
    go build -a -installsuffix cgo \
    -ldflags \
    "-X github.com/kubecost/ibm-finops-agent/cmd/finops-agent/version.Version=${version} \
    -X github.com/kubecost/ibm-finops-agent/cmd/finops-agent/version.GitCommit=${commit}" \
    -o /go/bin/app


FROM redhat/ubi9-micro:latest

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

# Copy ca-certificates and ClickHouse files from the download-env stage
COPY --from=build-env /etc/pki/ca-trust /etc/pki/ca-trust
ENV CONTAINERIZED="true"

# Add timezone data and set timezone to GMT
ENV TZ=UTC

# ADD ./ibm-finops-agent/configs/default.json /models/default.json
# ADD ./ibm-finops-agent/configs/azure.json /models/azure.json
# ADD ./ibm-finops-agent/configs/aws.json /models/aws.json
# ADD ./ibm-finops-agent/configs/gcp.json /models/gcp.json
# ADD ./ibm-finops-agent/configs/alibaba.json /models/alibaba.json
# ADD ./ibm-finops-agent/assets/kubecost_logo@2x.jpg /assets/kubecost_logo@2x.jpg
# ADD ./ibm-finops-agent/configs/carbonlookupdata.csv /static/carbonlookupdata.csv
# ADD ./ibm-finops-agent/ubi9_eula.txt /licenses/ubi9_eula.txt
COPY --from=build-env /go/bin/app /go/bin/app
USER 1001
ENTRYPOINT ["/go/bin/app/ibm-finops-agent"]