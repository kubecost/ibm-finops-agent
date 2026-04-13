#!/bin/bash
set -e

# Multi-architecture Docker image build script for ibm-finops-agent
# Builds for linux/amd64 and linux/arm64 platforms

# Default values
VERSION="${VERSION:-dev}"
COMMIT="${COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo 'HEAD')}"
IMAGE_NAME="${IMAGE_NAME:-ibm-finops-agent}"
IMAGE_TAG="${IMAGE_TAG:-${VERSION}}"
REGISTRY="${REGISTRY:-localhost}"
PUSH="${PUSH:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Build multi-architecture Docker image for ibm-finops-agent

OPTIONS:
    -v, --version VERSION       Version tag (default: dev)
    -c, --commit COMMIT         Git commit hash (default: current HEAD)
    -n, --name NAME             Image name (default: ibm-finops-agent)
    -t, --tag TAG               Image tag (default: same as version)
    -r, --registry REGISTRY     Registry prefix (default: localhost)
    -p, --push                  Push image to registry (default: false)
    -h, --help                  Show this help message

EXAMPLES:
    # Build locally without pushing
    $0 -v v1.0.0

    # Build and push to registry
    $0 -v v1.0.0 -r gcr.io/my-project -p

    # Build with custom tag
    $0 -v v1.0.0 -t latest

ENVIRONMENT VARIABLES:
    VERSION, COMMIT, IMAGE_NAME, IMAGE_TAG, REGISTRY, PUSH

EOF
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -v|--version)
            VERSION="$2"
            shift 2
            ;;
        -c|--commit)
            COMMIT="$2"
            shift 2
            ;;
        -n|--name)
            IMAGE_NAME="$2"
            shift 2
            ;;
        -t|--tag)
            IMAGE_TAG="$2"
            shift 2
            ;;
        -r|--registry)
            REGISTRY="$2"
            shift 2
            ;;
        -p|--push)
            PUSH="true"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            usage
            ;;
    esac
done

# Construct full image name
FULL_IMAGE="${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"

# Default platforms
PLATFORMS="${PLATFORMS:-linux/amd64,linux/arm64}"

echo -e "${GREEN}=== IBM FinOps Agent Multi-Arch Build ===${NC}"
echo -e "Version:    ${YELLOW}${VERSION}${NC}"
echo -e "Commit:     ${YELLOW}${COMMIT}${NC}"
echo -e "Image:      ${YELLOW}${FULL_IMAGE}${NC}"
echo -e "Platforms:  ${YELLOW}${PLATFORMS}${NC}"
echo -e "Push:       ${YELLOW}${PUSH}${NC}"
echo ""

# Check if docker buildx is available
if ! docker buildx version &> /dev/null; then
    echo -e "${RED}Error: docker buildx is not available${NC}"
    echo "Please install Docker Buildx: https://docs.docker.com/buildx/working-with-buildx/"
    exit 1
fi

# Create builder if it doesn't exist
BUILDER_NAME="ibm-finops-multiarch"
if ! docker buildx inspect ${BUILDER_NAME} &> /dev/null; then
    echo -e "${YELLOW}Creating buildx builder: ${BUILDER_NAME}${NC}"
    docker buildx create --name ${BUILDER_NAME} --use
else
    echo -e "${GREEN}Using existing buildx builder: ${BUILDER_NAME}${NC}"
    docker buildx use ${BUILDER_NAME}
fi

# Ensure builder is running
docker buildx inspect --bootstrap

# Build arguments
BUILD_ARGS="--build-arg version=${VERSION} --build-arg commit=${COMMIT}"

# Push flag
if [ "${PUSH}" = "true" ]; then
    PUSH_FLAG="--push"
    echo -e "${YELLOW}Image will be pushed to registry${NC}"
else
    PUSH_FLAG="--load"
    echo -e "${YELLOW}Image will be loaded locally (amd64 only when using --load)${NC}"
    echo -e "${YELLOW}Note: Multi-arch images cannot be loaded locally. Use --push to push to registry.${NC}"
fi

# Prepare build context
# The Dockerfile expects ./ibm-finops-agent and ./opencost directories
# For local builds, we need to set up the context properly
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_CONTEXT_DIR=$(mktemp -d)
trap "rm -rf ${BUILD_CONTEXT_DIR}" EXIT

echo -e "${YELLOW}Preparing build context...${NC}"

# Create directory structure expected by Dockerfile
mkdir -p "${BUILD_CONTEXT_DIR}/ibm-finops-agent"
mkdir -p "${BUILD_CONTEXT_DIR}/opencost/configs"

# Copy ibm-finops-agent files
echo -e "${YELLOW}Copying ibm-finops-agent files...${NC}"
rsync -a --exclude='.git' --exclude='bin' --exclude='*.test' "${SCRIPT_DIR}/" "${BUILD_CONTEXT_DIR}/ibm-finops-agent/"

# Copy opencost configs (they're already in our repo)
echo -e "${YELLOW}Copying opencost configs...${NC}"
cp -r "${SCRIPT_DIR}/opencost/configs/"* "${BUILD_CONTEXT_DIR}/opencost/configs/"

# Build the image
echo -e "${GREEN}Building multi-architecture image...${NC}"
docker buildx build \
    --platform ${PLATFORMS} \
    ${BUILD_ARGS} \
    -t ${FULL_IMAGE} \
    ${PUSH_FLAG} \
    -f "${BUILD_CONTEXT_DIR}/ibm-finops-agent/Dockerfile" \
    "${BUILD_CONTEXT_DIR}"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Build successful!${NC}"
    echo -e "Image: ${YELLOW}${FULL_IMAGE}${NC}"
    
    if [ "${PUSH}" = "true" ]; then
        echo -e "${GREEN}✓ Image pushed to registry${NC}"
    else
        echo -e "${YELLOW}Note: Only amd64 image loaded locally. For multi-arch, use --push${NC}"
    fi
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi

# Made with Bob
