# Building IBM FinOps Agent

## Multi-Architecture Builds

The IBM FinOps Agent supports multi-architecture builds for `linux/amd64` and `linux/arm64` platforms.

### Prerequisites

- Docker with [Buildx](https://docs.docker.com/buildx/working-with-buildx/) support
- Docker Buildx is included in Docker Desktop and recent Docker Engine versions

### Quick Start

#### Using Make (Recommended)

```bash
# Build multi-arch image locally
make build-multiarch

# Build and push to registry
make build-multiarch-push

# Build with specific version
VERSION=v1.0.0 make build-multiarch

# See all available targets
make help
```

#### Using the Build Script Directly

```bash
# Build locally (default)
./build-multiarch.sh

# Build with custom version
./build-multiarch.sh --version v1.0.0

# Build and push to registry
./build-multiarch.sh --version v1.0.0 --registry gcr.io/my-project --push

# Build with custom tag
./build-multiarch.sh --version v1.0.0 --tag latest --push
```

### Build Script Options

```
Usage: ./build-multiarch.sh [OPTIONS]

OPTIONS:
    -v, --version VERSION       Version tag (default: dev)
    -c, --commit COMMIT         Git commit hash (default: current HEAD)
    -n, --name NAME             Image name (default: ibm-finops-agent)
    -t, --tag TAG               Image tag (default: same as version)
    -r, --registry REGISTRY     Registry prefix (default: localhost)
    -p, --push                  Push image to registry (default: false)
    -h, --help                  Show this help message
```

### Environment Variables

You can also use environment variables instead of command-line flags:

```bash
export VERSION=v1.0.0
export REGISTRY=gcr.io/my-project
export PUSH=true
./build-multiarch.sh
```

### Examples

#### Local Development Build

```bash
# Build for local testing (amd64 only will be loaded)
./build-multiarch.sh --version dev
```

#### Production Release Build

```bash
# Build and push to production registry
./build-multiarch.sh \
  --version v1.2.3 \
  --registry gcr.io/production-project \
  --push
```

#### Custom Registry and Tag

```bash
# Build with custom registry and tag
./build-multiarch.sh \
  --version v1.2.3 \
  --tag latest \
  --registry docker.io/myorg \
  --push
```

### Important Notes

1. **Local Loading Limitation**: When building without `--push`, only the `amd64` image can be loaded locally due to Docker limitations. To use multi-arch images, you must push to a registry.

2. **Buildx Builder**: The script automatically creates and manages a buildx builder named `ibm-finops-multiarch`. This builder persists across builds.

3. **Build Context**: The script must be run from the repository root directory where the `Dockerfile` is located.

4. **Build Arguments**: The script passes `version` and `commit` as build arguments to the Dockerfile.

### CI/CD Integration

The GitHub Actions workflow (`.github/workflows/build-test.yaml`) automatically builds multi-arch images on pull requests:

```yaml
platforms: "linux/amd64,linux/arm64"
```

For releases, the workflow in `.github/workflows/build-release.yaml` triggers the integration CI/CD pipeline.

### Troubleshooting

#### Buildx Not Available

```bash
# Install buildx (if not included in your Docker installation)
docker buildx install
```

#### Builder Issues

```bash
# Remove and recreate the builder
docker buildx rm ibm-finops-multiarch
./build-multiarch.sh
```

#### Platform-Specific Builds

To build for a single platform:

```bash
docker buildx build \
  --platform linux/amd64 \
  --build-arg version=dev \
  --build-arg commit=$(git rev-parse --short HEAD) \
  -t localhost/ibm-finops-agent:dev \
  --load \
  -f Dockerfile \
  .
```

### Verifying Multi-Arch Images

After pushing to a registry, verify the manifest includes both architectures:

```bash
docker buildx imagetools inspect gcr.io/my-project/ibm-finops-agent:v1.0.0
```

Expected output:
```
Name:      gcr.io/my-project/ibm-finops-agent:v1.0.0
MediaType: application/vnd.docker.distribution.manifest.list.v2+json
Digest:    sha256:...

Manifests:
  Name:      gcr.io/my-project/ibm-finops-agent:v1.0.0@sha256:...
  MediaType: application/vnd.docker.distribution.manifest.v2+json
  Platform:  linux/amd64

  Name:      gcr.io/my-project/ibm-finops-agent:v1.0.0@sha256:...
  MediaType: application/vnd.docker.distribution.manifest.v2+json
  Platform:  linux/arm64
```

## Standard Docker Build

For single-architecture builds, you can use standard Docker commands:

```bash
docker build \
  --build-arg version=dev \
  --build-arg commit=$(git rev-parse --short HEAD) \
  -t ibm-finops-agent:dev \
  -f Dockerfile \
  .
```

## Testing Builds

After building, test the image:

```bash
# Run locally
docker run --rm ibm-finops-agent:dev --help

# Run with environment variables
docker run --rm \
  -e LOG_LEVEL=debug \
  ibm-finops-agent:dev