package harness

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Default Chart version pins the version that the tests will install
// docs explicitly called out have the image pinned.
const DefaultChartVersion = "v1.0.31"

var k8sVersionRE = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// ClusterNamePrefix is required on every cluster name. The harness deletes
// clusters by name, so the prefix keeps it from touching clusters it didn't
// create, such as the user's default "kind" cluster.
const ClusterNamePrefix = "e2e-"

// clusterNameRE is ClusterNamePrefix plus kind's allowed characters. It also
// keeps the name safe to use as a directory under ArtifactsDir (no '/').
var clusterNameRE = regexp.MustCompile(`^` + regexp.QuoteMeta(ClusterNamePrefix) + `[a-z0-9][a-z0-9.-]*$`)

// ImageRef is IMAGE split the way the chart wants it (image.registry/repository/tag).
type ImageRef struct {
	Registry   string
	Repository string
	Tag        string
}

// Config holds every setting the harness reads from the environment.
type Config struct {
	Image             string   // IMAGE, required
	ImageRef          ImageRef // parsed from Image
	KubernetesVersion string   // KUBERNETES_VERSION, required, e.g. v1.34.0
	ChartVersion      string   // CHART_VERSION, default DefaultChartVersion
	ClusterName       string   // CLUSTER_NAME, default "e2e-<KubernetesVersion>"
	Namespace         string   // NAMESPACE, default "ibm-finops-agent"
	ReleaseName       string   // RELEASE_NAME, default "unified-agent"
	ContainerRuntime  string   // CONTAINER_RUNTIME, default docker when CI=true, else podman
	KeepCluster       bool     // KEEP_CLUSTER, e.g. 1/true
	ArtifactsDir      string   // ARTIFACTS_DIR, default "_artifacts/<ClusterName>"
}

// ConfigFromEnv reads, defaults and validates all settings. On failure the
// error lists every problem at once, before any cluster is created.
func ConfigFromEnv() (Config, error) {
	return configFrom(os.LookupEnv)
}

func configFrom(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, def string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}

	var errs []error
	c := Config{
		// Image and Kubernetes version left with no default to ensure they are passed
		// defaults could lead to getting tests to pass on the wrong build
		Image:             get("IMAGE", ""),
		KubernetesVersion: get("KUBERNETES_VERSION", ""),
		ChartVersion:      get("CHART_VERSION", DefaultChartVersion),
		Namespace:         get("NAMESPACE", "ibm-finops-agent"),
		ReleaseName:       get("RELEASE_NAME", "unified-agent"),
	}
	c.ClusterName = get("CLUSTER_NAME", ClusterNamePrefix+c.KubernetesVersion)
	c.ArtifactsDir = get("ARTIFACTS_DIR", filepath.Join("_artifacts", c.ClusterName))

	defaultRuntime := "podman"
	if get("CI", "") == "true" {
		defaultRuntime = "docker"
	}
	c.ContainerRuntime = get("CONTAINER_RUNTIME", defaultRuntime)

	if ref, err := ParseImage(c.Image); err != nil {
		errs = append(errs, fmt.Errorf("IMAGE: %w", err))
	} else {
		c.ImageRef = ref
	}

	switch {
	case c.KubernetesVersion == "":
		errs = append(errs, errors.New("KUBERNETES_VERSION is required (e.g. v1.34.0)"))
	case !k8sVersionRE.MatchString(c.KubernetesVersion):
		errs = append(errs, fmt.Errorf("KUBERNETES_VERSION %q must look like v1.34.0 (a kindest/node tag)", c.KubernetesVersion))
	}

	// Skip when the name is just the default built from a missing version;
	// the KUBERNETES_VERSION error already covers that.
	if get("CLUSTER_NAME", "") != "" || c.KubernetesVersion != "" {
		if !clusterNameRE.MatchString(c.ClusterName) {
			errs = append(errs, fmt.Errorf("CLUSTER_NAME %q must start with %q and use only a-z, 0-9, '.' and '-'", c.ClusterName, ClusterNamePrefix))
		}
	}

	if c.ContainerRuntime != "docker" && c.ContainerRuntime != "podman" {
		errs = append(errs, fmt.Errorf("CONTAINER_RUNTIME %q must be docker or podman", c.ContainerRuntime))
	}

	keep, err := strconv.ParseBool(get("KEEP_CLUSTER", "false"))
	if err != nil {
		errs = append(errs, fmt.Errorf("KEEP_CLUSTER: %w", err))
	}
	c.KeepCluster = keep

	return c, errors.Join(errs...)
}

// ParseImage splits registry/repo[/...]:tag. The first path component is treated
// as a registry only if it has a '.' or ':' or is "localhost" (Docker's rule).
// A tag is required; digests and registry-less names are rejected.
func ParseImage(image string) (ImageRef, error) {
	if strings.Contains(image, "@") {
		return ImageRef{}, fmt.Errorf("%q: digests are not supported, use a tag", image)
	}

	name, tag := image, ""
	// A ':' only starts a tag if it comes after the last '/', so registry ports aren't mistaken for tags.
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		name, tag = image[:i], image[i+1:]
	}
	if tag == "" {
		return ImageRef{}, fmt.Errorf("%q: missing tag", image)
	}

	slash := strings.Index(name, "/")
	if slash < 0 {
		return ImageRef{}, fmt.Errorf("%q: missing registry host (e.g. ghcr.io/... or localhost/...)", image)
	}
	reg, repo := name[:slash], name[slash+1:]
	if !strings.ContainsAny(reg, ".:") && reg != "localhost" {
		return ImageRef{}, fmt.Errorf("%q: %q is not a registry host", image, reg)
	}
	if repo == "" {
		return ImageRef{}, fmt.Errorf("%q: missing repository", image)
	}
	return ImageRef{Registry: reg, Repository: repo, Tag: tag}, nil
}
