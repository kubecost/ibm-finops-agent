package harness

import (
	"maps"
	"strings"
	"testing"
)

func TestParseImage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		image   string
		want    ImageRef
		wantErr string // substring; empty means success
	}{
		{image: "ghcr.io/kubecost/finops-agent:pr-1", want: ImageRef{"ghcr.io", "kubecost/finops-agent", "pr-1"}},
		{image: "localhost/e2e/ibm-finops-agent:e2e", want: ImageRef{"localhost", "e2e/ibm-finops-agent", "e2e"}},
		{image: "localhost:5000/agent:e2e", want: ImageRef{"localhost:5000", "agent", "e2e"}},
		// Cases the bash split in e2e.sh gets wrong.
		{image: "localhost:5000/agent", wantErr: "missing tag"},
		{image: "agent:e2e", wantErr: "missing registry"},
		{image: "kubecost/agent:e2e", wantErr: "not a registry host"},
		{image: "ghcr.io/x/y@sha256:abc", wantErr: "digests"},
		{image: "ghcr.io/x/y:", wantErr: "missing tag"},
		{image: "ghcr.io/:e2e", wantErr: "missing repository"},
	}
	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			t.Parallel()
			got, err := ParseImage(tt.image)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ParseImage(%q) error = %v, want containing %q", tt.image, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseImage(%q) unexpected error: %v", tt.image, err)
			}
			if got != tt.want {
				t.Fatalf("ParseImage(%q) = %+v, want %+v", tt.image, got, tt.want)
			}
		})
	}
}

// envFrom builds a lookup over a fixed map, so the real shell env never leaks in.
func envFrom(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func TestConfigFrom(t *testing.T) {
	t.Parallel()
	base := map[string]string{
		"IMAGE":              "ghcr.io/kubecost/finops-agent:pr-1",
		"KUBERNETES_VERSION": "v1.34.0",
	}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		maps.Copy(m, base)
		maps.Copy(m, extra)
		return m
	}

	t.Run("defaults", func(t *testing.T) {
		t.Parallel()
		c, err := configFrom(envFrom(base))
		if err != nil {
			t.Fatal(err)
		}
		want := Config{
			Image:             "ghcr.io/kubecost/finops-agent:pr-1",
			ImageRef:          ImageRef{"ghcr.io", "kubecost/finops-agent", "pr-1"},
			KubernetesVersion: "v1.34.0",
			ChartVersion:      DefaultChartVersion,
			ClusterName:       "e2e-v1.34.0",
			Namespace:         "ibm-finops-agent",
			ReleaseName:       "unified-agent",
			ContainerRuntime:  "podman",
			KeepCluster:       false,
			ArtifactsDir:      "_artifacts/e2e-v1.34.0",
		}
		if c != want {
			t.Fatalf("got  %+v\nwant %+v", c, want)
		}
	})

	t.Run("reports every missing required var at once", func(t *testing.T) {
		t.Parallel()
		_, err := configFrom(envFrom(nil))
		if err == nil {
			t.Fatal("expected error")
		}
		for _, s := range []string{"IMAGE", "KUBERNETES_VERSION"} {
			if !strings.Contains(err.Error(), s) {
				t.Errorf("error %q does not mention %s", err, s)
			}
		}
	})

	t.Run("CI=true defaults to docker", func(t *testing.T) {
		t.Parallel()
		c, err := configFrom(envFrom(with(map[string]string{"CI": "true"})))
		if err != nil || c.ContainerRuntime != "docker" {
			t.Fatalf("runtime = %q, err = %v; want docker", c.ContainerRuntime, err)
		}
	})

	t.Run("overrides", func(t *testing.T) {
		t.Parallel()
		c, err := configFrom(envFrom(with(map[string]string{
			"CI":                "true",
			"CONTAINER_RUNTIME": "podman",
			"CLUSTER_NAME":      "e2e-mine",
			"NAMESPACE":         "ns",
			"RELEASE_NAME":      "rel",
			"CHART_VERSION":     "9.9.9",
			"ARTIFACTS_DIR":     "/tmp/a",
			"KEEP_CLUSTER":      "1",
		})))
		if err != nil {
			t.Fatal(err)
		}
		if c.ContainerRuntime != "podman" || c.ClusterName != "e2e-mine" || c.Namespace != "ns" ||
			c.ReleaseName != "rel" || c.ChartVersion != "9.9.9" || c.ArtifactsDir != "/tmp/a" || !c.KeepCluster {
			t.Fatalf("overrides not applied: %+v", c)
		}
	})
	t.Run("ArtifactsDir follows CLUSTER_NAME when not set", func(t *testing.T) {
		t.Parallel()
		c, err := configFrom(envFrom(with(map[string]string{"CLUSTER_NAME": "e2e-mine"})))
		if err != nil {
			t.Fatal(err)
		}
		if c.ClusterName != "e2e-mine" {
			t.Fatalf("ClusterName = %q, want %q", c.ClusterName, "e2e-mine")
		}
		if c.ArtifactsDir != "_artifacts/e2e-mine" {
			t.Fatalf("ArtifactsDir = %q, want %q", c.ArtifactsDir, "_artifacts/e2e-mine")
		}
	})
	t.Run("whitespace-only counts as unset", func(t *testing.T) {
		t.Parallel()
		c, err := configFrom(envFrom(with(map[string]string{"NAMESPACE": "  "})))
		if err != nil || c.Namespace != "ibm-finops-agent" {
			t.Fatalf("namespace = %q, err = %v", c.Namespace, err)
		}
	})

	for name, env := range map[string]map[string]string{
		"bad k8s version":  {"KUBERNETES_VERSION": "1.34"},
		"bad runtime":      {"CONTAINER_RUNTIME": "containerd"},
		"bad KEEP_CLUSTER": {"KEEP_CLUSTER": "yes"},
		"bad image":        {"IMAGE": "agent:e2e"},
		// The harness deletes clusters by name, so only e2e- names are allowed.
		// That keeps it away from e.g. the user's default "kind" cluster.
		"CLUSTER_NAME without e2e- prefix": {"CLUSTER_NAME": "kind"},
		"CLUSTER_NAME escaping artifacts":  {"CLUSTER_NAME": "e2e-../../x"},
		"CLUSTER_NAME with uppercase":      {"CLUSTER_NAME": "e2e-Mine"},
		"CLUSTER_NAME prefix only":         {"CLUSTER_NAME": "e2e-"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := configFrom(envFrom(with(env))); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
