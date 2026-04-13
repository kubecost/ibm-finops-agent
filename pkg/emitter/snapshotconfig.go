package emitter

import (
	"os"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"

	"strings"
)

const (
	SnapshotNodes                  = "nodes"
	SnapshotPods                   = "pods"
	SnapshotNamespaces             = "namespaces"
	SnapshotServices               = "services"
	SnapshotDaemonSets             = "daemonsets"
	SnapshotDeployments            = "deployments"
	SnapshotStatefulSets           = "statefulsets"
	SnapshotReplicaSets            = "replicasets"
	SnapshotPersistentVolumes      = "persistentvolumes"
	SnapshotPersistentVolumeClaims = "persistentvolumeclaims"
	SnapshotStorageClasses         = "storageclasses"
	SnapshotJobs                   = "jobs"
	SnapshotPodDisruptionBudgets   = "poddisruptionbudgets"
	SnapshotReplicationControllers = "replicationcontrollers"
	SnapshotResourceQuotas         = "resourcequotas"
)

// SnapshotAllResources is a list of all Kubernetes resources that can be snapshotted.
// This list is used to determine which resources to snapshot based on the KubernetesSnapshotConfig.
// It includes all resources defined in the KubernetesSnapshotConfig struct.
var SnapshotAllResources = []string{
	SnapshotNodes,
	SnapshotPods,
	SnapshotNamespaces,
	SnapshotServices,
	SnapshotDaemonSets,
	SnapshotDeployments,
	SnapshotStatefulSets,
	SnapshotReplicaSets,
	SnapshotPersistentVolumes,
	SnapshotPersistentVolumeClaims,
	SnapshotStorageClasses,
	SnapshotJobs,
	SnapshotPodDisruptionBudgets,
	SnapshotReplicationControllers,
	SnapshotResourceQuotas,
}

// KubernetesSnapshotConfig holds the configuration for Kubernetes snapshotting options, which is simply a
// set of flags indicating whether to snapshot specific Kubernetes resources.
type KubernetesSnapshotConfig struct {
	Nodes                  bool
	Pods                   bool
	Namespaces             bool
	Services               bool
	DaemonSets             bool
	Deployments            bool
	StatefulSets           bool
	ReplicaSets            bool
	PersistentVolumes      bool
	PersistentVolumeClaims bool
	StorageClasses         bool
	Jobs                   bool
	PodDisruptionBudgets   bool
	ReplicationControllers bool
	ResourceQuotas         bool
}

// NewKubernetesSnapshotConfig creates a new KubernetesSnapshotConfig instance with all fields set to false.
func NewKubernetesSnapshotConfig() *KubernetesSnapshotConfig {
	// defaults every snapshotted resources to false
	return new(KubernetesSnapshotConfig)
}

// NewKubernetesSnapshotConfigFrom creates a new KubernetesSnapshotConfig instance based on the provided list of enabled resources.
// It parses the list and sets the corresponding fields to true for each recognized resource.
func NewKubernetesSnapshotConfigFromEnabled(enabled []string) *KubernetesSnapshotConfig {
	ksc := NewKubernetesSnapshotConfig()
	for _, resource := range enabled {
		ksc.Set(resource, true)
	}
	return ksc
}

// NewKubernetesSnapshotConfigFrom creates a new KubernetesSnapshotConfig instance based on the provided list of disabled resources.
// It parses the list and sets the corresponding fields to false for each recognized resource. The rest are enabled by default.
func NewKubernetesSnapshotConfigFromDisabled(disabled []string) *KubernetesSnapshotConfig {
	ksc := NewKubernetesSnapshotConfig().EnableAll()
	for _, resource := range disabled {
		ksc.Set(resource, false)
	}
	return ksc
}

// EnableAll sets all the snapshot fields to true, enabling snapshotting for all resources.
func (ksc *KubernetesSnapshotConfig) EnableAll() *KubernetesSnapshotConfig {
	for _, resource := range SnapshotAllResources {
		ksc.Set(resource, true)
	}
	return ksc
}

// DisableAll sets all the snapshot fields to false.
func (ksc *KubernetesSnapshotConfig) DisableAll() *KubernetesSnapshotConfig {
	for _, resource := range SnapshotAllResources {
		ksc.Set(resource, false)
	}
	return ksc
}

// Set updates the specified field with the provided enabled value if it exists.
func (ksc *KubernetesSnapshotConfig) Set(field string, enabled bool) {
	switch strings.ToLower(field) {
	case SnapshotNodes:
		ksc.Nodes = enabled
	case SnapshotPods:
		ksc.Pods = enabled
	case SnapshotNamespaces:
		ksc.Namespaces = enabled
	case SnapshotServices:
		ksc.Services = enabled
	case SnapshotDaemonSets:
		ksc.DaemonSets = enabled
	case SnapshotDeployments:
		ksc.Deployments = enabled
	case SnapshotStatefulSets:
		ksc.StatefulSets = enabled
	case SnapshotReplicaSets:
		ksc.ReplicaSets = enabled
	case SnapshotPersistentVolumes:
		ksc.PersistentVolumes = enabled
	case SnapshotPersistentVolumeClaims:
		ksc.PersistentVolumeClaims = enabled
	case SnapshotStorageClasses:
		ksc.StorageClasses = enabled
	case SnapshotJobs:
		ksc.Jobs = enabled
	case SnapshotPodDisruptionBudgets:
		ksc.PodDisruptionBudgets = enabled
	case SnapshotReplicationControllers:
		ksc.ReplicationControllers = enabled
	case SnapshotResourceQuotas:
		ksc.ResourceQuotas = enabled
	default:
		log.Warnf("Unknown Kubernetes resource '%s' in snapshot configuration, ignoring.", field)
	}
}

// Append merges the provided KubernetesSnapshotConfig into the current instance, preserving
// all enabled options. If an option is enabled in either config, it will be enabled in the result.
// If config is nil, the method returns without making any changes.
func (ksc *KubernetesSnapshotConfig) Append(config *KubernetesSnapshotConfig) {
	if config == nil {
		return
	}
	ksc.Nodes = ksc.Nodes || config.Nodes
	ksc.Pods = ksc.Pods || config.Pods
	ksc.Namespaces = ksc.Namespaces || config.Namespaces
	ksc.Services = ksc.Services || config.Services
	ksc.DaemonSets = ksc.DaemonSets || config.DaemonSets
	ksc.Deployments = ksc.Deployments || config.Deployments
	ksc.StatefulSets = ksc.StatefulSets || config.StatefulSets
	ksc.ReplicaSets = ksc.ReplicaSets || config.ReplicaSets
	ksc.PersistentVolumes = ksc.PersistentVolumes || config.PersistentVolumes
	ksc.PersistentVolumeClaims = ksc.PersistentVolumeClaims || config.PersistentVolumeClaims
	ksc.StorageClasses = ksc.StorageClasses || config.StorageClasses
	ksc.Jobs = ksc.Jobs || config.Jobs
	ksc.PodDisruptionBudgets = ksc.PodDisruptionBudgets || config.PodDisruptionBudgets
	ksc.ReplicationControllers = ksc.ReplicationControllers || config.ReplicationControllers
	ksc.ResourceQuotas = ksc.ResourceQuotas || config.ResourceQuotas
}

// Now is used with the `SnapshotConfig` as an implementation to determine the current time.
type Now = func() time.Time

// defaultNow is the default `Now` implementation used to provide `time.Now().UTC()` from the
// go standard library. This should really only be overridden for testing purposes.
func defaultNow() time.Time {
	return time.Now().UTC()
}

// getScratchDir returns the scratch directory path from the SCRATCH_DIR environment variable,
// or the default path if not set.
func getScratchDir() string {
	if dir := os.Getenv("SCRATCH_DIR"); dir != "" {
		return dir
	}
	return "/opt/finops-agent"
}

// SnapshotConfig holds the configuration for general snapshotting options.
type SnapshotConfig struct {
	// UseMetricsCache indicates whether or not to use a cache for metrics query results.
	// NOTE: This is used when the metrics querier is reliant on prometheus.
	UseMetricsCache bool

	// MinutelyMetricsEnabled indicates whether or not to snapshot 10m resolution
	// metrics when the snapshot occurs.
	MinutelyMetricsEnabled bool

	// KubernetesSnapshotConfig holds the configuration for Kubernetes resources to snapshot.
	KubernetesSnapshot *KubernetesSnapshotConfig

	// ScratchDir is the directory path where snapshot state will be persisted.
	ScratchDir string

	// Now is the func used to determine the current time.
	Now Now
}

// WithKubernetesSnapshotConfig appends the provided KubernetesSnapshotConfig to the current SnapshotConfig.
// If the KubernetesSnapshot field is nil, it initializes a new KubernetesSnapshotConfig with all options disabled,
// then appends the provided configuration.
func (sc *SnapshotConfig) WithKubernetesSnapshotConfig(ksc *KubernetesSnapshotConfig) *SnapshotConfig {
	if sc.KubernetesSnapshot == nil {
		sc.KubernetesSnapshot = NewKubernetesSnapshotConfig()
	}
	sc.KubernetesSnapshot.Append(ksc)
	return sc
}

// NewSnapshotConfig creates a new SnapshotConfig instance populated with values
// parsed from environment variables.
func NewSnapshotConfigFromEnv() *SnapshotConfig {
	return &SnapshotConfig{
		UseMetricsCache:        !env.IsCollectorDataSourceEnabled(),
		MinutelyMetricsEnabled: env.IsMinuteMetricsEnabled(),
		ScratchDir:             getScratchDir(),
		Now:                    defaultNow,
	}
}

// DefaultSnapshotConfig returns a default SnapshotConfig instance.
func DefaultSnapshotConfig() *SnapshotConfig {
	return &SnapshotConfig{
		UseMetricsCache:        false,
		MinutelyMetricsEnabled: false,
		KubernetesSnapshot:     NewKubernetesSnapshotConfig().EnableAll(),
		ScratchDir:             "/opt/finops-agent",
		Now:                    defaultNow,
	}
}
