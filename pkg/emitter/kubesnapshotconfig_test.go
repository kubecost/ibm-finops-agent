package emitter

import (
	"reflect"
	"testing"
)

func TestNewKubernetesSnapshotConfig(t *testing.T) {
	config := NewKubernetesSnapshotConfig()

	// All fields should be false by default
	if config.Nodes || config.Pods || config.Namespaces || config.Services ||
		config.DaemonSets || config.Deployments || config.StatefulSets ||
		config.ReplicaSets || config.PersistentVolumes || config.PersistentVolumeClaims ||
		config.StorageClasses || config.Jobs || config.PodDisruptionBudgets ||
		config.ReplicationControllers || config.ResourceQuotas {
		t.Error("NewKubernetesSnapshotConfig should initialize all fields to false")
	}
}

func TestNewKubernetesSnapshotConfigFromEnabled(t *testing.T) {
	tests := []struct {
		name     string
		enabled  []string
		expected *KubernetesSnapshotConfig
	}{
		{
			name:    "empty list",
			enabled: []string{},
			expected: &KubernetesSnapshotConfig{
				Nodes: false, Pods: false, Namespaces: false, Services: false,
				DaemonSets: false, Deployments: false, StatefulSets: false,
				ReplicaSets: false, PersistentVolumes: false, PersistentVolumeClaims: false,
				StorageClasses: false, Jobs: false, PodDisruptionBudgets: false,
				ReplicationControllers: false, ResourceQuotas: false,
			},
		},
		{
			name:    "single resource",
			enabled: []string{"nodes"},
			expected: &KubernetesSnapshotConfig{
				Nodes: true, Pods: false, Namespaces: false, Services: false,
				DaemonSets: false, Deployments: false, StatefulSets: false,
				ReplicaSets: false, PersistentVolumes: false, PersistentVolumeClaims: false,
				StorageClasses: false, Jobs: false, PodDisruptionBudgets: false,
				ReplicationControllers: false, ResourceQuotas: false,
			},
		},
		{
			name:    "multiple resources",
			enabled: []string{"nodes", "pods", "services"},
			expected: &KubernetesSnapshotConfig{
				Nodes: true, Pods: true, Namespaces: false, Services: true,
				DaemonSets: false, Deployments: false, StatefulSets: false,
				ReplicaSets: false, PersistentVolumes: false, PersistentVolumeClaims: false,
				StorageClasses: false, Jobs: false, PodDisruptionBudgets: false,
				ReplicationControllers: false, ResourceQuotas: false,
			},
		},
		{
			name:    "all resources",
			enabled: SnapshotAllResources,
			expected: &KubernetesSnapshotConfig{
				Nodes: true, Pods: true, Namespaces: true, Services: true,
				DaemonSets: true, Deployments: true, StatefulSets: true,
				ReplicaSets: true, PersistentVolumes: true, PersistentVolumeClaims: true,
				StorageClasses: true, Jobs: true, PodDisruptionBudgets: true,
				ReplicationControllers: true, ResourceQuotas: true,
			},
		},
		{
			name:    "case insensitive",
			enabled: []string{"NODES", "Pods", "SERVICES"},
			expected: &KubernetesSnapshotConfig{
				Nodes: true, Pods: true, Namespaces: false, Services: true,
				DaemonSets: false, Deployments: false, StatefulSets: false,
				ReplicaSets: false, PersistentVolumes: false, PersistentVolumeClaims: false,
				StorageClasses: false, Jobs: false, PodDisruptionBudgets: false,
				ReplicationControllers: false, ResourceQuotas: false,
			},
		},
		{
			name:    "invalid resource ignored",
			enabled: []string{"nodes", "invalid-resource", "pods"},
			expected: &KubernetesSnapshotConfig{
				Nodes: true, Pods: true, Namespaces: false, Services: false,
				DaemonSets: false, Deployments: false, StatefulSets: false,
				ReplicaSets: false, PersistentVolumes: false, PersistentVolumeClaims: false,
				StorageClasses: false, Jobs: false, PodDisruptionBudgets: false,
				ReplicationControllers: false, ResourceQuotas: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewKubernetesSnapshotConfigFromEnabled(tt.enabled)

			if !reflect.DeepEqual(config, tt.expected) {
				t.Errorf("Expected %+v, got %+v", tt.expected, config)
			}
		})
	}
}

func TestNewKubernetesSnapshotConfigFromDisabled(t *testing.T) {
	tests := []struct {
		name     string
		disabled []string
		expected *KubernetesSnapshotConfig
	}{
		{
			name:     "empty list - all enabled",
			disabled: []string{},
			expected: &KubernetesSnapshotConfig{
				Nodes: true, Pods: true, Namespaces: true, Services: true,
				DaemonSets: true, Deployments: true, StatefulSets: true,
				ReplicaSets: true, PersistentVolumes: true, PersistentVolumeClaims: true,
				StorageClasses: true, Jobs: true, PodDisruptionBudgets: true,
				ReplicationControllers: true, ResourceQuotas: true,
			},
		},
		{
			name:     "single resource disabled",
			disabled: []string{"nodes"},
			expected: &KubernetesSnapshotConfig{
				Nodes: false, Pods: true, Namespaces: true, Services: true,
				DaemonSets: true, Deployments: true, StatefulSets: true,
				ReplicaSets: true, PersistentVolumes: true, PersistentVolumeClaims: true,
				StorageClasses: true, Jobs: true, PodDisruptionBudgets: true,
				ReplicationControllers: true, ResourceQuotas: true,
			},
		},
		{
			name:     "multiple resources disabled",
			disabled: []string{"nodes", "pods", "services"},
			expected: &KubernetesSnapshotConfig{
				Nodes: false, Pods: false, Namespaces: true, Services: false,
				DaemonSets: true, Deployments: true, StatefulSets: true,
				ReplicaSets: true, PersistentVolumes: true, PersistentVolumeClaims: true,
				StorageClasses: true, Jobs: true, PodDisruptionBudgets: true,
				ReplicationControllers: true, ResourceQuotas: true,
			},
		},
		{
			name:     "all resources disabled",
			disabled: SnapshotAllResources,
			expected: &KubernetesSnapshotConfig{
				Nodes: false, Pods: false, Namespaces: false, Services: false,
				DaemonSets: false, Deployments: false, StatefulSets: false,
				ReplicaSets: false, PersistentVolumes: false, PersistentVolumeClaims: false,
				StorageClasses: false, Jobs: false, PodDisruptionBudgets: false,
				ReplicationControllers: false, ResourceQuotas: false,
			},
		},
		{
			name:     "case insensitive disabled",
			disabled: []string{"NODES", "Pods", "SERVICES"},
			expected: &KubernetesSnapshotConfig{
				Nodes: false, Pods: false, Namespaces: true, Services: false,
				DaemonSets: true, Deployments: true, StatefulSets: true,
				ReplicaSets: true, PersistentVolumes: true, PersistentVolumeClaims: true,
				StorageClasses: true, Jobs: true, PodDisruptionBudgets: true,
				ReplicationControllers: true, ResourceQuotas: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := NewKubernetesSnapshotConfigFromDisabled(tt.disabled)

			if !reflect.DeepEqual(config, tt.expected) {
				t.Errorf("Expected %+v, got %+v", tt.expected, config)
			}
		})
	}
}

func TestKubernetesSnapshotConfig_EnableAll(t *testing.T) {
	config := NewKubernetesSnapshotConfig()

	// Initially all should be false
	if config.Nodes || config.Pods || config.Namespaces || config.Services ||
		config.DaemonSets || config.Deployments || config.StatefulSets ||
		config.ReplicaSets || config.PersistentVolumes || config.PersistentVolumeClaims ||
		config.StorageClasses || config.Jobs || config.PodDisruptionBudgets ||
		config.ReplicationControllers || config.ResourceQuotas {
		t.Error("Expected all fields to be false initially")
	}

	// Enable all
	result := config.EnableAll()

	// Should return the same instance
	if result != config {
		t.Error("EnableAll should return the same instance")
	}

	// All fields should now be true
	if !config.Nodes || !config.Pods || !config.Namespaces || !config.Services ||
		!config.DaemonSets || !config.Deployments || !config.StatefulSets ||
		!config.ReplicaSets || !config.PersistentVolumes || !config.PersistentVolumeClaims ||
		!config.StorageClasses || !config.Jobs || !config.PodDisruptionBudgets ||
		!config.ReplicationControllers || !config.ResourceQuotas {
		t.Error("EnableAll should set all fields to true")
	}
}

func TestKubernetesSnapshotConfig_DisableAll(t *testing.T) {
	config := NewKubernetesSnapshotConfig().EnableAll()

	// Initially all should be true
	if !config.Nodes || !config.Pods || !config.Namespaces || !config.Services ||
		!config.DaemonSets || !config.Deployments || !config.StatefulSets ||
		!config.ReplicaSets || !config.PersistentVolumes || !config.PersistentVolumeClaims ||
		!config.StorageClasses || !config.Jobs || !config.PodDisruptionBudgets ||
		!config.ReplicationControllers || !config.ResourceQuotas {
		t.Error("Expected all fields to be true initially")
	}

	// Disable all
	result := config.DisableAll()

	// Should return the same instance
	if result != config {
		t.Error("DisableAll should return the same instance")
	}

	// All fields should now be false
	if config.Nodes || config.Pods || config.Namespaces || config.Services ||
		config.DaemonSets || config.Deployments || config.StatefulSets ||
		config.ReplicaSets || config.PersistentVolumes || config.PersistentVolumeClaims ||
		config.StorageClasses || config.Jobs || config.PodDisruptionBudgets ||
		config.ReplicationControllers || config.ResourceQuotas {
		t.Error("DisableAll should set all fields to false")
	}
}

func TestKubernetesSnapshotConfig_Set(t *testing.T) {
	config := NewKubernetesSnapshotConfig()

	tests := []struct {
		field    string
		enabled  bool
		expected func(*KubernetesSnapshotConfig) bool
	}{
		{"nodes", true, func(c *KubernetesSnapshotConfig) bool { return c.Nodes }},
		{"pods", true, func(c *KubernetesSnapshotConfig) bool { return c.Pods }},
		{"namespaces", true, func(c *KubernetesSnapshotConfig) bool { return c.Namespaces }},
		{"services", true, func(c *KubernetesSnapshotConfig) bool { return c.Services }},
		{"daemonsets", true, func(c *KubernetesSnapshotConfig) bool { return c.DaemonSets }},
		{"deployments", true, func(c *KubernetesSnapshotConfig) bool { return c.Deployments }},
		{"statefulsets", true, func(c *KubernetesSnapshotConfig) bool { return c.StatefulSets }},
		{"replicasets", true, func(c *KubernetesSnapshotConfig) bool { return c.ReplicaSets }},
		{"persistentvolumes", true, func(c *KubernetesSnapshotConfig) bool { return c.PersistentVolumes }},
		{"persistentvolumeclaims", true, func(c *KubernetesSnapshotConfig) bool { return c.PersistentVolumeClaims }},
		{"storageclasses", true, func(c *KubernetesSnapshotConfig) bool { return c.StorageClasses }},
		{"jobs", true, func(c *KubernetesSnapshotConfig) bool { return c.Jobs }},
		{"poddisruptionbudgets", true, func(c *KubernetesSnapshotConfig) bool { return c.PodDisruptionBudgets }},
		{"replicationcontrollers", true, func(c *KubernetesSnapshotConfig) bool { return c.ReplicationControllers }},
		{"resourcequotas", true, func(c *KubernetesSnapshotConfig) bool { return c.ResourceQuotas }},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			config.Set(tt.field, tt.enabled)
			if tt.expected(config) != tt.enabled {
				t.Errorf("Set(%s, %v) failed", tt.field, tt.enabled)
			}
		})
	}

	// Test case insensitive
	config.Set("NODES", false)
	if config.Nodes != false {
		t.Error("Set should be case insensitive")
	}

	// Test setting to false
	config.Set("pods", false)
	if config.Pods != false {
		t.Error("Set should be able to set to false")
	}

	// Test unknown field - should not panic
	config.Set("unknown", true)
	// Should not affect any fields
}

func TestKubernetesSnapshotConfig_Append(t *testing.T) {
	tests := []struct {
		name     string
		config1  *KubernetesSnapshotConfig
		config2  *KubernetesSnapshotConfig
		expected *KubernetesSnapshotConfig
	}{
		{
			name:     "both empty",
			config1:  NewKubernetesSnapshotConfig(),
			config2:  NewKubernetesSnapshotConfig(),
			expected: NewKubernetesSnapshotConfig(),
		},
		{
			name:     "first empty, second has values",
			config1:  NewKubernetesSnapshotConfig(),
			config2:  NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"}),
			expected: NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"}),
		},
		{
			name:     "first has values, second empty",
			config1:  NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"}),
			config2:  NewKubernetesSnapshotConfig(),
			expected: NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"}),
		},
		{
			name:     "overlapping values",
			config1:  NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"}),
			config2:  NewKubernetesSnapshotConfigFromEnabled([]string{"pods", "services"}),
			expected: NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods", "services"}),
		},
		{
			name:     "all enabled in both",
			config1:  NewKubernetesSnapshotConfig().EnableAll(),
			config2:  NewKubernetesSnapshotConfig().EnableAll(),
			expected: NewKubernetesSnapshotConfig().EnableAll(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of config1 to preserve original
			result := &KubernetesSnapshotConfig{
				Nodes:                  tt.config1.Nodes,
				Pods:                   tt.config1.Pods,
				Namespaces:             tt.config1.Namespaces,
				Services:               tt.config1.Services,
				DaemonSets:             tt.config1.DaemonSets,
				Deployments:            tt.config1.Deployments,
				StatefulSets:           tt.config1.StatefulSets,
				ReplicaSets:            tt.config1.ReplicaSets,
				PersistentVolumes:      tt.config1.PersistentVolumes,
				PersistentVolumeClaims: tt.config1.PersistentVolumeClaims,
				StorageClasses:         tt.config1.StorageClasses,
				Jobs:                   tt.config1.Jobs,
				PodDisruptionBudgets:   tt.config1.PodDisruptionBudgets,
				ReplicationControllers: tt.config1.ReplicationControllers,
				ResourceQuotas:         tt.config1.ResourceQuotas,
			}

			result.Append(tt.config2)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Append result mismatch.\nGot: %+v\nExpected: %+v", result, tt.expected)
			}
		})
	}
}

func TestKubernetesSnapshotConfig_AppendNilConfig(t *testing.T) {
	config := NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"})
	original := &KubernetesSnapshotConfig{
		Nodes:                  config.Nodes,
		Pods:                   config.Pods,
		Namespaces:             config.Namespaces,
		Services:               config.Services,
		DaemonSets:             config.DaemonSets,
		Deployments:            config.Deployments,
		StatefulSets:           config.StatefulSets,
		ReplicaSets:            config.ReplicaSets,
		PersistentVolumes:      config.PersistentVolumes,
		PersistentVolumeClaims: config.PersistentVolumeClaims,
		StorageClasses:         config.StorageClasses,
		Jobs:                   config.Jobs,
		PodDisruptionBudgets:   config.PodDisruptionBudgets,
		ReplicationControllers: config.ReplicationControllers,
		ResourceQuotas:         config.ResourceQuotas,
	}

	config.Append(nil)

	if !reflect.DeepEqual(config, original) {
		t.Error("Append with nil should not modify the original config")
	}
}

func TestSnapshotConfig_WithKubernetesSnapshotConfig(t *testing.T) {
	tests := []struct {
		name             string
		initialConfig    *SnapshotConfig
		kubernetesConfig *KubernetesSnapshotConfig
		expectedResult   func(*SnapshotConfig) bool
	}{
		{
			name: "nil kubernetes config",
			initialConfig: &SnapshotConfig{
				UseMetricsCache:        true,
				MinutelyMetricsEnabled: false,
				KubernetesSnapshot:     nil,
			},
			kubernetesConfig: NewKubernetesSnapshotConfigFromEnabled([]string{"nodes"}),
			expectedResult: func(sc *SnapshotConfig) bool {
				return sc.KubernetesSnapshot != nil && sc.KubernetesSnapshot.Nodes == true
			},
		},
		{
			name: "existing kubernetes config",
			initialConfig: &SnapshotConfig{
				UseMetricsCache:        true,
				MinutelyMetricsEnabled: false,
				KubernetesSnapshot:     NewKubernetesSnapshotConfigFromEnabled([]string{"pods"}),
			},
			kubernetesConfig: NewKubernetesSnapshotConfigFromEnabled([]string{"nodes"}),
			expectedResult: func(sc *SnapshotConfig) bool {
				return sc.KubernetesSnapshot != nil &&
					sc.KubernetesSnapshot.Nodes == true &&
					sc.KubernetesSnapshot.Pods == true
			},
		},
		{
			name: "overlapping configs",
			initialConfig: &SnapshotConfig{
				UseMetricsCache:        true,
				MinutelyMetricsEnabled: false,
				KubernetesSnapshot:     NewKubernetesSnapshotConfigFromEnabled([]string{"nodes", "pods"}),
			},
			kubernetesConfig: NewKubernetesSnapshotConfigFromEnabled([]string{"pods", "services"}),
			expectedResult: func(sc *SnapshotConfig) bool {
				return sc.KubernetesSnapshot != nil &&
					sc.KubernetesSnapshot.Nodes == true &&
					sc.KubernetesSnapshot.Pods == true &&
					sc.KubernetesSnapshot.Services == true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.initialConfig.WithKubernetesSnapshotConfig(tt.kubernetesConfig)

			// Should return the same instance
			if result != tt.initialConfig {
				t.Error("WithKubernetesSnapshotConfig should return the same instance")
			}

			if !tt.expectedResult(result) {
				t.Errorf("WithKubernetesSnapshotConfig result did not match expected behavior")
			}
		})
	}
}

func TestDefaultSnapshotConfig(t *testing.T) {
	config := DefaultSnapshotConfig()

	if config.UseMetricsCache != false {
		t.Error("DefaultSnapshotConfig should have UseMetricsCache set to false")
	}

	if config.MinutelyMetricsEnabled != false {
		t.Error("DefaultSnapshotConfig should have MinutelyMetricsEnabled set to false")
	}

	if config.KubernetesSnapshot == nil {
		t.Error("DefaultSnapshotConfig should have KubernetesSnapshot initialized")
	}

	// Check that all kubernetes resources are enabled by default
	if !config.KubernetesSnapshot.Nodes || !config.KubernetesSnapshot.Pods ||
		!config.KubernetesSnapshot.Namespaces || !config.KubernetesSnapshot.Services ||
		!config.KubernetesSnapshot.DaemonSets || !config.KubernetesSnapshot.Deployments ||
		!config.KubernetesSnapshot.StatefulSets || !config.KubernetesSnapshot.ReplicaSets ||
		!config.KubernetesSnapshot.PersistentVolumes || !config.KubernetesSnapshot.PersistentVolumeClaims ||
		!config.KubernetesSnapshot.StorageClasses || !config.KubernetesSnapshot.Jobs ||
		!config.KubernetesSnapshot.PodDisruptionBudgets || !config.KubernetesSnapshot.ReplicationControllers ||
		!config.KubernetesSnapshot.ResourceQuotas {
		t.Error("DefaultSnapshotConfig should have all kubernetes resources enabled")
	}
}

func TestNewSnapshotConfigFromEnv(t *testing.T) {
	// This test primarily verifies the constructor works without panicking
	// since it depends on environment variables
	config := NewSnapshotConfigFromEnv()

	if config == nil {
		t.Error("NewSnapshotConfigFromEnv should not return nil")
	}

	// The actual values depend on environment, but we can verify structure
	if config.KubernetesSnapshot != nil {
		t.Error("NewSnapshotConfigFromEnv should not initialize KubernetesSnapshot")
	}
}
