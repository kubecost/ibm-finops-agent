package kubecost

import "github.com/ibm/finops-agent/pkg/emitter"

// RequiredComponents implements emitter.ComponentRequirer. The OpenCost adapters are built from
// cluster info, the Kubernetes objects and the metrics; node stats aren't used, so a node-stats
// failure doesn't stop Kubecost.
func (ke *KubecostEmitter) RequiredComponents() []emitter.SnapshotComponent {
	return []emitter.SnapshotComponent{emitter.ComponentClusterInfo, emitter.ComponentKubernetes, emitter.ComponentMetrics}
}
