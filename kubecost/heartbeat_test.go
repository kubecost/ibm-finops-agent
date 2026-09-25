package kubecost

// I6: the Kubecost heartbeat carries the agent's health summary (docs/reliability/FINDINGS.md
// chunk 09), alongside OpenCost's cluster info and log level.

import (
	"testing"

	"github.com/opencost/opencost/core/pkg/clusters"
)

type staticMetadata map[string]any

func (s staticMetadata) GetMetadata() map[string]any { return s }

type staticClusterInfo map[string]string

func (s staticClusterInfo) GetClusterInfo() map[string]string { return s }

func TestHeartbeatMetadataIncludesAgentHealth(t *testing.T) {
	ke := NewKubecostEmitter(nil, nil, &EmitterConfig{HeartbeatMetadata: staticMetadata{"agent_health": "summary"}})
	md := ke.heartbeatMetadata(staticClusterInfo{clusters.ClusterInfoIdKey: "cluster-1"}).GetMetadata()
	if md["agent_health"] != "summary" {
		t.Errorf("heartbeat metadata %v has no agent_health", md)
	}
	if md[clusters.ClusterInfoIdKey] != "cluster-1" || md["logLevel"] == nil {
		t.Errorf("heartbeat metadata %v lost OpenCost's cluster info or log level", md)
	}

	plain := NewKubecostEmitter(nil, nil, &EmitterConfig{})
	if md := plain.heartbeatMetadata(staticClusterInfo{}).GetMetadata(); md["agent_health"] != nil {
		t.Errorf("heartbeat metadata %v without a provider", md)
	}
}
