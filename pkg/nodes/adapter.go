package nodes

import (
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// LegacyStatSummaryClient adapts the new StatSummaryClient (3-return) to the legacy
// 2-return interface expected by external packages like the opencost collector.
type LegacyStatSummaryClient struct {
	Client StatSummaryClient
}

// GetNodeData wraps the new 3-return interface, discarding the per-node results.
func (l *LegacyStatSummaryClient) GetNodeData() ([]*stats.Summary, error) {
	data, _, err := l.Client.GetNodeData()
	return data, err
}
