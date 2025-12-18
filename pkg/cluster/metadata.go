package cluster

import "k8s.io/apimachinery/pkg/version"

type Metadata interface {
	GetClusterInfo() *Info
}

type ClusterMetadata struct {
	info Info
}

func NewClusterMetadata(versionInfo *version.Info) ClusterMetadata {
	return ClusterMetadata{
		info: Info{
			Version: versionInfo,
		},
	}
}

func (cm ClusterMetadata) GetClusterInfo() *Info {
	return &cm.info
}

type Info struct {
	Version *version.Info
}
