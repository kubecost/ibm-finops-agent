package cluster

import "k8s.io/apimachinery/pkg/version"

type Metadata interface {
	GetClusterInfo() *Info
}

type ClusterMetdata struct {
	info Info
}

func NewClusterMetadata(versionInfo *version.Info) ClusterMetdata {
	return ClusterMetdata{
		info: Info{
			Version: versionInfo,
		},
	}
}

func (cm ClusterMetdata) GetClusterInfo() *Info {
	return &cm.info
}

type Info struct {
	Version *version.Info
}