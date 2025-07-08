package version

import (
	"fmt"

	"k8s.io/apimachinery/pkg/version"
)

var (
	Version   = "dev"
	GitCommit = "HEAD"
)

func FriendlyVersion() string {
	return fmt.Sprintf("%s (%s)", Version, GitCommit)
}

func FormatVersionInfo(version *version.Info) string {
	if version != nil {
		return version.Major +"." +version.Minor
	}
	return ""
}

type Metadata interface {
	GetVersionInfo() *version.Info
}

type ClusterMetdata struct {
	versionInfo *version.Info
}

func NewClusterMetadata(versionInfo *version.Info) ClusterMetdata {
	return ClusterMetdata{
		versionInfo: versionInfo,
	}
}

func (cm ClusterMetdata) GetVersionInfo() *version.Info {
	return cm.versionInfo
}
