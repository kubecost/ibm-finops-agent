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

func FormatGitVersion(version *version.Info) string {
	if version != nil {
		return version.Major +"." +version.Minor
	}
	return ""
}