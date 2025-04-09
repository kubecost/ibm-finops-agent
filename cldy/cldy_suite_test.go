package cldy

//nolint:errcheck
import (
	"testing"

	. "github.com/onsi/ginkgo/v2" // nolint:revive
	. "github.com/onsi/gomega"    // nolint:revive
)

func TestResourcesPackage(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "resources Package Suite")
}
