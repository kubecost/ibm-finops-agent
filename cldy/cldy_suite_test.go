package cldy

//nolint:errcheck
import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2" // nolint:revive
	. "github.com/onsi/gomega"    // nolint:revive
)

func TestResourcesPackage(t *testing.T) {
	// Keep retry backoff short so failing requests don't slow the suite.
	retryBackoff = func(int) time.Duration { return time.Millisecond }
	RegisterFailHandler(Fail)
	RunSpecs(t, "resources Package Suite")
}
