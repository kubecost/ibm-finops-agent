//go:build reliability_repro

package cluster

// F-12 reproduction, run inside the envtest suite in dynamic_test.go.

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var _ = Describe("Informer sync (F-12)", func() {
	It("reports within a bounded time when an informer can never sync", func() {
		// A user with no RBAC bindings: every list the reflectors make is forbidden, as with a
		// missing ClusterRole verb in production.
		user, err := testEnv.AddUser(envtest.User{Name: "f12-no-rbac"}, nil)
		Expect(err).ShouldNot(HaveOccurred())

		dcc, err := NewDynamicClusterCache(user.Config(), 10*time.Minute, false, time.Minute)
		Expect(err).ShouldNot(HaveOccurred())

		returned := make(chan struct{})
		go func() {
			defer close(returned)
			// Exactly what pkg/core/datasource.go passes: a nil stop channel.
			dcc.Start(context.Background().Done())
		}()

		select {
		case <-returned:
			// Start returned: startup can report the unsynced informers.
		case <-time.After(10 * time.Second):
			// The goroutine and its reflectors leak until the suite's API server stops; that
			// leak is the finding.
			Fail("F-12: informer cache Start still blocked after 10s on a forbidden list with no report; startup hangs forever while /healthz stays 200")
		}
	})
})
