package cluster

// F-12 (docs/reliability/FINDINGS.md), run inside the envtest suite in dynamic_test.go. This
// was a reliability_repro reproduction; chunk 08 fixed it according to D9: startup waits a
// bounded time, reports the unsynced informers by resource, and they keep retrying.

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

var _ = Describe("Informer sync (F-12)", func() {
	It("reports within a bounded time when an informer can never sync, and syncs once RBAC is granted", func() {
		// A user with no RBAC bindings: every list the reflectors make is forbidden, as with a
		// missing ClusterRole verb in production.
		const userName = "f12-no-rbac"
		user, err := testEnv.AddUser(envtest.User{Name: userName}, nil)
		Expect(err).ShouldNot(HaveOccurred())

		cc, err := NewDynamicClusterCache(user.Config(), 10*time.Minute, false, time.Minute)
		Expect(err).ShouldNot(HaveOccurred())
		sr, ok := cc.(SyncReporter)
		Expect(ok).To(BeTrue(), "the dynamic cluster cache must report its sync state")

		informerCtx, stopInformers := context.WithCancel(context.Background())
		defer stopInformers()

		returned := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(returned)
			unsynced := sr.StartWithTimeout(informerCtx, 2*time.Second)
			Expect(FormatResources(unsynced)).To(ContainSubstring("pods"))
			Expect(unsynced).To(HaveLen(len(cacheResourceMap)))
		}()
		Eventually(returned).WithTimeout(10*time.Second).Should(BeClosed(),
			"F-12: informer cache start still blocked after 10s on a forbidden list with no report")

		// Readiness names the unsynced resources; liveness is unaffected (D9).
		report := SyncComponent(sr).HealthCheck(context.Background(), time.Now())
		Expect(report.Live).To(BeTrue())
		Expect(report.Conditions).To(HaveLen(1))
		Expect(report.Conditions[0].Type).To(Equal(ConditionInformersUnsynced))
		Expect(report.Conditions[0].Message).To(ContainSubstring("apps/deployments"))

		// Granting the permission lets the informers, which kept retrying, sync with no restart.
		binding := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "f12-grant"},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster-admin"},
			Subjects:   []rbacv1.Subject{{APIGroup: "rbac.authorization.k8s.io", Kind: "User", Name: userName}},
		}
		Expect(cli.Create(ctx, binding)).Should(Succeed())
		defer func() { _ = cli.Delete(ctx, binding, &client.DeleteOptions{}) }()

		Eventually(sr.UnsyncedResources).WithTimeout(90 * time.Second).WithPolling(500 * time.Millisecond).Should(BeEmpty())
		Expect(SyncComponent(sr).HealthCheck(context.Background(), time.Now()).Conditions).To(BeEmpty())
	})
})
