package cluster

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	cfg     *rest.Config
	cli     client.Client
	testEnv *envtest.Environment
	ctx     context.Context
	cancel  context.CancelFunc
)

func TestUtils(t *testing.T) {
	RegisterFailHandler(Fail)

	RunSpecs(t, "Dynamic Cluster Cache Testing")
}

var _ = Describe("Cache in dynamic informer factory", func() {

	It("can be created, started with existing resources", func() {
		// create an object for sync
		Expect(cli.Create(ctx, _testDeployment_.DeepCopy(), &client.CreateOptions{})).Should(Succeed())

		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dcc.Run(context.Background())

		deployments := dcc.GetAllDeployments()
		Expect(len(deployments)).Should(Equal(1))
		Expect(deployments[0].Name).Should(Equal(_testDeploymentName_))
		dcc.Stop()
	})

	It("can get auto sync with updated resources", func() {
		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute)
		Expect(err).ShouldNot(HaveOccurred())

		Expect(dcc).NotTo(BeNil())
		dcc.Run(context.Background())

		deploy := &appsv1.Deployment{}
		Expect(cli.Get(ctx, types.NamespacedName{Namespace: _testNamespace_, Name: _testDeploymentName_}, deploy, &client.GetOptions{})).Should(Succeed())
		// verify current annotations is empty
		Expect(len(deploy.GetAnnotations())).Should(BeZero())
		key := "key"
		value := "value"
		deploy.SetAnnotations(map[string]string{key: value})
		// update back to api-server
		Expect(cli.Update(ctx, deploy, &client.UpdateOptions{})).Should(Succeed())

		// let it settle
		time.Sleep(time.Second)

		// verify the change is in cache
		deployments := dcc.GetAllDeployments()
		Expect(len(deployments)).Should(Equal(1))
		annotations := deployments[0].GetAnnotations()
		Expect(len(annotations)).Should(Equal(1))
		Expect(annotations[key]).Should(Equal(value))

		dcc.Stop()
	})

})

var _ = BeforeSuite(func() {
	By("bootstrapping test environment")

	testEnv = &envtest.Environment{}
	ctx, cancel = context.WithCancel(context.TODO())

	var err error
	cfg, err = testEnv.Start()
	Expect(err).ShouldNot(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	cli, err = client.New(cfg, client.Options{})
	Expect(err).ShouldNot(HaveOccurred())
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	err := testEnv.Stop()
	Expect(err).NotTo(HaveOccurred())
})

var (
	_testNamespace_ = "default"

	_testDeploymentName_               = "test-deploy"
	_testDeploymentContainerName_      = "test-deploy-container"
	_testDeploymentContainerImageName_ = "test-deploy-container-image"
	_testDeploySelectorLabels          = map[string]string{"key": "value"}
	_testDeployment_                   = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      _testDeploymentName_,
			Namespace: _testNamespace_,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: _testDeploySelectorLabels,
			},

			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: _testDeploySelectorLabels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  _testDeploymentContainerName_,
							Image: _testDeploymentContainerImageName_,
						},
					},
				},
			},
		},
	}
)
