package cluster

import (
	"context"
	"github.com/aws/smithy-go/ptr"
	"k8s.io/apimachinery/pkg/types"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute, false)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dccCtx, dccCancel := context.WithCancel(context.Background())
		dcc.Start(dccCtx.Done())

		deployments := dcc.GetAllDeployments()
		Expect(len(deployments)).Should(Equal(1))
		Expect(deployments[0].Name).Should(Equal(_testDeploymentName_))
		// keep sanitized data when false
		Expect(*deployments[0].Spec.ProgressDeadlineSeconds).Should(Equal(int32(5)))

		// ensure standard field removal occurs in Transform
		Expect(deployments[0].ManagedFields).Should(BeNil())
		annotations := deployments[0].GetAnnotations()
		Expect(annotations[KubernetesLastAppliedConfig]).To(BeEmpty())
		Expect(annotations["real-label"]).To(Equal("should_keep"))

		dccCancel()
		dcc.Shutdown()
	})

	It("can get auto sync with updated resources", func() {
		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute, true)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dccCtx, dccCancel := context.WithCancel(context.Background())
		dcc.Start(dccCtx.Done())

		deploy := &appsv1.Deployment{}
		Expect(cli.Get(ctx, types.NamespacedName{Namespace: _testNamespace_, Name: _testDeploymentName_}, deploy, &client.GetOptions{})).Should(Succeed())
		// verify api-server labels is 2 (no transform)
		Expect(len(deploy.GetAnnotations())).Should(BeEquivalentTo(2))
		updatedAnnotations := _testAnnotations_
		updatedAnnotations["key"] = "value"
		deploy.SetAnnotations(updatedAnnotations)
		// update back to api-server
		Expect(cli.Update(ctx, deploy, &client.UpdateOptions{})).Should(Succeed())

		// let it settle
		time.Sleep(time.Second)

		// verify the change is in cache
		deployments := dcc.GetAllDeployments()
		Expect(len(deployments)).Should(Equal(1))
		annotations := deployments[0].GetAnnotations()
		Expect(len(annotations)).Should(Equal(2))
		Expect(annotations["key"]).Should(Equal("value"))
		Expect(annotations[KubernetesLastAppliedConfig]).Should(BeEmpty())
		Expect(annotations["real-label"]).Should(Equal("should_keep"))
		// expect sanitized data to be removed when enabled
		Expect(deployments[0].Spec.ProgressDeadlineSeconds).Should(BeNil())

		dccCancel()
		dcc.Shutdown()
	})
	It("can correctly strip values from containers from Pod with parseMetric disabled", func() {
		// create an object for sync
		Expect(cli.Create(ctx, _testPod_.DeepCopy(), &client.CreateOptions{})).Should(Succeed())
		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute, false)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dccCtx, dccCancel := context.WithCancel(context.Background())
		dcc.Start(dccCtx.Done())

		pods := dcc.GetAllPods()
		Expect(len(pods)).Should(Equal(1))
		Expect(pods[0].Name).Should(Equal(_testPodName_))
		// keep sanitized data when false
		Expect(pods[0].Spec.Containers[0].Command).To(Not(BeEmpty()))

		// ensure standard field removal occurs in Transform
		Expect(pods[0].Spec.Containers[0].Env).To(BeEmpty())
		Expect(pods[0].ManagedFields).Should(BeNil())
		annotations := pods[0].GetAnnotations()
		Expect(annotations[KubernetesLastAppliedConfig]).To(BeEmpty())
		Expect(annotations["real-label"]).To(Equal("should_keep"))
		Expect(cli.Delete(ctx, _testPod_.DeepCopy(), &client.DeleteOptions{})).Should(Succeed())
		dccCancel()
		dcc.Shutdown()
	})
	It("can correctly strip values from containers from Pod with parseMetric enabled", func() {
		Expect(cli.Create(ctx, _testPod_.DeepCopy(), &client.CreateOptions{})).Should(Succeed())
		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute, true)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dccCtx, dccCancel := context.WithCancel(context.Background())
		dcc.Start(dccCtx.Done())

		pods := dcc.GetAllPods()
		Expect(len(pods)).Should(Equal(1))
		Expect(pods[0].Name).Should(Equal(_testPodName_))
		// remove sanitized data when true
		Expect(pods[0].Spec.Containers[0].Command).To(BeEmpty())

		// ensure standard field removal continues to occur in Transform
		Expect(pods[0].Spec.Containers[0].Env).To(BeEmpty())
		Expect(pods[0].ManagedFields).Should(BeNil())
		annotations := pods[0].GetAnnotations()
		Expect(annotations[KubernetesLastAppliedConfig]).To(BeEmpty())
		Expect(annotations["real-label"]).To(Equal("should_keep"))
		Expect(cli.Delete(ctx, _testPod_.DeepCopy(), &client.DeleteOptions{})).Should(Succeed())
		dccCancel()
		dcc.Shutdown()
	})
	It("can correctly strip values from containers & initContainers from Replicaset with parseMetric enabled", func() {
		Expect(cli.Create(ctx, _testReplicaSet_.DeepCopy(), &client.CreateOptions{})).Should(Succeed())
		dcc, err := NewDynamicClusterCache(cfg, 10*time.Minute, true)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dccCtx, dccCancel := context.WithCancel(context.Background())
		dcc.Start(dccCtx.Done())

		replicaSets := dcc.GetAllReplicaSets()
		Expect(len(replicaSets)).Should(Equal(1))
		Expect(replicaSets[0].Name).Should(Equal(_testReplicaSetName_))
		// remove sanitized data when true
		Expect(replicaSets[0].Spec.Template.Spec.Containers[0].Command).To(BeEmpty())
		Expect(replicaSets[0].Spec.Template.Spec.InitContainers[0].Command).To(BeEmpty())

		// ensure standard field removal continues to occur in Transform
		Expect(replicaSets[0].Spec.Template.Spec.Containers[0].Env).To(BeEmpty())
		Expect(replicaSets[0].Spec.Template.Spec.InitContainers[0].Env).To(BeEmpty())
		Expect(replicaSets[0].ManagedFields).Should(BeNil())
		annotations := replicaSets[0].GetAnnotations()
		Expect(annotations[KubernetesLastAppliedConfig]).To(BeEmpty())
		Expect(annotations["real-label"]).To(Equal("should_keep"))
		dccCancel()
		dcc.Shutdown()
	})
	It("can correctly append short lived pods to snapshot", func() {
		// create an object for sync
		Expect(cli.Create(ctx, _testPod_.DeepCopy(), &client.CreateOptions{})).Should(Succeed())
		dcc, err := NewDynamicClusterCache(cfg, 1*time.Second, false)
		Expect(err).ShouldNot(HaveOccurred())
		Expect(dcc).NotTo(BeNil())

		dccCtx, dccCancel := context.WithCancel(context.Background())
		dcc.Start(dccCtx.Done())
		Expect(cli.Create(ctx, shortLivedPod.DeepCopy(), &client.CreateOptions{})).Should(Succeed())
		Expect(cli.Delete(ctx, shortLivedPod.DeepCopy(), &client.DeleteOptions{})).Should(Succeed())
		// small buffer allowing DeleteFunc to execute
		time.Sleep(500 * time.Millisecond)

		pods := dcc.GetAllPods()
		shortLivedPods := dcc.GetAllShortLivedPods()
		// ensure slp is collected
		Expect(len(pods)).Should(Equal(1))
		Expect(len(shortLivedPods)).Should(Equal(1))
		Expect(pods[0].Name).Should(Equal(_testPodName_))
		Expect(shortLivedPods[0].Name).Should(Equal(testPodName2))

		shortLivedPods = dcc.GetAllShortLivedPods()
		// ensure slp was removed during previous call
		Expect(len(shortLivedPods)).Should(Equal(0))
		dccCancel()
		dcc.Shutdown()
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
	_testDeploymentContainerName2_     = "test-deploy-container2"
	_testDeploymentContainerImageName_ = "test-deploy-container-image"
	_testSelectorLabels_               = map[string]string{"key": "value"}
	_testAnnotations_                  = map[string]string{
		"kubectl.kubernetes.io/last-applied-configuration": "should_delete",
		"real-label": "should_keep",
	}
	_testProgressDeadlineSeconds = int32(5)
	_testDeployment_             = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      _testDeploymentName_,
			Namespace: _testNamespace_,
			ManagedFields: []metav1.ManagedFieldsEntry{
				{FieldsType: "test_field"},
			},
			Annotations: _testAnnotations_,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: _testSelectorLabels_,
			},
			ProgressDeadlineSeconds: &_testProgressDeadlineSeconds,

			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: _testSelectorLabels_,
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser: ptr.Int64(1000),
					},
					Containers: []corev1.Container{
						{
							Name:    _testDeploymentContainerName_,
							Image:   _testDeploymentContainerImageName_,
							Command: []string{"/fake/command"},
						},
						{
							Name:    _testDeploymentContainerName2_,
							Image:   _testDeploymentContainerImageName_,
							Command: []string{"/fake/command"},
							Env: []corev1.EnvVar{
								{Name: "key", Value: "value"},
								{Name: "key2", Value: "value2"},
							},
						},
					},
				},
			},
		},
	}
	_testReplicaSetName_               = "test-rs"
	_testReplicaSetContainerName_      = "test-rs-container"
	_testReplicaSetContainerName2_     = "test-rs-container2"
	_testReplicaSetContainerName3_     = "test-rs-container3"
	_testReplicaSetContainerName4_     = "test-rs-container4"
	_testReplicaSetContainerImageName_ = "test-rs-container-image"
	_testReplicaSet_                   = &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      _testReplicaSetName_,
			Namespace: _testNamespace_,
			ManagedFields: []metav1.ManagedFieldsEntry{
				{FieldsType: "test_field"},
			},
			Annotations: _testAnnotations_,
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: _testSelectorLabels_,
			},
			Replicas: ptr.Int32(1),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: _testSelectorLabels_,
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser: ptr.Int64(1000),
					},
					InitContainers: []corev1.Container{
						{
							Name:    _testReplicaSetContainerName3_,
							Image:   _testReplicaSetContainerImageName_,
							Command: []string{"/fake/command"},
							Env: []corev1.EnvVar{
								{Name: "key1", Value: "value"},
								{Name: "key2", Value: "value2"},
							},
						},
						{
							Name:    _testReplicaSetContainerName4_,
							Image:   _testReplicaSetContainerImageName_,
							Command: []string{"/fake/command"},
							Env: []corev1.EnvVar{
								{Name: "key3", Value: "value"},
								{Name: "key4", Value: "value2"},
							},
						},
					},
					Containers: []corev1.Container{
						{
							Name:    _testReplicaSetContainerName_,
							Image:   _testReplicaSetContainerImageName_,
							Command: []string{"/fake/command"},
							Env: []corev1.EnvVar{
								{Name: "key1", Value: "value"},
								{Name: "key2", Value: "value2"},
							},
						},
						{
							Name:    _testReplicaSetContainerName2_,
							Image:   _testReplicaSetContainerImageName_,
							Command: []string{"/fake/command"},
							Env: []corev1.EnvVar{
								{Name: "key3", Value: "value"},
								{Name: "key4", Value: "value2"},
							},
						},
					},
				},
			},
		},
	}
	_testPodName_               = "test-pod"
	testPodName2                = "short-lived-pod"
	_testPodContainerName_      = "test-pod-container"
	_testPodContainerName2_     = "test-pod-container2"
	_testPodContainerImageName_ = "test-pod-container-image"
	_testPod_                   = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        _testPodName_,
			Annotations: _testAnnotations_,
			Namespace:   _testNamespace_,
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: ptr.Int64(1000),
			},
			Containers: []corev1.Container{
				{
					Name:    _testPodContainerName_,
					Image:   _testPodContainerImageName_,
					Command: []string{"/fake/command"},
					Env: []corev1.EnvVar{
						{Name: "key3", Value: "value"},
						{Name: "key4", Value: "value2"},
					},
				},
				{
					Name:    _testPodContainerName2_,
					Image:   _testPodContainerImageName_,
					Command: []string{"/fake/command"},
					Env: []corev1.EnvVar{
						{Name: "key3", Value: "value"},
						{Name: "key4", Value: "value2"},
					},
				},
			},
		},
	}
	shortLivedPod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testPodName2,
			Annotations: _testAnnotations_,
			Namespace:   _testNamespace_,
		},
		Spec: corev1.PodSpec{
			SecurityContext: &corev1.PodSecurityContext{
				RunAsUser: ptr.Int64(1000),
			},
			Containers: []corev1.Container{
				{
					Name:    _testPodContainerName_,
					Image:   _testPodContainerImageName_,
					Command: []string{"/fake/command"},
					Env: []corev1.EnvVar{
						{Name: "key3", Value: "value"},
						{Name: "key4", Value: "value2"},
					},
				},
				{
					Name:    _testPodContainerName2_,
					Image:   _testPodContainerImageName_,
					Command: []string{"/fake/command"},
					Env: []corev1.EnvVar{
						{Name: "key3", Value: "value"},
						{Name: "key4", Value: "value2"},
					},
				},
			},
		},
	}
)
