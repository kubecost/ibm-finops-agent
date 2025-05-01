package cluster

import (
	"github.com/spf13/viper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"

	"github.com/opencost/opencost/core/pkg/log"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
)

const (
	KubernetesLastAppliedConfig = "kubectl.kubernetes.io/last-applied-configuration"
)

type InfromerConfig struct {
	ResyncInterval time.Duration
	SanitizeData   bool
}

// LoadInformerConfig returns configs related to informer settings
func LoadInformerConfig() InfromerConfig {
	return InfromerConfig{
		ResyncInterval: time.Duration(viper.GetInt("INFORMER_RESYNC_INTERVAL")) * time.Hour,
		SanitizeData:   viper.GetBool("PARSE_METRICS_DATA"),
	}
}

// TODO: Cloudy/Turbo needs filtering functionality for specific k8s resources!
var (
	cacheResourceMap = map[reflect.Type]schema.GroupVersionResource{
		reflect.TypeOf(corev1.Namespace{}):             {Version: "v1", Resource: "namespaces"},
		reflect.TypeOf(corev1.Node{}):                  {Version: "v1", Resource: "nodes"},
		reflect.TypeOf(corev1.Pod{}):                   {Version: "v1", Resource: "pods"},
		reflect.TypeOf(corev1.Service{}):               {Version: "v1", Resource: "services"},
		reflect.TypeOf(corev1.PersistentVolume{}):      {Version: "v1", Resource: "persistentvolumes"},
		reflect.TypeOf(corev1.PersistentVolumeClaim{}): {Version: "v1", Resource: "persistentvolumeclaims"},
		reflect.TypeOf(corev1.ReplicationController{}): {Version: "v1", Resource: "replicationcontrollers"},
		reflect.TypeOf(appsv1.Deployment{}):            {Group: "apps", Version: "v1", Resource: "deployments"},
		reflect.TypeOf(appsv1.DaemonSet{}):             {Group: "apps", Version: "v1", Resource: "daemonsets"},
		reflect.TypeOf(appsv1.StatefulSet{}):           {Group: "apps", Version: "v1", Resource: "statefulsets"},
		reflect.TypeOf(appsv1.ReplicaSet{}):            {Group: "apps", Version: "v1", Resource: "replicasets"},
		reflect.TypeOf(stv1.StorageClass{}):            {Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},
		reflect.TypeOf(batchv1.Job{}):                  {Group: "batch", Version: "v1", Resource: "jobs"},
		reflect.TypeOf(policyv1.PodDisruptionBudget{}): {Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
	}
)

// DynamicClusterCache is the implementation of ClusterCache with dynamic informers
type DynamicClusterCache struct {
	dynamicinformer.DynamicSharedInformerFactory
}

func NewDynamicClusterCache(cfg *rest.Config, defaultResync time.Duration, sanitizeData bool) (ClusterCache, error) {
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	cache := DynamicClusterCache{
		DynamicSharedInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(client, defaultResync),
	}

	for _, gvr := range cacheResourceMap {
		transformErr := cache.ForResource(gvr).Informer().SetTransform(GetTransformFunc(sanitizeData))
		if transformErr != nil {
			return nil, transformErr
		}
	}

	return &cache, nil
}

// GetTransformFunc returns the correct transform to apply based on parseMetricsData flag
// when enabled, sensitive information from k8s resources will be stripped
func GetTransformFunc(parseMetricsData bool) func(resource interface{}) (interface{}, error) {
	return func(resource interface{}) (interface{}, error) {
		unTyped, ok := resource.(*unstructured.Unstructured)
		if !ok {
			log.Debugf("resource found that is not unstructured, skipping sanitization")
			return resource, nil
		}
		k8Obj := ConvertToKubernetesResource(unTyped)

		if parseMetricsData {
			resource = sanitizeData(k8Obj)
		}
		resource = trimData(k8Obj)

		unstructuredResource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(resource)
		if err != nil {
			return nil, err
		}

		return &unstructured.Unstructured{Object: unstructuredResource}, nil
	}
}

// Note: t is the actual struct not pointer
func ConvertUnstructuredArrayToTypedArray[T any](uObjs []*unstructured.Unstructured) []*T {

	if uObjs == nil {
		return nil
	}

	var array []*T
	for _, o := range uObjs {
		var obj T
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &obj)
		if err != nil {
			log.Warnf("failed to convert object. err: %s, obj: %v", err.Error(), obj)
			return nil
		}
		array = append(array, &obj)
	}

	return array
}

func ConvertToKubernetesResource(resource *unstructured.Unstructured) interface{} {
	switch resource.GetKind() {
	case "Deployment":
		return ConvertUnstructuredToTyped[appsv1.Deployment](resource)
	case "Pod":
		return ConvertUnstructuredToTyped[corev1.Pod](resource)
	case "Service":
		return ConvertUnstructuredToTyped[corev1.Service](resource)
	case "ConfigMap":
		return ConvertUnstructuredToTyped[corev1.ConfigMap](resource)
	case "PersistentVolume":
		return ConvertUnstructuredToTyped[corev1.PersistentVolume](resource)
	case "PersistentVolumeClaim":
		return ConvertUnstructuredToTyped[corev1.PersistentVolumeClaim](resource)
	case "ReplicationController":
		return ConvertUnstructuredToTyped[corev1.ReplicationController](resource)
	case "ReplicaSet":
		return ConvertUnstructuredToTyped[appsv1.ReplicaSet](resource)
	case "StatefulSet":
		return ConvertUnstructuredToTyped[appsv1.StatefulSet](resource)
	case "DaemonSet":
		return ConvertUnstructuredToTyped[appsv1.DaemonSet](resource)
	case "Job":
		return ConvertUnstructuredToTyped[batchv1.Job](resource)
	case "CronJob":
		return ConvertUnstructuredToTyped[batchv1.CronJob](resource)
	case "Namespace":
		return ConvertUnstructuredToTyped[corev1.Namespace](resource)
	case "Node":
		return ConvertUnstructuredToTyped[corev1.Node](resource)
	default:
		log.Warnf("unknown resource added to infromer, not sanitizing Kind: %s", resource.GetKind())
	}
	return resource
}

func ConvertUnstructuredToTyped[T any](uObj *unstructured.Unstructured) *T {
	var obj T
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(uObj.Object, &obj)
	if err != nil {
		log.Warnf("failed to convert object. err: %s, obj: %v", err.Error(), obj)
		return nil
	}
	return &obj
}

func (dcc *DynamicClusterCache) Start(stopCh <-chan struct{}) {

	dcc.DynamicSharedInformerFactory.Start(stopCh)

	synced := dcc.WaitForCacheSync(stopCh)
	for v, ok := range synced {
		if !ok {
			log.Fatalf("caches failed to sync: %v", v)
		}
	}
}

func (dcc *DynamicClusterCache) ListUnstructuredByGroupVersionResource(gvr schema.GroupVersionResource) []*unstructured.Unstructured {
	list := dcc.ForResource(gvr).Informer().GetIndexer().List()

	var objs []*unstructured.Unstructured

	for _, o := range list {
		objs = append(objs, o.(*unstructured.Unstructured).DeepCopy())
	}

	return objs
}

func (dcc *DynamicClusterCache) GetAllNamespaces() []*corev1.Namespace {

	return ConvertUnstructuredArrayToTypedArray[corev1.Namespace](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.Namespace{})]))
}

func (dcc *DynamicClusterCache) GetAllNodes() []*corev1.Node {

	return ConvertUnstructuredArrayToTypedArray[corev1.Node](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.Node{})]))
}

func (dcc *DynamicClusterCache) GetAllPods() []*corev1.Pod {

	return ConvertUnstructuredArrayToTypedArray[corev1.Pod](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.Pod{})]))
}

func (dcc *DynamicClusterCache) GetAllServices() []*corev1.Service {

	return ConvertUnstructuredArrayToTypedArray[corev1.Service](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.Service{})]))
}

func (dcc *DynamicClusterCache) GetAllPersistentVolumes() []*corev1.PersistentVolume {

	return ConvertUnstructuredArrayToTypedArray[corev1.PersistentVolume](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.PersistentVolume{})]))
}

func (dcc *DynamicClusterCache) GetAllPersistentVolumeClaims() []*corev1.PersistentVolumeClaim {

	return ConvertUnstructuredArrayToTypedArray[corev1.PersistentVolumeClaim](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.PersistentVolumeClaim{})]))
}

func (dcc *DynamicClusterCache) GetAllDeployments() []*appsv1.Deployment {

	return ConvertUnstructuredArrayToTypedArray[appsv1.Deployment](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(appsv1.Deployment{})]))
}

func (dcc *DynamicClusterCache) GetAllDaemonSets() []*appsv1.DaemonSet {

	return ConvertUnstructuredArrayToTypedArray[appsv1.DaemonSet](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(appsv1.DaemonSet{})]))
}

func (dcc *DynamicClusterCache) GetAllStatefulSets() []*appsv1.StatefulSet {

	return ConvertUnstructuredArrayToTypedArray[appsv1.StatefulSet](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(appsv1.StatefulSet{})]))
}

func (dcc *DynamicClusterCache) GetAllReplicaSets() []*appsv1.ReplicaSet {

	return ConvertUnstructuredArrayToTypedArray[appsv1.ReplicaSet](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(appsv1.ReplicaSet{})]))
}

func (dcc *DynamicClusterCache) GetAllStorageClasses() []*stv1.StorageClass {

	return ConvertUnstructuredArrayToTypedArray[stv1.StorageClass](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(stv1.StorageClass{})]))
}

func (dcc *DynamicClusterCache) GetAllJobs() []*batchv1.Job {

	return ConvertUnstructuredArrayToTypedArray[batchv1.Job](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(batchv1.Job{})]))
}

func (dcc *DynamicClusterCache) GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget {

	return ConvertUnstructuredArrayToTypedArray[policyv1.PodDisruptionBudget](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(policyv1.PodDisruptionBudget{})]))
}

func (dcc *DynamicClusterCache) GetAllReplicationControllers() []*corev1.ReplicationController {

	return ConvertUnstructuredArrayToTypedArray[corev1.ReplicationController](dcc.ListUnstructuredByGroupVersionResource(cacheResourceMap[reflect.TypeOf(corev1.ReplicationController{})]))
}

// sanitizeData removes information from kubernetes resources for customer security purposes
// nolint:gocyclo
func sanitizeData(to interface{}) interface{} {
	switch to.(type) {
	case *corev1.Pod:
		return sanitizePod(to)
	case *appsv1.DaemonSet:
		cast := to.(*appsv1.DaemonSet)
		cast.Spec.Template = corev1.PodTemplateSpec{}
		cast.Spec.RevisionHistoryLimit = nil
		cast.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{}
		cast.Spec.MinReadySeconds = 0
		cast.Spec.RevisionHistoryLimit = nil
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *appsv1.ReplicaSet:
		cast := to.(*appsv1.ReplicaSet)
		cast.Spec.Replicas = nil
		cast.Spec.Template = corev1.PodTemplateSpec{}
		cast.Spec.MinReadySeconds = 0
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *appsv1.Deployment:
		cast := to.(*appsv1.Deployment)
		cast.Spec.Template = corev1.PodTemplateSpec{}
		cast.Spec.Replicas = nil
		cast.Spec.Strategy = appsv1.DeploymentStrategy{}
		cast.Spec.MinReadySeconds = 0
		cast.Spec.RevisionHistoryLimit = nil
		cast.Spec.ProgressDeadlineSeconds = nil
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *batchv1.Job:
		cast := to.(*batchv1.Job)
		cast.Spec.Template = corev1.PodTemplateSpec{}
		cast.Spec.Parallelism = nil
		cast.Spec.Completions = nil
		cast.Spec.ActiveDeadlineSeconds = nil
		cast.Spec.BackoffLimit = nil
		cast.Spec.ManualSelector = nil
		cast.Spec.TTLSecondsAfterFinished = nil
		cast.Spec.CompletionMode = nil
		cast.Spec.Suspend = nil
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *batchv1.CronJob:
		cast := to.(*batchv1.CronJob)
		// cronjobs have no Selector
		cast.Spec = batchv1.CronJobSpec{}
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *corev1.Service:
		cast := to.(*corev1.Service)
		cast.Spec.Ports = nil
		cast.Spec.ClusterIP = ""
		cast.Spec.ClusterIPs = nil
		cast.Spec.Type = ""
		cast.Spec.ExternalIPs = nil
		cast.Spec.SessionAffinity = ""
		cast.Spec.LoadBalancerIP = ""
		cast.Spec.LoadBalancerSourceRanges = nil
		cast.Spec.ExternalName = ""
		cast.Spec.ExternalTrafficPolicy = ""
		cast.Spec.HealthCheckNodePort = 0
		cast.Spec.SessionAffinityConfig = nil
		cast.Spec.IPFamilies = nil
		cast.Spec.IPFamilyPolicy = nil
		cast.Spec.AllocateLoadBalancerNodePorts = nil
		cast.Spec.LoadBalancerClass = nil
		cast.Spec.InternalTrafficPolicy = nil
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *corev1.ReplicationController:
		cast := to.(*corev1.ReplicationController)
		cast.Spec.Replicas = nil
		cast.Spec.Template = nil
		cast.Spec.MinReadySeconds = 0
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *corev1.PersistentVolume:
		cast := to.(*corev1.PersistentVolume)
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *corev1.PersistentVolumeClaim:
		cast := to.(*corev1.PersistentVolumeClaim)
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	case *corev1.Node:
		cast := to.(*corev1.Node)
		sanitizeMeta(&cast.ObjectMeta)
		return cast
	}
	return to
}

// trimData removes unneeded kubernetes resource fields
// nolint:gocyclo
func trimData(to interface{}) interface{} {
	switch to.(type) {
	case *corev1.Pod:
		return trimPod(to)
	case *appsv1.DaemonSet:
		cast := to.(*appsv1.DaemonSet)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *appsv1.ReplicaSet:
		cast := to.(*appsv1.ReplicaSet)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *appsv1.Deployment:
		cast := to.(*appsv1.Deployment)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *batchv1.Job:
		cast := to.(*batchv1.Job)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *batchv1.CronJob:
		cast := to.(*batchv1.CronJob)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *corev1.Service:
		cast := to.(*corev1.Service)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *corev1.ReplicationController:
		cast := to.(*corev1.ReplicationController)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *corev1.Namespace:
		return trimNamespace(to)
	case *corev1.PersistentVolume:
		cast := to.(*corev1.PersistentVolume)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *corev1.PersistentVolumeClaim:
		cast := to.(*corev1.PersistentVolumeClaim)
		trimMeta(&cast.ObjectMeta)
		return cast
	case *corev1.Node:
		cast := to.(*corev1.Node)
		trimMeta(&cast.ObjectMeta)
		return cast
	}
	return to
}

func sanitizeMeta(objectMeta *metav1.ObjectMeta) {
	objectMeta.Finalizers = nil
}

func trimMeta(objectMeta *metav1.ObjectMeta) {
	objectMeta.ManagedFields = nil
	delete(objectMeta.Annotations, KubernetesLastAppliedConfig)
}

func sanitizePod(to interface{}) interface{} {
	cast := to.(*corev1.Pod)
	for j, container := range (*cast).Spec.Containers {
		(*cast).Spec.Containers[j] = sanitizeContainer(container)
	}
	for j, container := range (*cast).Spec.InitContainers {
		(*cast).Spec.InitContainers[j] = sanitizeContainer(container)
	}
	return cast
}

func trimPod(to interface{}) interface{} {
	cast := to.(*corev1.Pod)
	// removing env var and related data from the object
	(*cast).ObjectMeta.ManagedFields = nil
	delete((*cast).ObjectMeta.Annotations, KubernetesLastAppliedConfig)

	for j, container := range (*cast).Spec.Containers {
		(*cast).Spec.Containers[j] = trimContainer(container)
	}
	for j, container := range (*cast).Spec.InitContainers {
		(*cast).Spec.InitContainers[j] = trimContainer(container)
	}
	return cast
}

func sanitizeContainer(container corev1.Container) corev1.Container {
	container.Command = nil
	container.Args = nil
	container.ImagePullPolicy = ""
	container.LivenessProbe = nil
	container.StartupProbe = nil
	container.ReadinessProbe = nil
	container.TerminationMessagePath = ""
	container.TerminationMessagePolicy = ""
	container.SecurityContext = nil
	return container
}

func trimContainer(container corev1.Container) corev1.Container {
	container.Env = nil
	return container
}

func trimNamespace(to interface{}) interface{} {
	cast := to.(*corev1.Namespace)
	(*cast).ObjectMeta.ManagedFields = nil
	return cast
}
