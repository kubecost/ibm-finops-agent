package cluster

import (
	"log"
	"reflect"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	stv1 "k8s.io/api/storage/v1"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
)

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

func NewDynamicClusterCache(cfg *rest.Config, defaultResync time.Duration) (ClusterCache, error) {

	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	cache := DynamicClusterCache{
		DynamicSharedInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(client, defaultResync),
	}

	for _, gvr := range cacheResourceMap {
		cache.ForResource(gvr)
	}

	return &cache, nil
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
			log.Printf("failed to convert object. err: %s, obj: %v", err.Error(), obj)
			return nil
		}
		array = append(array, &obj)
	}

	return array
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
