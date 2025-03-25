package cluster

import (
	"context"
	"log"
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
	cacheResources = []schema.GroupVersionResource{
		{Version: "v1", Resource: "namespaces"},
		{Version: "v1", Resource: "nodes"},
		{Version: "v1", Resource: "pods"},
		{Version: "v1", Resource: "services"},
		{Version: "v1", Resource: "persistentvolumes"},
		{Version: "v1", Resource: "persistentvolumeclaims"},
		{Version: "v1", Resource: "replicationcontrollers"},

		{Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Resource: "daemonsets"},
		{Group: "apps", Version: "v1", Resource: "daemonsets"},
		{Group: "apps", Version: "v1", Resource: "replicasets"},

		{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"},

		{Group: "batch", Version: "v1", Resource: "jobs"},

		{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"},
	}
)

// DynamicClusterCache is the implementation of ClusterCache with dynamic informers
type DynamicClusterCache struct {
	dynamicinformer.DynamicSharedInformerFactory
	ctx    context.Context
	cancel context.CancelFunc
}

func NewDynamicClusterCache(cfg *rest.Config, defaultResync time.Duration) (ClusterCache, error) {

	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	cache := DynamicClusterCache{
		DynamicSharedInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(client, defaultResync),
	}

	for _, gvr := range cacheResources {
		cache.ForResource(gvr)
	}

	return &cache, nil
}

// Note: t is the actual struct not pointer
func ConvertUnstructuredArrayToTypedArray[T any](uObjs []*unstructured.Unstructured, t T) []*T {

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

func (dcc *DynamicClusterCache) Run(ctx context.Context) {

	dcc.ctx, dcc.cancel = context.WithCancel(ctx)

	dcc.Start(dcc.ctx.Done())

	synced := dcc.WaitForCacheSync(dcc.ctx.Done())
	for v, ok := range synced {
		if !ok {
			log.Fatalf("caches failed to sync: %v", v)
		}
	}
}

func (dcc *DynamicClusterCache) Stop() {
	dcc.cancel()

	dcc.Shutdown()
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
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "namespaces"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.Namespace{})
}

func (dcc *DynamicClusterCache) GetAllNodes() []*corev1.Node {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "nodes"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.Node{})
}

func (dcc *DynamicClusterCache) GetAllPods() []*corev1.Pod {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "pods"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.Pod{})
}

func (dcc *DynamicClusterCache) GetAllServices() []*corev1.Service {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "services"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.Service{})
}

func (dcc *DynamicClusterCache) GetAllPersistentVolumes() []*corev1.PersistentVolume {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.PersistentVolume{})
}

func (dcc *DynamicClusterCache) GetAllPersistentVolumeClaims() []*corev1.PersistentVolumeClaim {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.PersistentVolumeClaim{})
}

func (dcc *DynamicClusterCache) GetAllDeployments() []*appsv1.Deployment {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})

	return ConvertUnstructuredArrayToTypedArray(objs, appsv1.Deployment{})
}

func (dcc *DynamicClusterCache) GetAllDaemonSets() []*appsv1.DaemonSet {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"})

	return ConvertUnstructuredArrayToTypedArray(objs, appsv1.DaemonSet{})
}

func (dcc *DynamicClusterCache) GetAllStatefulSets() []*appsv1.StatefulSet {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"})

	return ConvertUnstructuredArrayToTypedArray(objs, appsv1.StatefulSet{})
}

func (dcc *DynamicClusterCache) GetAllReplicaSets() []*appsv1.ReplicaSet {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"})

	return ConvertUnstructuredArrayToTypedArray(objs, appsv1.ReplicaSet{})
}

func (dcc *DynamicClusterCache) GetAllStorageClasses() []*stv1.StorageClass {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "storageclass"})

	return ConvertUnstructuredArrayToTypedArray(objs, stv1.StorageClass{})
}

func (dcc *DynamicClusterCache) GetAllJobs() []*batchv1.Job {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"})

	return ConvertUnstructuredArrayToTypedArray(objs, batchv1.Job{})
}

func (dcc *DynamicClusterCache) GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"})

	return ConvertUnstructuredArrayToTypedArray(objs, policyv1.PodDisruptionBudget{})
}

func (dcc *DynamicClusterCache) GetAllReplicationControllers() []*corev1.ReplicationController {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Version: "v1", Resource: "replicationcontrollers"})

	return ConvertUnstructuredArrayToTypedArray(objs, corev1.ReplicationController{})
}
