package cluster

import (
	"context"
	"fmt"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
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

func (dcc *DynamicClusterCache) Run(ctx context.Context) {

	dcc.ctx, dcc.cancel = context.WithCancel(ctx)

	dcc.Start(dcc.ctx.Done())

	synced := dcc.WaitForCacheSync(dcc.ctx.Done())
	for v, ok := range synced {
		if !ok {
			fmt.Fprintf(os.Stderr, "caches failed to sync: %v", v)
			return
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

func (dcc *DynamicClusterCache) GetAllDeployments() ([]*appsv1.Deployment, error) {
	objs := dcc.ListUnstructuredByGroupVersionResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})

	return ConvertUnstructuredArrayToTypedArray(objs, appsv1.Deployment{})
}

// Note: t is the actual struct not pointer
func ConvertUnstructuredArrayToTypedArray[T any](uObjs []*unstructured.Unstructured, t T) ([]*T, error) {

	if uObjs == nil {
		return nil, nil
	}

	var array []*T
	for _, o := range uObjs {
		var obj T
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.Object, &obj)
		if err != nil {
			return nil, err
		}
		array = append(array, &obj)
	}

	return array, nil
}

func NewDynamicClusterCache(client dynamic.Interface, defaultResync time.Duration) *DynamicClusterCache {

	if client == nil {
		return nil
	}

	cache := DynamicClusterCache{
		DynamicSharedInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(client, defaultResync),
	}

	for _, gvr := range cacheResources {
		cache.ForResource(gvr)
	}

	return &cache
}
