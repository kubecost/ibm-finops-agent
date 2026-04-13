package cluster

import (
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	cache2 "k8s.io/client-go/tools/cache"

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

type InformerConfig struct {
	ResyncInterval time.Duration
	SanitizeData   bool
}

// LoadInformerConfig returns configs related to informer settings
func LoadInformerConfig() InformerConfig {
	return InformerConfig{
		ResyncInterval: env.GetInformerReSyncInterval(),
		SanitizeData:   env.GetSanitizeData(),
	}
}

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
		reflect.TypeOf(corev1.ResourceQuota{}):         {Version: "v1", Resource: "resourcequotas"},
	}
	// fields to trim on specific resources if parseMetricsData is enabled
	gvkToSanitizePaths = map[schema.GroupVersionKind][]string{
		{Group: "apps", Version: "v1", Kind: "Deployment"}: {
			"spec.progressDeadlineSeconds",
		},
		{Group: "apps", Version: "v1", Kind: "Daemonset"}: {
			"spec.updateStrategy",
		},
		{Group: "batch", Version: "v1", Kind: "Job"}: {
			"spec.parallelism",
			"spec.completions",
			"spec.activeDeadlineSeconds",
			"spec.backoffLimit",
			"spec.manualSelector",
			"spec.ttlSecondsAfterFinished",
			"spec.completionMode",
			"spec.suspend",
		},
		{Group: "batch", Version: "v1", Kind: "Cronjob"}: {
			"spec",
		},
		// custom gvk based on common nested k8s resources
		{Version: "v1", Kind: "Container"}: {
			"command",
			"args",
			"imagePullPolicy",
			"livenessProbe",
			"readinessProbe",
			"startupProbe",
			"terminationMessagePath",
			"terminationMessagePolicy",
			"securityContext",
		},
		{Version: "v1", Kind: "Service"}: {
			"spec.ports",
			"spec.clusterIPs",
			"spec.externalIPs",
			"spec.sessionAffinity",
			"spec.loadBalancerIP",
			"spec.loadBalancerSourceRanges",
			"spec.externalName",
			"spec.externalTrafficPolicy",
			"spec.healthCheckNodePort",
			"spec.sessionAffinityConfig",
			"spec.ipFamilies",
			"spec.ipFamilyPolicy",
			"spec.allocatedLoadBalancerNodePorts",
			"spec.loadBalancerClass",
			"spec.internalTrafficPolicy",
		},
	}
	// common fields to trim on all resources if parseMetricsData is enabled
	commonSanitizePaths = []string{
		"spec.revisionHistoryLimit",
		"spec.minReadySeconds",
		"metadata.finalizers",
	}
	// fields to trim on specific resources by default
	gvkToTrimPaths = map[schema.GroupVersionKind][]string{
		{Version: "v1", Kind: "Container"}: {
			"env",
		},
	}
	// common fields to trim on all resources by default
	commonTrimPaths = []string{"metadata.managedFields"}

	containerGVK = schema.GroupVersionKind{Version: "v1", Kind: "Container"}
)

// DynamicClusterCache is the implementation of ClusterCache with dynamic informers
type DynamicClusterCache struct {
	dynamicinformer.DynamicSharedInformerFactory
	shortLivedPods []*corev1.Pod
	slpMux         sync.RWMutex
	slpDuration    time.Duration
}

func NewDynamicClusterCache(
	cfg *rest.Config,
	defaultResync time.Duration,
	sanitizeData bool,
	slpDuration time.Duration,
) (ClusterCache, error) {
	client, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}

	cache := DynamicClusterCache{
		DynamicSharedInformerFactory: dynamicinformer.NewDynamicSharedInformerFactory(client, defaultResync),
		slpDuration:                  slpDuration,
	}

	for _, gvr := range cacheResourceMap {
		transformErr := cache.ForResource(gvr).Informer().SetTransform(GetTransformFunc(sanitizeData))
		if transformErr != nil {
			return nil, transformErr
		}
	}

	cache.shortLivedPods = []*corev1.Pod{}
	// add delete event on pods informer to track short-lived pods
	_, eventErr := cache.ForResource(cacheResourceMap[reflect.TypeOf(corev1.Pod{})]).Informer().
		AddEventHandler(cache2.ResourceEventHandlerFuncs{
			DeleteFunc: cache.captureShortLivedPodFunc(),
		})
	if eventErr != nil {
		return nil, eventErr
	}

	return &cache, nil
}

func (dcc *DynamicClusterCache) captureShortLivedPodFunc() func(pod interface{}) {
	return func(pod interface{}) {
		unstructuredPod, ok := pod.(*unstructured.Unstructured)
		if !ok {
			log.Warnf("failed to cast interface to unstructured, not capturing delete event")
			return
		}
		var castedPod corev1.Pod
		err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredPod.Object, &castedPod)
		if err != nil {
			log.Warnf("failed to unstructure object. not capturing delete event. err: %s", err.Error())
			return
		}
		// only capture deleted pods if they have a short lifespan
		if castedPod.Status.StartTime == nil ||
			castedPod.Status.StartTime.After(time.Now().Add(-dcc.slpDuration)) {
			dcc.addShortLivedPod(&castedPod)
		}
	}
}

func (dcc *DynamicClusterCache) addShortLivedPod(pod *corev1.Pod) {
	dcc.slpMux.Lock()
	dcc.shortLivedPods = append(dcc.shortLivedPods, pod)
	dcc.slpMux.Unlock()
}

// GetTransformFunc returns the correct transform to apply based on parseMetricsData flag
// when enabled, sensitive information from k8s resources will be stripped
func GetTransformFunc(parseMetricsData bool) func(resource interface{}) (interface{}, error) {
	return func(resource interface{}) (interface{}, error) {
		var casted *unstructured.Unstructured
		var ok bool
		if casted, ok = resource.(*unstructured.Unstructured); !ok {
			log.Warnf("Not trimming or sanitizing non-unstructured resource: %s", reflect.TypeOf(resource))
			return resource, nil
		}
		_ = cleanResource(casted, parseMetricsData)
		return resource, nil
	}
}

func cleanResource(resource *unstructured.Unstructured, parseMetricsData bool) *unstructured.Unstructured {
	gvk := resource.GetObjectKind().GroupVersionKind()
	// for resources with containers separate container cleaning needs to be done
	if gvk.Group == "apps" || gvk.Kind == "Pod" || gvk.Kind == "Job" {
		cleanContainers(resource, gvk, parseMetricsData)
	}
	// remove fields (if any) that are specific to the resource
	cleanResourceFieldsFromPath(resource, gvkToTrimPaths[gvk])
	// remove common fields for all resources
	cleanResourceFieldsFromPath(resource, commonTrimPaths)
	// perform further sanitization of resource if enabled
	if parseMetricsData {
		// remove fields (if any) that are specific to the resource
		cleanResourceFieldsFromPath(resource, gvkToSanitizePaths[gvk])
		// remove common fields for all resources
		cleanResourceFieldsFromPath(resource, commonSanitizePaths)
	}
	return resource
}

func cleanResourceFieldsFromPath(resource *unstructured.Unstructured, paths []string) {
	// remove specific junk annotation
	annotations := resource.GetAnnotations()
	delete(annotations, KubernetesLastAppliedConfig)
	resource.SetAnnotations(annotations)

	for _, path := range paths {
		unstructured.RemoveNestedField(resource.Object, strings.Split(path, ".")...)
	}
}

func cleanContainers(resource *unstructured.Unstructured, gvk schema.GroupVersionKind, parseMetricsData bool) {
	var pathsToContainers []string
	if gvk.Kind == "Pod" {
		pathsToContainers = []string{"spec.containers", "spec.initContainers"}
	} else {
		// Deployment, DaemonSet, ReplicaSet, ReplicationController, & Job
		pathsToContainers = []string{"spec.template.spec.containers", "spec.template.spec.initContainers"}
	}

	for _, path := range pathsToContainers {
		containersUnstructured, found, err := unstructured.NestedFieldNoCopy(resource.Object, strings.Split(path, ".")...)
		// some kubernetes resources will not have initContainers, we can just skip if so
		if !found {
			continue
		}
		if err != nil {
			log.Warnf("an error occurred getting resources containers %v", err)
			continue
		}
		containers, ok := containersUnstructured.([]interface{})
		if !ok {
			log.Warnf("containers field is not a list. Not cleaning resource")
			continue
		}
		for i := 0; i < len(containers); i++ {
			if parseMetricsData {
				for _, pathToContainer := range gvkToSanitizePaths[containerGVK] {
					unstructured.RemoveNestedField(containers[i].(map[string]interface{}), strings.Split(pathToContainer, ".")...)
				}
			}
			for _, pathToContainer := range gvkToTrimPaths[containerGVK] {
				unstructured.RemoveNestedField(containers[i].(map[string]interface{}), strings.Split(pathToContainer, ".")...)
			}
		}
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
	return AllOf[corev1.Namespace](dcc)
}

func (dcc *DynamicClusterCache) GetAllNodes() []*corev1.Node {
	return AllOf[corev1.Node](dcc)
}

func (dcc *DynamicClusterCache) GetAllPods() []*corev1.Pod {
	return AllOf[corev1.Pod](dcc)
}

func (dcc *DynamicClusterCache) GetAllShortLivedPods() []*corev1.Pod {
	dcc.slpMux.RLock()
	defer dcc.slpMux.RUnlock()

	pods := make([]*corev1.Pod, len(dcc.shortLivedPods))
	copy(pods, dcc.shortLivedPods)
	return pods
}

func (dcc *DynamicClusterCache) AcknowledgeShortLivedPods() {
	dcc.slpMux.Lock()
	defer dcc.slpMux.Unlock()
	dcc.shortLivedPods = []*corev1.Pod{}
}

func (dcc *DynamicClusterCache) GetAllServices() []*corev1.Service {
	return AllOf[corev1.Service](dcc)
}

func (dcc *DynamicClusterCache) GetAllPersistentVolumes() []*corev1.PersistentVolume {
	return AllOf[corev1.PersistentVolume](dcc)
}

func (dcc *DynamicClusterCache) GetAllPersistentVolumeClaims() []*corev1.PersistentVolumeClaim {
	return AllOf[corev1.PersistentVolumeClaim](dcc)
}

func (dcc *DynamicClusterCache) GetAllDeployments() []*appsv1.Deployment {
	return AllOf[appsv1.Deployment](dcc)
}

func (dcc *DynamicClusterCache) GetAllDaemonSets() []*appsv1.DaemonSet {
	return AllOf[appsv1.DaemonSet](dcc)
}

func (dcc *DynamicClusterCache) GetAllStatefulSets() []*appsv1.StatefulSet {
	return AllOf[appsv1.StatefulSet](dcc)
}

func (dcc *DynamicClusterCache) GetAllReplicaSets() []*appsv1.ReplicaSet {
	return AllOf[appsv1.ReplicaSet](dcc)
}

func (dcc *DynamicClusterCache) GetAllStorageClasses() []*stv1.StorageClass {
	return AllOf[stv1.StorageClass](dcc)
}

func (dcc *DynamicClusterCache) GetAllJobs() []*batchv1.Job {
	return AllOf[batchv1.Job](dcc)
}

func (dcc *DynamicClusterCache) GetAllPodDisruptionBudgets() []*policyv1.PodDisruptionBudget {
	return AllOf[policyv1.PodDisruptionBudget](dcc)
}

func (dcc *DynamicClusterCache) GetAllReplicationControllers() []*corev1.ReplicationController {
	return AllOf[corev1.ReplicationController](dcc)
}

func (dcc *DynamicClusterCache) GetAllResourceQuotas() []*corev1.ResourceQuota {
	return AllOf[corev1.ResourceQuota](dcc)
}

// AllOf returns all resources of type T from the dynamic cluster cache
func AllOf[T any](dcc *DynamicClusterCache) []*T {
	t := reflect.TypeFor[T]()
	resource, ok := cacheResourceMap[t]
	if !ok {
		log.Errorf("No resource mapping found for type %s", t)
		return nil
	}

	return ConvertUnstructuredArrayToTypedArray[T](
		dcc.ListUnstructuredByGroupVersionResource(resource),
	)
}
