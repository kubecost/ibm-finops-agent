package cluster

import (
	"github.com/spf13/viper"
	"reflect"
	"strings"
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
	annotationsPath             = "metadata.annotations"
)

type InformerConfig struct {
	ResyncInterval time.Duration
	SanitizeData   bool
}

// LoadInformerConfig returns configs related to informer settings
func LoadInformerConfig() InformerConfig {
	return InformerConfig{
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
	gvkToGvr = map[schema.GroupVersionKind]schema.GroupVersionResource{
		{Group: "apps", Version: "v1", Kind: "Deployment"}:            {Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Kind: "StatefulSet"}:           {Group: "apps", Version: "v1", Resource: "statefulsets"},
		{Group: "apps", Version: "v1", Kind: "DaemonSet"}:             {Group: "apps", Version: "v1", Resource: "daemonsets"},
		{Group: "apps", Version: "v1", Kind: "ReplicaSet"}:            {Group: "apps", Version: "v1", Resource: "replicasets"},
		{Group: "apps", Version: "v1", Kind: "ReplicationController"}: {Group: "apps", Version: "v1", Resource: "replicationcontrollers"},
		{Group: "batch", Version: "v1", Kind: "Job"}:                  {Group: "batch", Version: "v1", Resource: "jobs"},
		{Group: "batch", Version: "v1", Kind: "CronJob"}:              {Group: "batch", Version: "v1", Resource: "cronjobs"},
		{Version: "v1", Kind: "Node"}:                                 {Group: "v1", Resource: "nodes"},
		{Version: "v1", Kind: "Namespace"}:                            {Group: "v1", Resource: "namespaces"},
		{Version: "v1", Kind: "Service"}:                              {Group: "v1", Resource: "services"},
		{Version: "v1", Kind: "Pod"}:                                  {Group: "v1", Resource: "pods"},
		{Version: "v1", Kind: "PersistentVolumeClaim"}:                {Group: "v1", Resource: "persistentvolumeclaims"},
		{Version: "v1", Kind: "PersistentVolume"}:                     {Group: "v1", Resource: "persistentvolumes"},
		{Version: "v1", Kind: "Container"}:                            {Group: "v1", Resource: "containers"},
	}
	containerGVR = schema.GroupVersionResource{Group: "v1", Resource: "containers"}
	// fields to trim on specific resources if parseMetricsData is enabled
	gvrToSanitizePaths = map[schema.GroupVersionResource][]string{
		{Group: "apps", Version: "v1", Resource: "deployments"}: {"spec.progressDeadlineSeconds"},
		{Group: "apps", Version: "v1", Resource: "daemonsets"}:  {"spec.updateStrategy"},
		{Group: "batch", Version: "v1", Resource: "jobs"}:       {"spec.parallelism", "spec.completions", "spec.activeDeadlineSeconds", "spec.backoffLimit", "spec.manualSelector", "spec.ttlSecondsAfterFinished", "spec.completionMode", "spec.suspend"},
		{Group: "batch", Version: "v1", Resource: "cronjobs"}:   {"spec"},
		{Group: "v1", Resource: "containers"}:                   {"command", "args", "imagePullPolicy", "livenessProbe", "readinessProbe", "startupProbe", "terminationMessagePath", "terminationMessagePolicy", "securityContext"},
		{Version: "v1", Resource: "services"}:                   {"spec.ports", "spec.clusterIPs", "spec.externalIPs", "spec.sessionAffinity", "spec.loadBalancerIP", "spec.loadBalancerSourceRanges", "spec.externalName", "spec.externalTrafficPolicy", "spec.healthCheckNodePort", "spec.sessionAffinityConfig", "spec.ipFamilies", "spec.ipFamilyPolicy", "spec.allocatedLoadBalancerNodePorts", "spec.loadBalancerClass", "spec.internalTrafficPolicy"},
	}
	// common fields to trim on all resources if parseMetricsData is enabled
	commonSanitizePaths = []string{"spec.revisionHistoryLimit", "spec.minReadySeconds", "metadata.finalizers"}
	// fields to trim on specific resources by default
	gvrToTrimPaths = map[schema.GroupVersionResource][]string{
		{Group: "v1", Resource: "containers"}: {"env"},
	}
	// common fields to trim on all resources by default
	commonTrimPaths = []string{"metadata.managedFields", annotationsPath}
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
		var casted *unstructured.Unstructured
		var ok bool
		if casted, ok = resource.(*unstructured.Unstructured); !ok {
			log.Warnf("Not trimming or sanitizing non-unstructured resource: %s", reflect.TypeOf(resource))
			return resource, nil
		}
		casted = cleanResource(casted, parseMetricsData)
		return resource, nil
	}
}

func cleanResource(resource *unstructured.Unstructured, parseMetricsData bool) *unstructured.Unstructured {
	gvk := resource.GetObjectKind().GroupVersionKind()
	gvr := gvkToGvr[gvk]
	// for pods, we need to clean the individual containers before cleaning the pod fields
	if gvr.Resource == "pods" || gvr.Resource == "deployments" || gvr.Resource == "replicasets" || gvr.Resource == "replicationcontrollers" || gvr.Resource == "jobs" || gvr.Resource == "daemonsets" {
		cleanContainers(resource, gvr, parseMetricsData)
	}
	// remove paths (if any) that are specific to the resource
	cleanResourceFieldsFromPath(resource, gvrToTrimPaths[gvr])
	// remove common paths for all resources
	cleanResourceFieldsFromPath(resource, commonTrimPaths)
	// perform further sanitization of kubernetes resources if enabled
	if parseMetricsData {
		// remove paths (if any) that are specific to the resource
		cleanResourceFieldsFromPath(resource, gvrToSanitizePaths[gvr])
		// remove common paths for all resources
		cleanResourceFieldsFromPath(resource, commonSanitizePaths)
	}
	return resource
}

func cleanResourceFieldsFromPath(resource *unstructured.Unstructured, paths []string) {
	for _, path := range paths {
		if path == annotationsPath {
			annotations := resource.GetAnnotations()
			delete(annotations, KubernetesLastAppliedConfig)
			resource.SetAnnotations(annotations)
			continue
		}
		unstructured.RemoveNestedField(resource.Object, strings.Split(path, ".")...)
	}
}

func cleanContainers(resource *unstructured.Unstructured, gvr schema.GroupVersionResource, parseMetricsData bool) {
	var pathsToContainers []string
	if gvr.Resource == "pods" {
		pathsToContainers = append(pathsToContainers, "spec.containers", "spec.initContainers")
	} else {
		// deployments, daemonsets, replicasets, replicationcontrollers, & jobs
		pathsToContainers = append(pathsToContainers, "spec.template.spec.containers", "spec.template.spec.initContainers")
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
				for _, pathToContainer := range gvrToSanitizePaths[containerGVR] {
					unstructured.RemoveNestedField(containers[i].(map[string]interface{}), strings.Split(pathToContainer, ".")...)
				}
			}
			for _, pathToContainer := range gvrToTrimPaths[containerGVR] {
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
