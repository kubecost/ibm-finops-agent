package cldy

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	url "net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/client-go/kubernetes/scheme"

	"github.com/ibm/finops-agent/pkg/emitter"
	"github.com/ibm/finops-agent/pkg/version"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/spf13/viper"
	v1 "k8s.io/api/core/v1"
)

const initialSampleCt = -1
const statsFileTemplate = "%s-summary-%s.json"
const baseline = "baseline"
const stats = "stats"
const scratchPath = "scratch"
const uploadPath = "upload"
const agentName = "ibm-finops-agent"

type Emitter struct {
	config            EmitterConfig
	startTime         time.Time
	lastEmission      time.Time
	emissionInterval  time.Duration
	sampleCt          int
	currentSamplePath string
	nextSamplePath    string
	agentVersion      string
	Uploader          Uploader
	ClusterID         *string
	ScratchPath       string

	// Node-stats collection diagnostics, updated from each snapshot the emitter receives.
	// Guarded by nodeStatsMu because Healthy() is invoked from the /healthz handler goroutine
	// while Emit()/Init() run on the exporter goroutine.
	nodeStatsMu                  sync.RWMutex
	lastSuccessfulNodeCollection time.Time
	lastNodeCollectionErr        error
}

type EmitterConfig struct {
	UploaderConfig
	EmitAsJson                  bool
	ParseMetricData             bool
	EmissionInterval            time.Duration
	KubernetesResourcesRequired []string
	ClusterVersion              string
	ClusterVersionGit           string
	ClusterVersionMajor         string
	ClusterVersionMinor         string
}

const UPLOAD_FREQUENCY = 10

// UploadFrequencyDuration is the upload cadence as a time.Duration.
var UploadFrequencyDuration = time.Minute * time.Duration(UPLOAD_FREQUENCY)

// MaxStaleUploadCycles is how many upload cycles may pass with no successful node-stats
// collection before the agent considers itself wedged. Used by the /healthz staleness check.
const MaxStaleUploadCycles = 3

func NewEmitterConfigFromEnv() (EmitterConfig, error) {
	viper.SetEnvPrefix("CLOUDABILITY")
	defer viper.SetEnvPrefix("")
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("HTTPS_CLIENT_TIMEOUT", 60) // Note for readme: In seconds
	viper.SetDefault("UPLOAD_RETRY_COUNT", 5)
	viper.SetDefault("OUTBOUND_PROXY_INSECURE", false)
	viper.SetDefault("UPLOAD_REGION", "us")
	viper.SetDefault("SCRATCH_DIR", "/opt/finops-agent")
	viper.SetDefault("EMIT_AS_JSON", true)
	viper.SetDefault("PARSE_METRIC_DATA", false)
	viper.SetDefault("EMISSION_INTERVAL", "3m")
	viper.SetDefault("USE_PROXY_FOR_GETTING_UPLOAD_URL_ONLY", false)

	var outboundProxyUrl *url.URL
	proxyURL := viper.GetString("OUTBOUND_PROXY")
	if proxyURL != "" {
		var err error
		outboundProxyUrl, err = url.Parse(proxyURL)
		if err != nil {
			return EmitterConfig{}, fmt.Errorf("failed to parse CLOUDABILITY_OUTBOUND_PROXY")
		}
	}
	// check for custom mode env vars
	customS3UploadBucket := viper.GetString("CUSTOM_S3_UPLOAD_BUCKET")
	customS3UploadRegion := viper.GetString("CUSTOM_S3_UPLOAD_REGION")
	customAzureBlobContainerName := viper.GetString("CUSTOM_AZURE_BLOB_CONTAINER_NAME")

	var azureBlobClientSecret string
	if customAzureBlobContainerName != "" {
		azureBlobClientSecret = getSecretFromFileVolume(viper.GetString("CUSTOM_AZURE_BLOB_CLIENT_SECRET_FILEPATH"))
	}

	apiKey := strings.TrimSpace(viper.GetString("API_KEY"))
	if apiKey == "" {
		if fp := viper.GetString("API_KEY_FILEPATH"); fp != "" {
			if key, err := os.ReadFile(fp); err == nil {
				apiKey = strings.TrimSpace(string(key))
			}
		}
	}

	var keyAccess, keySecret, envID string
	if customS3UploadBucket == "" && customS3UploadRegion == "" && customAzureBlobContainerName == "" && apiKey == "" {
		keyAccess = getSecretFromFileVolume(viper.GetString("KEY_ACCESS_FILEPATH"))
		keySecret = getSecretFromFileVolume(viper.GetString("KEY_SECRET_FILEPATH"))
		envID = getSecretFromFileVolume(viper.GetString("ENV_ID_FILEPATH"))
	}
	return EmitterConfig{
		UploaderConfig: UploaderConfig{
			ApptioConfig: ApptioConfig{
				ClusterName:                     viper.GetString("CLUSTER_NAME"),
				SecretManager:                   NewKeyValueSecretManager(keyAccess, keySecret),
				APIKeySecretManager:             NewValueSecretManager(apiKey),
				EnvID:                           envID,
				Timeout:                         time.Second * time.Duration(viper.GetInt("HTTPS_CLIENT_TIMEOUT")),
				Retries:                         viper.GetInt("UPLOAD_RETRY_COUNT"),
				ProxyURL:                        outboundProxyUrl,
				ProxyAuth:                       viper.GetString("OUTBOUND_PROXY_AUTH"),
				ProxyInsecure:                   viper.GetBool("OUTBOUND_PROXY_INSECURE"),
				Region:                          viper.GetString("UPLOAD_REGION"),
				CustomS3UploadBucket:            customS3UploadBucket,
				CustomS3UploadRegion:            customS3UploadRegion,
				CustomAzureBlobContainerName:    customAzureBlobContainerName,
				CustomAzureBlobUrl:              viper.GetString("CUSTOM_AZURE_BLOB_URL"),
				CustomAzureTenantID:             viper.GetString("CUSTOM_AZURE_BLOB_TENANT_ID"),
				CustomAzureClientID:             viper.GetString("CUSTOM_AZURE_BLOB_CLIENT_ID"),
				CustomAzureClientSecret:         NewValueSecretManager(azureBlobClientSecret),
				UseProxyForGettingUploadURLOnly: viper.GetBool("USE_PROXY_FOR_GETTING_UPLOAD_URL_ONLY"),
			},
			UploadFrequency: time.Minute * time.Duration(UPLOAD_FREQUENCY),
			ScratchDir:      viper.GetString("SCRATCH_DIR"),
		},
		EmitAsJson:       viper.GetBool("EMIT_AS_JSON"),
		ParseMetricData:  viper.GetBool("PARSE_METRIC_DATA"),
		EmissionInterval: viper.GetDuration("EMISSION_INTERVAL"),
		// Cloudy emitter requires the following kubernetes resources to be enabled
		KubernetesResourcesRequired: []string{
			emitter.SnapshotNodes,
			emitter.SnapshotPods,
			emitter.SnapshotDeployments,
			emitter.SnapshotNamespaces,
			emitter.SnapshotServices,
			emitter.SnapshotDaemonSets,
			emitter.SnapshotStatefulSets,
			emitter.SnapshotJobs,
			emitter.SnapshotReplicaSets,
			emitter.SnapshotPersistentVolumes,
			emitter.SnapshotPersistentVolumeClaims,
			emitter.SnapshotReplicationControllers,
		},
	}, nil
}

func NewEmitter(config EmitterConfig, stop chan struct{}) emitter.Emitter {
	currentTime := time.Now().UTC()

	return &Emitter{
		config:           config,
		Uploader:         NewCldyUploader(config.UploaderConfig, stop),
		sampleCt:         initialSampleCt,
		startTime:        currentTime,
		lastEmission:     currentTime,
		emissionInterval: config.EmissionInterval,
		agentVersion:     version.Version,
	}
}

// sampleDirPerm is the mode applied to the scratch, upload and per-sample
// directories. Those hold a full inventory of the cluster's Kubernetes
// resources between collection and upload, so they must not be world-readable
// or world-writable -- os.ModePerm is 0777, which is both.
//
// 0750 rather than 0700 so that group access is retained, which some Kubernetes
// volume setups rely on when a pod sets fsGroup. The agent itself is the only
// writer either way.
const sampleDirPerm = 0o750

func createIfNotExists(path string) error {
	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, sampleDirPerm)
}

// getSecretFromFileVolume attempts to gather secret from filepath
func getSecretFromFileVolume(filepath string) string {
	key, err := os.ReadFile(filepath)
	if err != nil {
		log.Warnf("error attempting to collect secret from file: %s with err: %v", filepath, err)
		return ""
	}
	// Trim space on secrets
	return strings.TrimSpace(string(key))
}

func (ce *Emitter) ID() emitter.EmitterID {
	return emitter.CldyEmitterID
}

// Healthy reports whether the cldy emitter considers itself healthy.
// Returns false when node-stats collection has been stale for longer than
// MaxStaleUploadCycles * UploadFrequencyDuration, signaling that a container
// restart is warranted.
//
// Staleness is tracked from the snapshots the emitter receives, so this works on the
// foreground collection path (node stats fetched at snapshot time). When node-stats
// collection fails for every node, the snapshot itself fails upstream and Emit() is not
// called, so lastSuccessfulNodeCollection stops advancing and the emitter eventually
// reports unhealthy.
func (ce *Emitter) Healthy() bool {
	ce.nodeStatsMu.RLock()
	lastSuccess := ce.lastSuccessfulNodeCollection
	ce.nodeStatsMu.RUnlock()

	// startup grace: no successful collection has been observed yet
	if lastSuccess.IsZero() {
		return true
	}
	restartThreshold := time.Duration(MaxStaleUploadCycles) * UploadFrequencyDuration
	return time.Since(lastSuccess) <= restartThreshold
}

// recordNodeStats captures node-stats collection diagnostics from a snapshot. It advances
// the last-successful-collection timestamp whenever any node stats were returned, and stores
// the (partial) collection error for reporting in the agent status file.
func (ce *Emitter) recordNodeStats(ns *emitter.NodeStatsSummary) {
	ce.nodeStatsMu.Lock()
	defer ce.nodeStatsMu.Unlock()

	if ns == nil {
		return
	}
	if len(ns.Stats) > 0 {
		ce.lastSuccessfulNodeCollection = time.Now().UTC()
	}
	ce.lastNodeCollectionErr = ns.CollectionErr
}

// unwrapNodeErrors splits a possibly-joined collection error into its component per-node
// errors. Returns nil when err is nil.
func unwrapNodeErrors(err error) []error {
	if err == nil {
		return nil
	}
	if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
		return multiErr.Unwrap()
	}
	return []error{err}
}

// nodeErrorDetails deduplicates node collection errors by message and returns one entry per
// distinct message with its occurrence count. Returns nil when there are no errors.
func nodeErrorDetails(nodeErrors []error) []errorDetail {
	if len(nodeErrors) == 0 {
		return nil
	}
	counts := map[string]int{}
	for _, ne := range nodeErrors {
		msg := ""
		if ne != nil {
			msg = ne.Error()
		}
		counts[msg]++
	}
	details := make([]errorDetail, 0, len(counts))
	for msg, count := range counts {
		details = append(details, errorDetail{
			Message: msg,
			Type:    "node_error",
			Count:   count,
		})
	}
	return details
}

func (ce *Emitter) Init(cs *emitter.ClusterSnapshot) error {
	log.Infof("Initializing Cloudability emitter.")

	clusterID := getClusterID(cs.Kubernetes.Namespaces)
	ce.ClusterID = &clusterID
	ce.Uploader.SetClusterID(clusterID)

	ce.ScratchPath = ce.config.ScratchDir + "/" + scratchPath + "/" + clusterID
	err := createIfNotExists(ce.ScratchPath)
	if err != nil {
		return fmt.Errorf("failed to create scratch directory: %s", err.Error())
	}

	// Since sample count is intiialized at -1, use next path to get 0 as first index
	ce.currentSamplePath = ce.newNextSamplePath()
	err = os.Mkdir(ce.currentSamplePath, sampleDirPerm)
	if err != nil {
		return err
	}

	ce.recordNodeStats(cs.NodeStats)
	err = ce.writeStatsData(cs.NodeStats)
	if err != nil {
		return err
	}

	ce.sampleCt = 0
	return nil
}

func (ce *Emitter) Emit(ctx context.Context, cs *emitter.ClusterSnapshot) error {
	// Record collection diagnostics from every snapshot, even when downsampling uploads, so
	// health/staleness tracking reflects the latest node-stats collection.
	ce.recordNodeStats(cs.NodeStats)

	// Emit only after the emission interval has been met
	if ce.shouldDownsample() {
		return nil
	}

	ce.nextSamplePath = ce.newNextSamplePath()
	err := os.Mkdir(ce.nextSamplePath, sampleDirPerm)
	if err != nil {
		return err
	}

	err = ce.writeStatsData(cs.NodeStats)
	if err != nil {
		return err
	}
	err = ce.writeMetadata(cs.Kubernetes)
	if err != nil {
		return err
	}

	ce.Uploader.AddSample(ce.currentSamplePath)
	ce.sampleCt++
	ce.currentSamplePath = ce.nextSamplePath
	log.Debugf("Emitted sample to Cldy: %d", ce.sampleCt)

	return nil
}

func (ce *Emitter) writeStatsData(statsData *emitter.NodeStatsSummary) error {
	if statsData == nil {
		return fmt.Errorf("stats data was nil")
	}
	for _, val := range statsData.Stats {
		data, err := json.Marshal(val)
		if err != nil {
			return err
		}
		err = ce.writeStatsFile(stats, val.Node.NodeName, data)
		if err != nil {
			return err
		}
		err = ce.writeStatsFile(baseline, val.Node.NodeName, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ce *Emitter) writeStatsFile(outputPrefix string, nodeName string, data []byte) (rerr error) {
	if !IsAvailableDiskSpace(uint64(len(data)), ce.ScratchPath) {
		err := ce.ClearOldScratchSamples()
		if err != nil {
			return err
		}
	}

	var fileName string
	if outputPrefix == stats {
		fileName = ce.currentSamplePath
	} else {
		if ce.sampleCt == -1 {
			return nil
		}
		fileName = ce.nextSamplePath
	}
	fileName = fileName + fmt.Sprintf(statsFileTemplate, outputPrefix, nodeName)
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer safeClose(file.Close, &rerr)

	_, err = file.Write(data)
	return err
}

func (ce *Emitter) writeMetadata(snapshot *emitter.KubernetesSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("k8s snapshot was nil")
	}
	for name, objs := range metadataToObj(snapshot) {
		err := ce.writeObjects(name, objs)
		if err != nil {
			return err
		}
	}
	return ce.writeAgentFile()
}

func (ce *Emitter) ClearOldScratchSamples() error {
	log.Infof("Disk space threshold met. Attempting to clear samples over 1 hour old.")

	files, err := os.ReadDir(ce.ScratchPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		filePath := filepath.Join(ce.ScratchPath, file.Name())
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			log.Warnf("problem retrieving file information: %s", err)
			continue
		}

		if time.Since(fileInfo.ModTime()) > time.Hour*1 {
			err := os.RemoveAll(filePath)
			if err != nil {
				log.Warnf("problem deleting file: %s", err)
			}
			ce.Uploader.RemoveSample(filePath + "/")
		}
	}

	return nil
}

func metadataToObj(snapshot *emitter.KubernetesSnapshot) map[string][]runtime.Object {
	return map[string][]runtime.Object{
		//TODO: add cronjobs
		"nodes":                  checkAndConvertNodes(snapshot.Nodes),
		"pods":                   convertObj(append(snapshot.Pods, snapshot.ShortLivedPods...)),
		"deployments":            convertObj(snapshot.Deployments),
		"replicasets":            convertObj(snapshot.ReplicaSets),
		"daemonsets":             convertObj(snapshot.DaemonSets),
		"namespaces":             convertObj(snapshot.Namespaces),
		"services":               convertObj(snapshot.Services),
		"replicationcontrollers": convertObj(snapshot.ReplicationControllers),
		"persistentvolumes":      convertObj(snapshot.PersistentVolumes),
		"persistentvolumeclaims": convertObj(snapshot.PersistentVolumeClaims),
		"statefulsets":           convertObj(snapshot.StatefulSets),
		"jobs":                   convertObj(snapshot.Jobs),
	}
}

func checkAndConvertNodes(nodes []*v1.Node) []runtime.Object {
	var data []runtime.Object
	for _, node := range nodes {
		if node.Spec.ProviderID == "" {
			log.Warnf("Node ProviderID is not set for node: %s which may be because the node is running in a self managed environment, and this may cause inconsistent gathering of metrics data.", node.Name)
		}
		data = append(data, node)
	}
	return data
}

func convertObj[T runtime.Object](objs []T) []runtime.Object {
	var data []runtime.Object
	for _, obj := range objs {
		if shouldSkipResource(obj) {
			continue
		}

		data = append(data, obj)
	}
	return data
}

func shouldSkipResource[T runtime.Object](obj T) bool {
	// safe buffer to allow for longer lived resources to be ingested correctly
	previousHour := time.Now().UTC().Add(-1 * time.Hour)
	switch resource := any(obj).(type) {
	case *batchv1.Job:
		return shouldSkipJob(previousHour, resource)
	case *v1.Pod:
		return shouldSkipPod(previousHour, resource)
	case *appsv1.ReplicaSet:
		return resource.Status.Replicas == 0 && previousHour.After(resource.CreationTimestamp.Time)
	}
	return false
}

func shouldSkipJob(previousHour time.Time, resource *batchv1.Job) bool {
	if resource.Status.CompletionTime != nil &&
		previousHour.After(resource.Status.CompletionTime.Time) {
		return true
	}
	if resource.Status.Failed > 0 {
		for _, condition := range resource.Status.Conditions {
			if condition.Type == batchv1.JobFailed {
				if previousHour.After(condition.LastTransitionTime.Time) {
					return true
				}
			}
		}
	}
	return false
}

func shouldSkipPod(previousHour time.Time, resource *v1.Pod) bool {
	if resource.Status.Phase == v1.PodSucceeded || resource.Status.Phase == v1.PodFailed {
		canSkip := true
		for _, v := range resource.Status.ContainerStatuses {
			if v.State.Terminated != nil && v.State.Terminated.FinishedAt.After(previousHour) {
				canSkip = false
			}
		}
		return canSkip
	}
	return false
}

func (ce *Emitter) writeObjects(name string, data []runtime.Object) (err error) {
	outputPath := ce.currentSamplePath + name + ce.getSuffix()
	outFile, err := os.Create(outputPath)
	defer safeClose(outFile.Close, &err)
	if err != nil {
		return err
	}
	for _, obj := range data {
		bytes, err := ce.marshalObject(obj)
		if err != nil {
			return err
		}
		_, err = outFile.Write(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ce *Emitter) writeAgentFile() (err error) {
	outputPath := ce.currentSamplePath + "agent-measurement.json"
	outFile, err := os.Create(outputPath)
	defer safeClose(outFile.Close, &err)
	if err != nil {
		return err
	}
	now := time.Now()
	values := map[string]string{}
	metrics := map[string]int{}
	values["agent_version"] = ce.agentVersion
	values["agent_name"] = agentName
	values["cluster_name"] = ce.config.ClusterName
	values["cluster_version"] = ce.config.ClusterVersion
	// for backwards compatibility with the metrics-agent
	values["cluster_version_git"] = ce.config.ClusterVersionGit
	values["cluster_version_major"] = ce.config.ClusterVersionMajor
	values["cluster_version_minor"] = ce.config.ClusterVersionMinor
	if ce.config.ProxyURL != nil {
		values["outbound_proxy_url"] = ce.config.ProxyURL.Path
	}
	values["insecure"] = strconv.FormatBool(ce.config.ProxyInsecure)
	values["emission_interval"] = ce.emissionInterval.String()
	values["parse_metric_data"] = strconv.FormatBool(ce.config.ParseMetricData)
	values["upload_region"] = ce.config.Region
	values["custom_s3_bucket"] = ce.config.CustomS3UploadBucket
	values["custom_s3_region"] = ce.config.CustomS3UploadRegion
	values["custom_azure_blob_name"] = ce.config.CustomAzureBlobContainerName
	metrics["uptime"] = int(now.UTC().Sub(ce.startTime).Seconds())

	// Node collection diagnostics derived from the most recent snapshot.
	ce.nodeStatsMu.RLock()
	lastSuccess := ce.lastSuccessfulNodeCollection
	collectionErr := ce.lastNodeCollectionErr
	ce.nodeStatsMu.RUnlock()

	// node_stats_age_seconds is the age of the node-stats data in this sample. On the
	// foreground path (default) stats are fetched live per emit, so it is effectively ~0.
	// On the background path (future work) it reports the cache age and grows as it goes stale.
	var nodeStatsAgeSeconds int64
	if !lastSuccess.IsZero() {
		nodeStatsAgeSeconds = int64(time.Since(lastSuccess).Seconds())
	}
	values["node_stats_age_seconds"] = strconv.FormatInt(nodeStatsAgeSeconds, 10)

	nodeErrors := unwrapNodeErrors(collectionErr)
	metrics["nodes_failed"] = len(nodeErrors)

	agent := agentData{
		Name:    "cldy_agent_status",
		Metrics: metrics,
		Tags: map[string]string{
			"cluster_uid": *ce.ClusterID,
		},
		Ts:     now.UTC().UnixMilli() / 1000,
		Values: values,
		Errors: nodeErrorDetails(nodeErrors),
	}
	agentBytes, err := json.Marshal(agent)
	if err != nil {
		return err
	}
	_, err = outFile.Write(agentBytes)
	return err
}

type agentData struct {
	Name    string            `json:"name"`
	Metrics map[string]int    `json:"metrics"`
	Tags    map[string]string `json:"tags"`
	Ts      int64             `json:"ts"`
	Values  map[string]string `json:"values"`
	Errors  []errorDetail     `json:"errors,omitempty"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Count   int    `json:"count"`
}

func (ce *Emitter) getSuffix() string {
	if ce.config.EmitAsJson {
		return ".jsonl"
	}
	return ".proto"
}

func (ce *Emitter) newNextSamplePath() string {
	return SafePath(ce.ScratchPath, fmt.Sprintf("%d_%d/", time.Now().UTC().UnixMilli(), ce.sampleCt+1))
}

func (ce *Emitter) marshalObject(object runtime.Object) ([]byte, error) {
	if ce.config.EmitAsJson {
		data, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		data = append(data, []byte("\n")...)
		return data, nil
	}
	serializer := protobuf.NewSerializer(scheme.Scheme, scheme.Scheme)
	data, err := runtime.Encode(serializer, object)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(len(data)))
	buf = append(buf, data...)
	return buf, nil
}

func getClusterID(namespaces []*v1.Namespace) string {
	for _, ns := range namespaces {
		if ns.Name == "default" {
			return string(ns.GetUID())
		}
	}
	// should probably error?
	return ""
}

// Checks sample count and emits sample only if it equals or exceeds the emission interval
// (trimmed down to 90%) plus the time of the last emission
func (ce *Emitter) shouldDownsample() bool {
	bufferedEmissionInterval := float32(ce.emissionInterval) * .9
	emissionThreshold := ce.lastEmission.Add(time.Duration(bufferedEmissionInterval))

	if time.Now().UTC().After(emissionThreshold) {
		ce.lastEmission = ce.lastEmission.Add(ce.emissionInterval)
		return false
	}
	return true
}
