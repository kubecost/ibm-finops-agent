package cldy

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	url "net/url"
	"os"
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
	"github.com/ibm/finops-agent/pkg/telemetry"
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

// maxPendingShortLivedPods caps the short-lived pods the emitter holds between samples. Chunk 05
// caps the cluster cache's buffer; this is the emitter's own bound until chunk 06 moves the
// commit into the exporter.
const maxPendingShortLivedPods = 10000

// Init retry backoff: 30s doubling to 5 min.
const (
	initRetryBaseBackoff = 30 * time.Second
	initRetryMaxBackoff  = 5 * time.Minute
)

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

	// now is the emitter's clock. It is nil in production (see clock) and only set by tests.
	now func() time.Time

	// Node-stats collection diagnostics, updated from each snapshot the emitter receives.
	// lastSuccessfulNodeCollection is when the stats in the last snapshot were collected (F-21).
	nodeStatsMu                  sync.RWMutex
	lastSuccessfulNodeCollection time.Time
	lastNodeCollectionErr        error

	// Init retry state (F-08). Only touched on the exporter goroutine, like the sample state.
	initialised     bool
	initFailures    int
	nextInitAttempt time.Time

	// pendingShortLivedPods are short-lived pods drained from the cluster cache that no finalised
	// sample holds yet, oldest first, at most maxPendingShortLivedPods (F-37).
	pendingShortLivedPods []*v1.Pod
	pendingShortLivedKeys map[string]int // shortLivedPodKey -> index in pendingShortLivedPods

	// lastSampleBytes is the size of the last finalised sample, the disk budget for the next.
	lastSampleBytes int64

	// unsyncedResources are the resources the sample being written leaves out because their
	// informers haven't synced, reported in agent-measurement.json (I6).
	unsyncedResources []string

	// events receives drops, discards, emit outcomes and condition changes; conditions holds
	// the active conditions for edge-triggered logging and health checks. Both are the
	// uploader's, when the uploader is a *CldyUploader.
	events     EventSink
	counts     *EventCounts
	conditions *conditionStore

	// queue is the upload queue on disk, which the disk budget evicts from. It is the
	// uploader's, when the uploader is a *CldyUploader.
	queue *diskQueue
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
	// StatusSummary, if set, returns the whole agent's health, written to agent-measurement.json
	// as agent_health (I6): telemetry.Metrics.Summary in production. Without it the emitter
	// writes its own. It is not read from the environment.
	StatusSummary func() telemetry.Summary
}

const UPLOAD_FREQUENCY = 10

// UploadFrequencyDuration is the upload cadence as a time.Duration.
var UploadFrequencyDuration = time.Minute * time.Duration(UPLOAD_FREQUENCY)

// MaxStaleUploadCycles is how many upload cycles may pass with no successful node-stats
// collection before the agent is not ready (node_stats_stale). It is a readiness threshold, not a
// liveness one (D1): a restart doesn't fix unreachable kubelets.
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
	viper.SetDefault("RECOVERY_PERIOD", defaultRecoveryPeriod.String())
	viper.SetDefault("BACKLOG_MAX_MB", defaultBacklogMaxBytes>>20)

	// Pending data older than this at startup is dropped rather than uploaded. A bare number
	// parses as nanoseconds, so anything under one upload interval is rejected as a mistake.
	recoveryPeriod := viper.GetDuration("RECOVERY_PERIOD")
	if recoveryPeriod < UploadFrequencyDuration {
		return EmitterConfig{}, fmt.Errorf("CLOUDABILITY_RECOVERY_PERIOD must be a duration of at least %s, such as 72h; got %q",
			UploadFrequencyDuration, viper.GetString("RECOVERY_PERIOD"))
	}

	// The most the upload queue may hold, samples and payloads together; past it the oldest are
	// evicted and counted.
	backlogMaxMB := viper.GetInt64("BACKLOG_MAX_MB")
	if backlogMaxMB < 1 {
		return EmitterConfig{}, fmt.Errorf("CLOUDABILITY_BACKLOG_MAX_MB must be a whole number of MiB, at least 1; got %q",
			viper.GetString("BACKLOG_MAX_MB"))
	}

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
		UploadFrequency:                 time.Minute * time.Duration(UPLOAD_FREQUENCY),
		ScratchDir:                      viper.GetString("SCRATCH_DIR"),
		RecoveryPeriod:                  recoveryPeriod,
		BacklogMaxBytes:                 backlogMaxMB << 20,
		EmitAsJson:                      viper.GetBool("EMIT_AS_JSON"),
		ParseMetricData:                 viper.GetBool("PARSE_METRIC_DATA"),
		EmissionInterval:                viper.GetDuration("EMISSION_INTERVAL"),
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
	return newEmitter(config, NewCldyUploader(config.UploaderConfig, stop), nil)
}

// newEmitter builds an Emitter around the given uploader. now is the emitter's clock; nil
// means time.Now. Tests use it to inject a fake clock and uploader.
func newEmitter(config EmitterConfig, uploader Uploader, now func() time.Time) *Emitter {
	// Share the uploader's sink so that startup recovery and the emitter report through one, and
	// its queue so that the disk budget and packaging don't race.
	var queue *diskQueue
	counts, events := newEventSinks(config.Events)
	conditions := newConditionStore(now)
	if cu, ok := uploader.(*CldyUploader); ok {
		events, counts = cu.events, cu.counts
		queue = cu.queue
		conditions = cu.conditions
	} else {
		queue = newDiskQueue(config.ScratchDir, events, now)
	}
	ce := &Emitter{
		config:           config,
		Uploader:         uploader,
		sampleCt:         initialSampleCt,
		emissionInterval: config.EmissionInterval,
		agentVersion:     version.Version,
		now:              now,
		events:           events,
		counts:           counts,
		conditions:       conditions,
		queue:            queue,
	}
	currentTime := ce.clock().UTC()
	ce.startTime = currentTime
	ce.lastEmission = currentTime
	return ce
}

// clock returns the current time from the injected clock, or time.Now when none is set.
func (ce *Emitter) clock() time.Time {
	if ce.now == nil {
		return time.Now()
	}
	return ce.now()
}

func createIfNotExists(path string) error {
	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, os.ModePerm)
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

// recordNodeStats captures node-stats collection diagnostics from a snapshot. Whenever any node
// stats were returned it records when they were collected, which in background collection mode
// can be well before the snapshot (F-21), and it stores the (partial) collection error for
// reporting in the agent status file.
func (ce *Emitter) recordNodeStats(ns *emitter.NodeStatsSummary) {
	ce.nodeStatsMu.Lock()
	defer ce.nodeStatsMu.Unlock()

	if ns == nil {
		return
	}
	if len(ns.Stats) > 0 {
		// CollectedAt is on the wall clock; carry its age over to the emitter's clock.
		collected := ce.clock()
		if !ns.CollectedAt.IsZero() {
			collected = collected.Add(-max(time.Since(ns.CollectedAt), 0))
		}
		ce.lastSuccessfulNodeCollection = collected.UTC()
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

// Init initialises the emitter from the first snapshot: it takes the cluster ID from the
// default namespace, creates the scratch directory and starts the first sample. On failure
// nothing is created and Emit retries Init with backoff until it succeeds (F-08). Init is a
// no-op once it has succeeded.
func (ce *Emitter) Init(cs *emitter.ClusterSnapshot) error {
	if cs == nil {
		return ce.initialise(cs)
	}
	ce.recordNodeStats(cs.NodeStats)
	if cs.Kubernetes != nil {
		ce.addShortLivedPods(cs.Kubernetes.ShortLivedPods)
	}
	return ce.initialise(cs)
}

// initialise runs one Init attempt and records its outcome for the retry in Emit.
func (ce *Emitter) initialise(cs *emitter.ClusterSnapshot) error {
	if ce.initialised {
		return nil
	}
	log.Infof("Initializing Cloudability emitter.")
	err := ce.tryInit(cs)
	if err != nil {
		ce.initFailures++
		ce.nextInitAttempt = ce.clock().Add(initRetryBackoff(ce.initFailures))
		ce.setCondition(conditionUninitialised, true, fmt.Sprintf("Cloudability emitter failed to initialise, nothing is written until it does: %v", err))
		return err
	}
	ce.initialised = true
	ce.setCondition(conditionUninitialised, false, "Cloudability emitter initialised")
	return nil
}

func (ce *Emitter) tryInit(cs *emitter.ClusterSnapshot) error {
	if cs == nil || cs.Kubernetes == nil {
		return fmt.Errorf("snapshot has no kubernetes data")
	}
	clusterID, err := getClusterID(cs.Kubernetes.Namespaces)
	if err != nil {
		return err
	}

	scratch := ce.config.ScratchDir + "/" + scratchPath + "/" + clusterID
	err = createIfNotExists(scratch)
	if err != nil {
		return fmt.Errorf("failed to create scratch directory: %s", err.Error())
	}
	ce.ScratchPath = scratch

	// Since sample count is intiialized at -1, use next path to get 0 as first index
	current := ce.newStagingPath()
	err = os.Mkdir(current, os.ModePerm)
	if err != nil {
		return err
	}
	ce.currentSamplePath = current
	err = ce.writeStatsData(cs.NodeStats)
	if err != nil {
		ce.discardStaging(current)
		ce.currentSamplePath = ""
		return err
	}

	ce.ClusterID = &clusterID
	ce.Uploader.SetClusterID(clusterID)
	ce.sampleCt = 0
	ce.lastEmission = ce.clock().UTC()
	return nil
}

// initRetryBackoff is the wait before Init attempt failures+1: capped exponential with jitter.
func initRetryBackoff(failures int) time.Duration {
	backoff := initRetryMaxBackoff
	if failures < 16 {
		backoff = min(initRetryBaseBackoff<<(failures-1), initRetryMaxBackoff)
	}
	// Jitter down by up to 20% so a fleet of agents doesn't retry in step.
	return backoff - time.Duration(rand.Int64N(int64(backoff)/5+1))
}

func (ce *Emitter) Emit(ctx context.Context, cs *emitter.ClusterSnapshot) error {
	// Record collection diagnostics from every snapshot, even when downsampling uploads, so
	// health/staleness tracking reflects the latest node-stats collection.
	ce.recordNodeStats(cs.NodeStats)
	// The snapshot drained these from the cluster cache, so keep them until a sample holds them,
	// whether or not this tick emits (F-37).
	if cs.Kubernetes != nil {
		ce.addShortLivedPods(cs.Kubernetes.ShortLivedPods)
	}

	if !ce.initialised {
		if ce.clock().Before(ce.nextInitAttempt) {
			ce.events.EmitResult(emitResultSkipped)
			return nil
		}
		if err := ce.initialise(cs); err != nil {
			ce.events.EmitResult(emitResultError)
			return fmt.Errorf("cloudability emitter is not initialised: %w", err)
		}
		return nil
	}

	// Emit only after the emission interval has been met
	slot, skipped, due := ce.emissionSlot()
	if !due {
		return nil
	}

	ce.sweepOrphanedStaging()
	if !ce.ensureDiskBudget() {
		// The live staging directory keeps this sample's baseline, so the next sample's usage
		// delta still covers this slot; the objects of this slot are lost.
		ce.drop(dropReasonDiskPressureSkipped, 1, "no disk space for the next sample after evicting every finalised sample")
		ce.events.EmitResult(emitResultSkipped)
		ce.commitSlot(slot, skipped)
		return nil
	}

	if err := ce.writeSample(cs); err != nil {
		// The slot is not consumed: the next tick retries it (F-45).
		ce.events.EmitResult(emitResultError)
		return err
	}
	ce.events.EmitResult(emitResultOK)
	ce.commitSlot(slot, skipped)
	log.Debugf("Emitted sample to Cldy: %d", ce.sampleCt)
	return nil
}

// writeSample writes the current sample and the next sample's baselines, then finalises the
// current sample and queues it. On error the next sample's staging directory is removed and the
// current one is kept, with its baselines, for the retry.
func (ce *Emitter) writeSample(cs *emitter.ClusterSnapshot) (rerr error) {
	if cs.NodeStats == nil {
		return fmt.Errorf("stats data was nil")
	}
	if cs.Kubernetes == nil {
		return fmt.Errorf("k8s snapshot was nil")
	}
	// Something outside the emitter removed the live staging directory. Start it again, without
	// its baselines, rather than failing every Emit.
	if _, err := os.Stat(ce.currentSamplePath); errors.Is(err, fs.ErrNotExist) {
		log.Errorf("Cloudability sample directory %s vanished; recreating it, its baselines are lost", ce.currentSamplePath)
		if err := os.MkdirAll(ce.currentSamplePath, os.ModePerm); err != nil {
			return err
		}
	}

	next := ce.newStagingPath()
	err := os.Mkdir(next, os.ModePerm)
	if err != nil {
		return err
	}
	ce.nextSamplePath = next
	defer func() {
		if rerr != nil {
			ce.discardStaging(next)
			ce.nextSamplePath = ""
		}
	}()

	err = ce.writeStatsData(cs.NodeStats)
	if err != nil {
		return err
	}
	err = ce.writeMetadata(cs.Kubernetes)
	if err != nil {
		return err
	}
	final, manifest, err := finaliseSample(ce.currentSamplePath, *ce.ClusterID, ce.clock(), len(cs.NodeStats.Stats))
	if err != nil {
		return err
	}

	ce.Uploader.AddSample(final)
	ce.sampleCt++
	ce.currentSamplePath = next
	ce.lastSampleBytes = manifest.totalBytes()
	ce.clearShortLivedPods()
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
	if err != nil {
		return err
	}
	return crashPoint(crashSampleFileWritten)
}

func (ce *Emitter) writeMetadata(snapshot *emitter.KubernetesSnapshot) error {
	if snapshot == nil {
		return fmt.Errorf("k8s snapshot was nil")
	}
	ce.unsyncedResources = snapshot.UnsyncedResources
	for name, objs := range metadataToObj(snapshot, ce.pendingShortLivedPods, ce.clock()) {
		err := ce.writeObjects(name, objs)
		if err != nil {
			return err
		}
	}
	return ce.writeAgentFile()
}

// metadataToObj returns the objects to write per file. shortLivedPods are the pending short-lived
// pods, already filtered when drained; the snapshot's own ShortLivedPods are among them.
func metadataToObj(snapshot *emitter.KubernetesSnapshot, shortLivedPods []*v1.Pod, now time.Time) map[string][]runtime.Object {
	// safe buffer to allow for longer lived resources to be ingested correctly
	previousHour := now.UTC().Add(-1 * time.Hour)
	return map[string][]runtime.Object{
		//TODO: add cronjobs
		"nodes":                  checkAndConvertNodes(snapshot.Nodes),
		"pods":                   podObjects(snapshot.Pods, shortLivedPods, previousHour),
		"deployments":            convertObj(snapshot.Deployments, previousHour),
		"replicasets":            convertObj(snapshot.ReplicaSets, previousHour),
		"daemonsets":             convertObj(snapshot.DaemonSets, previousHour),
		"namespaces":             convertObj(snapshot.Namespaces, previousHour),
		"services":               convertObj(snapshot.Services, previousHour),
		"replicationcontrollers": convertObj(snapshot.ReplicationControllers, previousHour),
		"persistentvolumes":      convertObj(snapshot.PersistentVolumes, previousHour),
		"persistentvolumeclaims": convertObj(snapshot.PersistentVolumeClaims, previousHour),
		"statefulsets":           convertObj(snapshot.StatefulSets, previousHour),
		"jobs":                   convertObj(snapshot.Jobs, previousHour),
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

func convertObj[T runtime.Object](objs []T, previousHour time.Time) []runtime.Object {
	var data []runtime.Object
	for _, obj := range objs {
		if shouldSkipResource(previousHour, obj) {
			continue
		}

		data = append(data, obj)
	}
	return data
}

func shouldSkipResource[T runtime.Object](previousHour time.Time, obj T) bool {
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
	return crashPoint(crashSampleFileWritten)
}

func (ce *Emitter) writeAgentFile() (err error) {
	outputPath := ce.currentSamplePath + "agent-measurement.json"
	outFile, err := os.Create(outputPath)
	defer safeClose(outFile.Close, &err)
	if err != nil {
		return err
	}
	now := ce.clock()
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
	// foreground path (default) stats are fetched live per snapshot, so it is effectively ~0.
	// With background collection it is the age of the cached stats and grows while collection
	// fails (F-21).
	var nodeStatsAgeSeconds int64
	if !lastSuccess.IsZero() {
		nodeStatsAgeSeconds = int64(now.Sub(lastSuccess).Seconds())
	}
	values["node_stats_age_seconds"] = strconv.FormatInt(nodeStatsAgeSeconds, 10)

	nodeErrors := unwrapNodeErrors(collectionErr)
	metrics["nodes_failed"] = len(nodeErrors)

	// Resources this sample leaves out because their informers haven't synced (F-12, D9), usually
	// for want of an RBAC permission. Their files are empty because they are unknown.
	if len(ce.unsyncedResources) > 0 {
		metrics["unsynced_resources"] = len(ce.unsyncedResources)
		values["unsynced_resources"] = strings.Join(ce.unsyncedResources, ",")
	}

	agent := agentData{
		Name:    "cldy_agent_status",
		Metrics: metrics,
		Tags: map[string]string{
			"cluster_uid": *ce.ClusterID,
		},
		Ts:     now.UTC().UnixMilli() / 1000,
		Values: values,
		Errors: nodeErrorDetails(nodeErrors),
		Health: ce.statusSummary(),
	}
	agentBytes, err := json.Marshal(agent)
	if err != nil {
		return err
	}
	_, err = outFile.Write(agentBytes)
	if err != nil {
		return err
	}
	return crashPoint(crashSampleFileWritten)
}

type agentData struct {
	Name    string            `json:"name"`
	Metrics map[string]int    `json:"metrics"`
	Tags    map[string]string `json:"tags"`
	Ts      int64             `json:"ts"`
	Values  map[string]string `json:"values"`
	Errors  []errorDetail     `json:"errors,omitempty"`
	// Health is added by chunk 09 (additive: every other field is unchanged).
	Health *telemetry.Summary `json:"agent_health,omitempty"`
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

// newStagingPath returns the staging directory for the sample after the current one, with a
// trailing separator. See sample.go for the staging and finalised layout.
func (ce *Emitter) newStagingPath() string {
	return SafePath(ce.ScratchPath, stagingPrefix+sampleDirName(ce.clock(), ce.sampleCt+1)+"/")
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

// getClusterID returns the default namespace's UID, which is the Cloudability cluster ID for
// compatibility with the legacy metrics-agent. There is no fallback (F-08).
func getClusterID(namespaces []*v1.Namespace) (string, error) {
	for _, ns := range namespaces {
		if ns.Name == "default" {
			if ns.GetUID() == "" {
				return "", fmt.Errorf("the default namespace has no UID, so there is no cluster ID")
			}
			return string(ns.GetUID()), nil
		}
	}
	return "", fmt.Errorf("the default namespace is not in the snapshot, so there is no cluster ID")
}

// emissionSlot reports whether an emission is due and, if so, the slot it fills and how many
// slots before it were missed. A slot is due once 90% of the emission interval has passed since
// the last one. It doesn't advance lastEmission: commitSlot does, once the sample is finalised
// or deliberately skipped (F-45). After a stall the missed slots are skipped rather than emitted
// as a burst (F-32): the first sample's baseline delta already covers the stall's usage.
func (ce *Emitter) emissionSlot() (slot time.Time, skipped int, due bool) {
	now := ce.clock().UTC()
	if ce.emissionInterval <= 0 {
		return now, 0, true
	}
	elapsed := now.Sub(ce.lastEmission)
	if elapsed <= time.Duration(float64(ce.emissionInterval)*.9) {
		return time.Time{}, 0, false
	}
	slots := max(int((elapsed+ce.emissionInterval/10)/ce.emissionInterval), 1)
	return ce.lastEmission.Add(time.Duration(slots) * ce.emissionInterval), slots - 1, true
}

// commitSlot records slot as the last emission.
func (ce *Emitter) commitSlot(slot time.Time, skipped int) {
	ce.lastEmission = slot
	if skipped > 0 {
		log.Infof("Cloudability emitter skipped %d missed emission slots after a stall; the next sample covers their usage", skipped)
		ce.events.EmissionSlotsSkipped(skipped)
	}
}
