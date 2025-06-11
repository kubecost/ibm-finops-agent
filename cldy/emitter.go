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
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"

	"github.com/gogo/protobuf/proto"
	"github.com/ibm/finops-agent/pkg/emitter"

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

type Emitter struct {
	config            EmitterConfig
	startTime         time.Time
	lastEmission      time.Time
	emissionInterval  time.Duration
	sampleCt          int
	currentSamplePath string
	nextSamplePath    string
	Uploader          Uploader
	ClusterID         *string
	ScratchPath       string
}

type EmitterConfig struct {
	UploaderConfig
	EmitAsJson       bool
	ParseMetricData  bool
	EmissionInterval time.Duration
}

const UPLOAD_FREQUENCY = 10

func NewEmitterConfigFromEnv() (EmitterConfig, error) {
	viper.SetEnvPrefix("CLOUDABILITY")
	defer viper.SetEnvPrefix("")
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("HTTPS_CLIENT_TIMEOUT", 60) // Note for readme: In seconds
	viper.SetDefault("UPLOAD_RETRY_COUNT", 5)
	viper.SetDefault("OUTBOUND_PROXY_INSECURE", false)
	viper.SetDefault("UPLOAD_REGION", "us")
	viper.SetDefault("SCRATCH_DIR", "/tmp")
	viper.SetDefault("EMIT_AS_JSON", true)
	viper.SetDefault("PARSE_METRIC_DATA", false)
	viper.SetDefault("EMISSION_INTERVAL", "3m")

	var outboundProxyUrl *url.URL
	proxyURL := viper.GetString("OUTBOUND_PROXY")
	if proxyURL != "" {
		var err error
		outboundProxyUrl, err = url.Parse(proxyURL)
		if err != nil {
			return EmitterConfig{}, fmt.Errorf("failed to parse CLOUDABILITY_OUTBOUND_PROXY")
		}
	}

	return EmitterConfig{
		UploaderConfig: UploaderConfig{
			ApptioConfig: ApptioConfig{
				ClusterName:                  viper.GetString("CLUSTER_NAME"),
				SecretManager:                NewKeyValueSecretManager(viper.GetString("KEY_ACCESS"), viper.GetString("KEY_SECRET")),
				EnvID:                        viper.GetString("ENV_ID"),
				Timeout:                      time.Second * time.Duration(viper.GetInt("HTTPS_CLIENT_TIMEOUT")),
				Retries:                      viper.GetInt("UPLOAD_RETRY_COUNT"),
				ProxyURL:                     outboundProxyUrl,
				ProxyAuth:                    viper.GetString("OUTBOUND_PROXY_AUTH"),
				ProxyInsecure:                viper.GetBool("OUTBOUND_PROXY_INSECURE"),
				Region:                       viper.GetString("UPLOAD_REGION"),
				CustomS3UploadBucket:         viper.GetString("CUSTOM_S3_UPLOAD_BUCKET"),
				CustomS3UploadRegion:         viper.GetString("CUSTOM_S3_UPLOAD_REGION"),
				CustomAzureBlobContainerName: viper.GetString("CUSTOM_AZURE_BLOB_CONTAINER_NAME"),
				CustomAzureBlobUrl:           viper.GetString("CUSTOM_AZURE_BLOB_URL"),
				CustomAzureTenantID:          viper.GetString("CUSTOM_AZURE_BLOB_TENANT_ID"),
				CustomAzureClientID:          viper.GetString("CUSTOM_AZURE_BLOB_CLIENT_ID"),
				CustomAzureClientSecret:      NewValueSecretManager(viper.GetString("CUSTOM_AZURE_BLOB_CLIENT_SECRET")),
			},
			UploadFrequency: time.Minute * time.Duration(UPLOAD_FREQUENCY),
			ScratchDir:      viper.GetString("SCRATCH_DIR"),
		},
		EmitAsJson:       viper.GetBool("EMIT_AS_JSON"),
		ParseMetricData:  viper.GetBool("PARSE_METRIC_DATA"),
		EmissionInterval: viper.GetDuration("EMISSION_INTERVAL"),
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
	}
}

func createIfNotExists(path string) error {
	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, os.ModePerm)
}

func (ce *Emitter) ID() emitter.EmitterID {
	return emitter.CldyEmitterID
}

func (ce *Emitter) Init(cs *emitter.ClusterSnapshot) error {
	log.Infof("Initializing cldy emitter")

	clusterID := getClusterID(cs.Kubernetes.Namespaces)
	ce.ClusterID = &clusterID
	ce.Uploader.SetClusterID(clusterID)

	ce.ScratchPath = ce.config.ScratchDir + "/" + scratchPath + "/" + clusterID
	err := createIfNotExists(ce.ScratchPath)
	if err != nil {
		return fmt.Errorf("failed to create scratch directory: %s", err.Error())
	}

	ce.currentSamplePath = ce.newCurrentSamplePath()
	err = os.Mkdir(ce.currentSamplePath, os.ModePerm)
	if err != nil {
		return err
	}

	err = ce.writeStatsData(cs.NodeStats)
	if err != nil {
		return err
	}

	ce.sampleCt = 0
	return nil
}

func (ce *Emitter) Emit(ctx context.Context, cs *emitter.ClusterSnapshot) error {
	// Emit only after the emission interval has been met
	if ce.shouldDownsample() {
		return nil
	}

	log.Infof("emitting sample to Cldy %d", ce.sampleCt)
	ce.nextSamplePath = ce.newNextSamplePath()
	err := os.Mkdir(ce.nextSamplePath, os.ModePerm)
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
	log.Info("added sample to Cldy")

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

func (ce *Emitter) writeStatsFile(outputPrefix string, nodeName string, data []byte) error {
	if !IsAvailableDiskSpace(uint64(len(data))) {
		err := ce.ClearDayOldScratchSamples()
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

func (ce *Emitter) ClearDayOldScratchSamples() error {
	log.Infof("disk space threshold met. attempting to clear samples over 1 day old.")

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

		if time.Since(fileInfo.ModTime()) > time.Hour*24 {
			err := os.RemoveAll(filePath)
			if err != nil {
				log.Warnf("problem deleting file: %s", err)
			}
			ce.Uploader.RemoveSample(filePath + "/")
		}
	}

	return nil
}

func metadataToObj(snapshot *emitter.KubernetesSnapshot) map[string][]proto.Message {
	return map[string][]proto.Message{
		//TODO: add cronjobs
		"nodes":                  convertObj(snapshot.Nodes),
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

func convertObj[T proto.Message](objs []T) []proto.Message {
	var data []proto.Message
	for _, obj := range objs {
		if shouldSkipResource(obj) {
			continue
		}
		data = append(data, obj)
	}
	return data
}

func shouldSkipResource[T proto.Message](obj T) bool {
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

func (ce *Emitter) writeObjects(name string, data []proto.Message) (err error) {
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
	values["agent_version"] = "TODO"
	values["cluster_name"] = ce.config.ApptioConfig.ClusterName
	if ce.config.ProxyURL != nil {
		values["outbound_proxy_url"] = ce.config.ProxyURL.Path
	}
	values["insecure"] = strconv.FormatBool(ce.config.ApptioConfig.ProxyInsecure)
	values["parse_metrics_data"] = strconv.FormatBool(ce.config.ParseMetricData)
	values["upload_region"] = ce.config.Region
	values["custom_s3_bucket"] = "TODO"
	values["custom_s3_region"] = "TODO"
	metrics["uptime"] = int(now.UTC().Sub(ce.startTime).Seconds())
	agent := agentData{
		Name:    "cldy_agent_status",
		Metrics: metrics,
		Tags: map[string]string{
			"cluster_uid": *ce.ClusterID,
		},
		Ts:     now.UTC().UnixMilli() / 1000,
		Values: values,
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
}

func (ce *Emitter) getSuffix() string {
	if ce.config.EmitAsJson {
		return ".jsonl"
	}
	return ".proto"
}

func (ce *Emitter) newCurrentSamplePath() string {
	return SafePath(ce.ScratchPath, fmt.Sprintf("%d_%d/", time.Now().UTC().UnixMilli(), ce.sampleCt))
}

func (ce *Emitter) newNextSamplePath() string {
	return SafePath(ce.ScratchPath, fmt.Sprintf("%d_%d/", time.Now().UTC().UnixMilli(), ce.sampleCt+1))
}

func (ce *Emitter) marshalObject(object proto.Message) ([]byte, error) {
	if ce.config.EmitAsJson {
		data, err := json.Marshal(object)
		if err != nil {
			return nil, err
		}
		data = append(data, []byte("\n")...)
		return data, nil
	}
	data, err := proto.Marshal(object)
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
