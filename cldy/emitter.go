package cldy

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	url "net/url"
	"os"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ibm/finops-agent/pkg/emitter"

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
	config          EmitterConfig
	startTimePrefix string
	sampleCt        int
	Uploader        Uploader
	ClusterID       *string
}

type EmitterConfig struct {
	UploaderConfig
	EmitAsJson bool
}

func NewEmitterConfigFromEnv() EmitterConfig {
	viper.SetEnvPrefix("CLOUDABILITY")
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("HTTPS_CLIENT_TIMEOUT", 60) // Note for readme: In seconds
	viper.SetDefault("UPLOAD_RETRY_COUNT", 5)
	viper.SetDefault("UPLOAD_REGION", "us")
	viper.SetDefault("UPLOAD_FREQUENCY", 1) // Note for readme: In minutes
	viper.SetDefault("SCRATCH_DIR", "")
	viper.SetDefault("EMIT_AS_JSON", true)

	// Check existence of required fields
	keyAccess := viper.GetString("KEY_ACCESS")
	if keyAccess == "" {
		log.Fatalf("CLOUDABILITY_KEY_ACCESS is required")
	}
	keySecret := viper.GetString("KEY_SECRET")
	if keySecret == "" {
		log.Fatalf("CLOUDABILITY_KEY_SECRET is required")
	}
	envID := viper.GetString("ENV_ID")
	if envID == "" {
		log.Fatalf("CLOUDABILITY_ENV_ID is required")
	}

	url, err := url.Parse(viper.GetString("OUTBOUND_PROXY"))
	if err != nil {
		fmt.Errorf("failed to parse CLOUDABILITY_OUTBOUND_PROXY")
	}

	return EmitterConfig{
		UploaderConfig: UploaderConfig{
			ApptioConfig: ApptioConfig{
				KeyAccess:     viper.GetString("KEY_ACCESS"),
				KeySecret:     viper.GetString("KEY_SECRET"),
				EnvID:         viper.GetString("ENV_ID"),
				Timeout:       time.Second * time.Duration(viper.GetInt("HTTPS_CLIENT_TIMEOUT")),
				Retries:       viper.GetInt("UPLOAD_RETRY_COUNT"),
				ProxyURL:      url,
				ProxyAuth:     viper.GetString("OUTBOUND_PROXY_AUTH"),
				ProxyInsecure: viper.GetBool("OUTBOUND_PROXY_INSECURE"),
				Region:        viper.GetString("UPLOAD_REGION"),
			},
			UploadFrequency: time.Minute * time.Duration(viper.GetInt("UPLOAD_FREQUENCY")),
			ScratchDir:      viper.GetString("SCRATCH_DIR"),
		},
		EmitAsJson: viper.GetBool("EMIT_AS_JSON"),
	}
}

func NewEmitter(config EmitterConfig, stop chan struct{}) emitter.Emitter {
	err := createIfNotExists(config.ScratchDir + "/" + scratchPath)
	if err != nil {
		panic("failed to create scratch directory: " + err.Error())
	}
	// TODO: evaluate whether or not to check scratch dir for completed samples
	// TODO: cleanup old samples (> 72 hrs?)
	return &Emitter{
		config:          config,
		Uploader:        NewCldyUploader(config.UploaderConfig, stop),
		sampleCt:        initialSampleCt,
		startTimePrefix: strconv.Itoa(int(time.Now().UTC().UnixMilli())),
	}
}

func createIfNotExists(path string) error {
	_, err := os.Stat(path)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Mkdir(path, os.ModePerm)
}

func (ce *Emitter) ID() emitter.EmitterID {
	return emitter.CldyEmitterID
}

func (ce *Emitter) Init(snapshot *emitter.ClusterSnapshot) error {
	// TODO: Implement any initialization logic needed for the emitter. This will run once on Start(),
	// TODO: before any Emit() calls.
	return nil
}

func (ce *Emitter) Emit(ctx context.Context, cs *emitter.ClusterSnapshot) error {
	err := os.Mkdir(ce.nextSamplePath(), os.ModePerm)
	if err != nil {
		return err
	}
	if ce.ClusterID == nil {
		clusterID := getClusterID(cs.Kubernetes.Namespaces)
		ce.ClusterID = &clusterID
		ce.Uploader.SetClusterID(clusterID)
	}

	err = ce.writeStatsData(cs.NodeStats)
	if err != nil {
		return err
	}
	if ce.sampleCt == initialSampleCt {
		ce.sampleCt = 0
		return nil
	}
	err = ce.writeMetadata(cs.Kubernetes)
	if err != nil {
		return err
	}

	ce.Uploader.AddSample(ce.currentSamplePath())
	ce.sampleCt++
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
	var fileName string
	if outputPrefix == stats {
		if ce.sampleCt == -1 {
			return nil
		}
		fileName = ce.currentSamplePath()
	} else {
		fileName = ce.nextSamplePath()
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

func metadataToObj(snapshot *emitter.KubernetesSnapshot) map[string][]proto.Message {
	return map[string][]proto.Message{
		//TODO: add cronjobs
		"nodes":                  convertObj(snapshot.Nodes),
		"pods":                   convertObj(snapshot.Pods),
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
	data := make([]proto.Message, len(objs), len(objs))
	for i, obj := range objs {
		data[i] = obj
	}
	return data
}

func (ce *Emitter) writeObjects(name string, data []proto.Message) (err error) {
	outputPath := ce.currentSamplePath() + name + ce.getSuffix()
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
	outputPath := ce.currentSamplePath() + "agent-measurement.json"
	outFile, err := os.Create(outputPath)
	defer safeClose(outFile.Close, &err)
	if err != nil {
		return err
	}
	agent := agentData{
		Name: "cldy_agent_status",
		// put uptime in here
		Metrics: nil,
		Tags: map[string]string{
			"cluster_uid": *ce.ClusterID,
		},
		Ts:     time.Now().UTC().UnixMilli() / 1000,
		Values: nil,
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

func (ce *Emitter) currentSamplePath() string {
	return ce.config.ScratchDir + "/" + scratchPath + "/" + ce.startTimePrefix + "_" + strconv.Itoa(ce.sampleCt) + "/"
}

func (ce *Emitter) nextSamplePath() string {
	return ce.config.ScratchDir + "/" + scratchPath + "/" + ce.startTimePrefix + "_" + strconv.Itoa(ce.sampleCt+1) + "/"
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
