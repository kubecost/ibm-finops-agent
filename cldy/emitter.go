package cldy

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ibm/finops-agent/pkg/emitter"
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
	return os.Mkdir(path, 0777)
}

func (ce *Emitter) Emit(cs emitter.ClusterSnapshot) error {
	err := os.Mkdir(ce.nextSamplePath(), 0777)
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
		err = ce.writeStatsFile(true, val.Node.NodeName, data)
		if err != nil {
			return err
		}
		err = ce.writeStatsFile(false, val.Node.NodeName, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ce *Emitter) writeStatsFile(isStats bool, nodeName string, data []byte) error {
	var outputPath string
	if isStats {
		if ce.sampleCt == -1 {
			return nil
		}
		fileName := fmt.Sprintf(statsFileTemplate, stats, nodeName)
		outputPath = ce.currentSamplePath() + fileName
	} else {
		fileName := fmt.Sprintf(statsFileTemplate, baseline, nodeName)
		outputPath = ce.nextSamplePath() + fileName
	}
	file, err := os.Create(outputPath)
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
