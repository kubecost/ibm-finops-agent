package cldy_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ibm/finops-agent/cldy"

	"github.com/ibm/finops-agent/pkg/emitter"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

var _ = Describe("Emitter", func() {
	Context("TestLoadData", func() {
		It("should load data", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).NotTo(HaveOccurred())
			defer safeRemove(tempDir)
			config := cldy.EmitterConfig{
				UploaderConfig: cldy.UploaderConfig{
					ScratchDir: tempDir,
					ApptioConfig: cldy.ApptioConfig{
						SecretManager: cldy.NewKeyValueSecretManager("", ""),
						EnvID:         "1",
					},
				},
			}
			cldyEmitter := cldy.NewEmitter(config, make(chan struct{}), nil)
			actualEmitter := cldyEmitter.(*cldy.Emitter)

			mockUpload := mockUploader{data: []string{}}

			data, err := buildTestData()
			Expect(err).NotTo(HaveOccurred())
			err = cldyEmitter.Init(data)
			Expect(err).NotTo(HaveOccurred())
			actualEmitter.Uploader = &mockUpload
			err = cldyEmitter.Emit(context.TODO(), data)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(mockUpload.data)).To(Equal(1))

			expectedData := []string{
				"agent-measurement.json",
				"baseline-summary-nodename1.json",
				"baseline-summary-nodename2.json",
				"baseline-summary-nodename3.json",
				"baseline-summary-nodename4.json",
				"daemonsets.proto",
				"deployments.proto",
				"jobs.proto",
				"namespaces.proto",
				"nodes.proto",
				"persistentvolumeclaims.proto",
				"persistentvolumes.proto",
				"pods.proto",
				"replicasets.proto",
				"replicationcontrollers.proto",
				"services.proto",
				"statefulsets.proto",
				"stats-summary-nodename1.json",
				"stats-summary-nodename2.json",
				"stats-summary-nodename3.json",
				"stats-summary-nodename4.json",
			}
			seenFiles := map[string]struct{}{}
			err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					parts := strings.Split(path, "/")
					name := parts[len(parts)-1]
					seenFiles[name] = struct{}{}
				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			for _, path := range expectedData {
				Expect(seenFiles).To(HaveKey(path))
			}
		})
		It("should load data as JSON and skip un-allocatable resources", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).NotTo(HaveOccurred())
			defer safeRemove(tempDir)
			config := cldy.EmitterConfig{
				UploaderConfig: cldy.UploaderConfig{
					ScratchDir: tempDir,
					ApptioConfig: cldy.ApptioConfig{
						SecretManager: cldy.NewKeyValueSecretManager("", ""),
						EnvID:         "1",
					}},
				EmitAsJson: true,
			}
			cldyEmitter := cldy.NewEmitter(config, make(chan struct{}), nil)
			actualEmitter := cldyEmitter.(*cldy.Emitter)

			mockUpload := mockUploader{data: []string{}}

			data, err := buildTestData()
			Expect(err).NotTo(HaveOccurred())
			err = cldyEmitter.Init(data)
			Expect(err).NotTo(HaveOccurred())
			actualEmitter.Uploader = &mockUpload

			err = cldyEmitter.Emit(context.TODO(), data)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(mockUpload.data)).To(Equal(1))
			expectedData := []string{
				"agent-measurement.json",
				"baseline-summary-nodename1.json",
				"baseline-summary-nodename2.json",
				"baseline-summary-nodename3.json",
				"baseline-summary-nodename4.json",
				"daemonsets.jsonl",
				"deployments.jsonl",
				"jobs.jsonl",
				"namespaces.jsonl",
				"nodes.jsonl",
				"persistentvolumeclaims.jsonl",
				"persistentvolumes.jsonl",
				"pods.jsonl",
				"replicasets.jsonl",
				"replicationcontrollers.jsonl",
				"services.jsonl",
				"statefulsets.jsonl",
				"stats-summary-nodename1.json",
				"stats-summary-nodename2.json",
				"stats-summary-nodename3.json",
				"stats-summary-nodename4.json",
			}
			seenFiles := map[string]struct{}{}
			err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
				if !info.IsDir() {
					switch {
					case strings.Contains(path, "replicasets"):
						err := checkForDeadReplicaSets(path)
						Expect(err).NotTo(HaveOccurred())
					case strings.Contains(path, "pods"):
						err := checkForDeadPods(path)
						Expect(err).NotTo(HaveOccurred())
					case strings.Contains(path, "jobs"):
						err := checkForDeadJobs(path)
						Expect(err).NotTo(HaveOccurred())
					}
					parts := strings.Split(path, "/")
					name := parts[len(parts)-1]
					seenFiles[name] = struct{}{}

				}
				return nil
			})
			Expect(err).NotTo(HaveOccurred())
			for _, path := range expectedData {
				Expect(seenFiles).To(HaveKey(path))
			}
		})
	})
	Context("Emission", func() {
		// Note: This test operates on a timer, so it could fail in a scenario where its execution
		// is halted or slowed
		It("should emit each time emission interval is satisifed", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).NotTo(HaveOccurred())
			defer safeRemove(tempDir)
			config := cldy.EmitterConfig{
				UploaderConfig: cldy.UploaderConfig{
					ScratchDir: tempDir,
					ApptioConfig: cldy.ApptioConfig{
						SecretManager: cldy.NewKeyValueSecretManager("", ""),
					},
				},
				EmissionInterval: time.Duration(200) * time.Millisecond,
			}
			cldyEmitter := cldy.NewEmitter(config, make(chan struct{}), nil)
			actualEmitter := cldyEmitter.(*cldy.Emitter)

			mockUpload := mockUploader{data: []string{}}

			data, err := buildTestData()
			Expect(err).NotTo(HaveOccurred())
			err = cldyEmitter.Init(data)
			Expect(err).NotTo(HaveOccurred())
			actualEmitter.Uploader = &mockUpload

			// Should not emit before interval has been satisfied (<200 milliseconds)
			err = cldyEmitter.Emit(context.TODO(), data)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(mockUpload.data)).To(Equal(0))

			time.Sleep(time.Duration(200) * time.Millisecond)
			// Should emit after interval has been satisfied (>=200 milliseconds)
			err = cldyEmitter.Emit(context.TODO(), data)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(mockUpload.data)).To(Equal(1))
		})
		It("should clean old scratch samples on exceeded disk", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).NotTo(HaveOccurred())
			defer safeRemove(tempDir)
			config := cldy.EmitterConfig{
				UploaderConfig: cldy.UploaderConfig{
					ScratchDir: tempDir,
					ApptioConfig: cldy.ApptioConfig{
						SecretManager: cldy.NewKeyValueSecretManager("", ""),
					},
				},
			}
			cldyEmitter := cldy.NewEmitter(config, make(chan struct{}), nil)
			actualEmitter := cldyEmitter.(*cldy.Emitter)

			data, err := buildTestData()
			Expect(err).NotTo(HaveOccurred())
			err = cldyEmitter.Init(data)
			Expect(err).NotTo(HaveOccurred())

			// check number of files in upload path
			files, err := os.ReadDir(actualEmitter.ScratchPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// don't clear sample since it's recent
			err = actualEmitter.ClearOldScratchSamples()
			Expect(err).ToNot(HaveOccurred())
			files, err = os.ReadDir(actualEmitter.ScratchPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// change file mod time to be very old
			filePath := filepath.Join(actualEmitter.ScratchPath, files[0].Name())
			err = os.Chtimes(filePath, time.Now(), time.Date(1, 1, 1, 1, 1, 1, 1, time.Local))
			Expect(err).ToNot(HaveOccurred())

			// clear samples
			err = actualEmitter.ClearOldScratchSamples()
			Expect(err).ToNot(HaveOccurred())

			// check there are no files in the upload path
			files, err = os.ReadDir(actualEmitter.ScratchPath)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 0))
		})
	})
	Context("Config", func() {
		It("should load defaults", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).ToNot(HaveOccurred())

			defer safeRemove(tempDir)
			config, err := cldy.NewEmitterConfigFromEnv()
			Expect(err).ToNot(HaveOccurred())

			Expect(config.ParseMetricData).To(BeFalse())
			Expect(config.UploaderConfig.ScratchDir).To(Equal("/opt/finops-agent"))
			Expect(config.UploaderConfig.ApptioConfig.Region).To(Equal("us"))
		})
		It("should load and parse custom outbound config", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).ToNot(HaveOccurred())

			t := GinkgoT()
			t.Setenv("CLOUDABILITY_OUTBOUND_PROXY", "1.1.1.1")

			defer safeRemove(tempDir)
			config, err := cldy.NewEmitterConfigFromEnv()
			Expect(err).ToNot(HaveOccurred())

			Expect(config.UploaderConfig.ApptioConfig.ProxyURL.Path).To(Equal("1.1.1.1"))
		})
		It("should load api key from env", func() {
			t := GinkgoT()
			t.Setenv("CLOUDABILITY_API_KEY", "goodkey123")

			config, err := cldy.NewEmitterConfigFromEnv()
			Expect(err).ToNot(HaveOccurred())

			apiKey, err := config.UploaderConfig.ApptioConfig.APIKeySecretManager.GetSecret()
			Expect(err).ToNot(HaveOccurred())
			Expect(string(apiKey)).To(Equal("goodkey123"))
		})
		It("should throw error on improper outbound format", func() {
			tempDir, err := os.MkdirTemp("", "")
			Expect(err).ToNot(HaveOccurred())

			t := GinkgoT()
			t.Setenv("CLOUDABILITY_OUTBOUND_PROXY", "2.2.2.2:2")

			defer safeRemove(tempDir)
			_, err = cldy.NewEmitterConfigFromEnv()
			Expect(err).To(HaveOccurred())
		})
	})
})

type mockUploader struct {
	data      []string
	clusterID string
}

func (m *mockUploader) SetClusterID(id string) {
	m.clusterID = id
}

func (m *mockUploader) AddSample(sample string) {
	m.data = append(m.data, sample)
}

func (m *mockUploader) RemoveSample(sample string) {}

// ensure replicaSets with zero replicas are not emitted
func checkForDeadReplicaSets(path string) error {
	var rs *appsv1.ReplicaSet
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer safeClose(file.Close)
	decoder := json.NewDecoder(file)
	for decoder.More() {
		err = decoder.Decode(&rs)
		if err != nil {
			return err
		}
		// ensure replicaset with zero replicas is not present in emitted data
		Expect(rs.Name).ToNot(Equal("dead-cloudability-metrics-agent-9b5b46685"))
	}
	return nil
}

// ensure pods that are not running are not emitted
func checkForDeadPods(path string) error {
	var pod *v1.Pod
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer safeClose(file.Close)
	decoder := json.NewDecoder(file)
	for decoder.More() {
		err = decoder.Decode(&pod)
		if err != nil {
			return err
		}
		// ensure terminated pod is not emitted
		Expect(pod.Name).ToNot(Equal("completed-cloudability-metrics-agent-2-84775d78df-9qh4d"))
	}
	return nil
}

// ensure jobs completed for over an hour are not emitted
func checkForDeadJobs(path string) error {
	var job *batchv1.Job
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer safeClose(file.Close)
	decoder := json.NewDecoder(file)
	for decoder.More() {
		err = decoder.Decode(&job)
		if err != nil {
			return err
		}
		// ensure old, completed job is not emitted
		Expect(job.Name).ToNot(Equal("my-dead-job"))
	}
	return nil
}

func buildTestData() (*emitter.ClusterSnapshot, error) {
	metadata := emitter.KubernetesSnapshot{}
	nodeStats := emitter.NodeStatsSummary{}
	snapshot := &emitter.ClusterSnapshot{
		Kubernetes: &metadata,
		NodeStats:  &nodeStats,
	}
	errs := make([]error, 20)
	metadata.Nodes, errs[0] = loadNodes()
	metadata.Deployments, errs[1] = loadDeployments()
	metadata.ReplicaSets, errs[2] = loadReplicaSets()
	metadata.DaemonSets, errs[3] = loadDaemonSets()
	metadata.Pods, errs[4] = loadPods()
	metadata.Namespaces, errs[5] = loadNamespaces()
	metadata.Jobs, errs[6] = loadJobs()
	metadata.PersistentVolumes, errs[7] = loadPVs()
	metadata.PersistentVolumeClaims, errs[8] = loadPVCs()
	metadata.ReplicationControllers, errs[9] = loadReplicationControllers()
	metadata.Services, errs[10] = loadServices()
	metadata.StatefulSets, errs[11] = loadStatefulSets()

	nodeStats.Stats, errs[12] = loadStats()

	for _, err := range errs {
		if err != nil {
			return snapshot, err
		}
	}

	return snapshot, nil
}

func loadNodes() ([]*v1.Node, error) {
	file, err := os.Open("testdata/nodes.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var nodes []*v1.Node
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var node v1.Node
		err := decoder.Decode(&node)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, &node)
	}
	return nodes, nil
}

func loadDeployments() ([]*appsv1.Deployment, error) {
	file, err := os.Open("testdata/deployments.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*appsv1.Deployment
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object appsv1.Deployment
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadReplicaSets() ([]*appsv1.ReplicaSet, error) {
	file, err := os.Open("testdata/replicasets.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*appsv1.ReplicaSet
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object appsv1.ReplicaSet
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadDaemonSets() ([]*appsv1.DaemonSet, error) {
	file, err := os.Open("testdata/daemonsets.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*appsv1.DaemonSet
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object appsv1.DaemonSet
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadPods() ([]*v1.Pod, error) {
	file, err := os.Open("testdata/pods.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*v1.Pod
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object v1.Pod
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadNamespaces() ([]*v1.Namespace, error) {
	file, err := os.Open("testdata/namespaces.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*v1.Namespace
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object v1.Namespace
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadJobs() ([]*batchv1.Job, error) {
	file, err := os.Open("testdata/jobs.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*batchv1.Job
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object batchv1.Job
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadServices() ([]*v1.Service, error) {
	file, err := os.Open("testdata/services.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*v1.Service
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object v1.Service
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadReplicationControllers() ([]*v1.ReplicationController, error) {
	file, err := os.Open("testdata/replicationcontrollers.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*v1.ReplicationController
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object v1.ReplicationController
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadPVs() ([]*v1.PersistentVolume, error) {
	file, err := os.Open("testdata/persistentvolumes.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*v1.PersistentVolume
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object v1.PersistentVolume
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadPVCs() ([]*v1.PersistentVolumeClaim, error) {
	file, err := os.Open("testdata/persistentvolumeclaims.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*v1.PersistentVolumeClaim
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object v1.PersistentVolumeClaim
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadStatefulSets() ([]*appsv1.StatefulSet, error) {
	file, err := os.Open("testdata/statefulsets.jsonl")
	if err != nil {
		return nil, err
	}
	defer safeClose(file.Close)
	var objects []*appsv1.StatefulSet
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var object appsv1.StatefulSet
		err := decoder.Decode(&object)
		if err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

func loadStats() ([]*stats.Summary, error) {
	var data []*stats.Summary
	for i := 1; i <= 4; i++ {
		file, err := os.Open(fmt.Sprintf("testdata/stats-summary-nodename%d.json", i))
		if err != nil {
			return nil, err
		}
		defer safeClose(file.Close)
		decoder := json.NewDecoder(file)
		obj := &stats.Summary{}
		err = decoder.Decode(obj)
		if err != nil {
			return data, err
		}
		data = append(data, obj)
	}
	return data, nil
}
