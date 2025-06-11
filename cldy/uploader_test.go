package cldy_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const goodFileName = "8604469a-1368-44ee-9f1c-c5cc8c2121c1_2025-05-05-18-05-17.tgz"

var _ = Describe("Uploader", func() {
	var tempDir string
	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "")
		Expect(err).ToNot(HaveOccurred())
		err = os.Mkdir(tempDir+"/scratch", os.ModePerm)
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		err := os.RemoveAll(tempDir)
		Expect(err).ToNot(HaveOccurred())
	})
	Context("TestBuildTar", func() {
		It("should build Tar", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)

			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())

			uploader.SetClusterID("test_id")
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			actualUploader := uploader.(*cldy.CldyUploader)
			path, err := actualUploader.ConstructPayload(time.Now())
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(path)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
		It("should clean old tars on exceeded disk", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)

			// create existing tar in upload path
			_, err := os.Create(actualUploader.UploadPathDir + "/test.tgz")
			Expect(err).ToNot(HaveOccurred())

			// check number of files in upload path
			files, err := os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// do not remove upload since it is recent
			err = actualUploader.ClearAndRecreateUploadDir()
			Expect(err).ToNot(HaveOccurred())
			files, err = os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// change file mod time to be very old
			filePath := filepath.Join(actualUploader.UploadPathDir, files[0].Name())
			err = os.Chtimes(filePath, time.Now(), time.Date(1, 1, 1, 1, 1, 1, 1, time.Local))
			Expect(err).ToNot(HaveOccurred())

			// purge old upload
			err = actualUploader.ClearAndRecreateUploadDir()
			Expect(err).ToNot(HaveOccurred())

			// check there are no files in the upload path
			files, err = os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 0))
		})
	})
	Context("TestTarCleanup", func() {
		It("should cleanup Tar", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
					EnvID:         "1",
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)

			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())

			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			mockService := cldy.ApptioServiceImpl{}
			actualUploader.StorageServices[0] = &mockService
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(time.Second)
			fileInfo, err := os.Stat(tempDir + "/upload")
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestStartupRecovery", func() {
		It("should recover complete sample", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				// 100 years (should recover all samples)
				RecoveryPeriod: 1000000 * time.Hour,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
				},
			}
			// copy over data before creating uploader simulating recovery state
			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(1))
			Expect(actualUploader.RecoveredUploads).To(Equal(1))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
		It("should recover sample but not upload when outside recovery range", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				// 1 hour (will not recover as agent-measurement timestamp is old)
				RecoveryPeriod: 1 * time.Hour,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
				},
			}
			// copy over data before creating uploader simulating recovery state
			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(1))
			Expect(actualUploader.RecoveredUploads).To(Equal(0))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
		It("should not recover incomplete sample", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				// 100 years (should recover all samples if complete)
				RecoveryPeriod: 1000000 * time.Hour,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
				},
			}
			// copy over data before creating uploader simulating recovery state
			err := copyIncompleteData(tempDir+"/scratch/temp_test_data", "testdata", []string{"deployments.jsonl"})
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(0))
			Expect(actualUploader.RecoveredUploads).To(Equal(0))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
		It("should recover multiple complete samples and ignore 1 incomplete sample", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				// 100 years (should recover all samples)
				RecoveryPeriod: 1000000 * time.Hour,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
				},
			}
			// copy over data before creating uploader simulating recovery state
			err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
			Expect(err).ToNot(HaveOccurred())
			// still valid test data, just with only 1 node file
			err = copyIncompleteData(tempDir+"/scratch/temp_test_data_1", "testdata", []string{"stats-summary-nodename2.json", "stats-summary-nodename3.json", "stats-summary-nodename4.json"})
			Expect(err).ToNot(HaveOccurred())
			err = updateAgentTimestamp(tempDir+"/scratch/temp_test_data_1/agent-measurement.json", 1743499000)
			Expect(err).ToNot(HaveOccurred())
			// invalid data set
			err = copyIncompleteData(tempDir+"/scratch/temp_test_data_2", "testdata", []string{"deployments.jsonl"})
			Expect(err).ToNot(HaveOccurred())
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("123456-1234-1234-123456789012")
			actualUploader := uploader.(*cldy.CldyUploader)
			Expect(actualUploader.RecoveredSamples).To(Equal(2))
			Expect(actualUploader.RecoveredUploads).To(Equal(2))
			checkScratchEmpty(tempDir + "/scratch")

			// copy over another sample and ensure recovery does not break happy path
			checkCollectionAndConstruction(tempDir, uploader, actualUploader)
		})
	})
	Context("TestUpload", func() {
		It("should upload", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
					EnvID:         "1",
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mockClientService{},
				SecretManager:    cldy.NewKeyValueSecretManager("bad-key", ""),
			}
			actualUploader.StorageServices[0] = &service
			payload := cldy.UploadPayload{
				ClusterUID:   "bad-cluster",
				FileName:     "temp_test_data",
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			// upload with bad froontdoor credentials
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("frontdoor service login call failed"))
			service.SecretManager = cldy.NewKeyValueSecretManager("good-key", "")
			// upload with good key but bad clusterUID
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cloudability clusters/upload request call failed with status"))
			// upload with successful login and successful url generation
			payload.ClusterUID = "good-cluster"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should only login once", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: 250 * time.Millisecond,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
					EnvID:         "1",
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")

			actualUploader := uploader.(*cldy.CldyUploader)
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("good-key", ""),
			}
			actualUploader.StorageServices[0] = &service

			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})

		It("should log back in if required", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: 250 * time.Millisecond,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
					EnvID:         "1",
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")

			actualUploader := uploader.(*cldy.CldyUploader)
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("short-lived-token", ""),
			}
			actualUploader.StorageServices[0] = &service

			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(2))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})
		It("should upload to custom s3 bucket", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager: cldy.NewKeyValueSecretManager("", ""),
					EnvID:         "1",
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			uploadClient := &mockS3UploadService{}
			service := cldy.CustomS3Client{
				UploadClient: uploadClient,
			}
			actualUploader.StorageServices[0] = &service

			// Succeed on a good filename
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(uploadClient.UploadedSampleName).To(Equal("production/data/metrics-agent/2025/05/05/good-cluster/good-cluster-20250505-18-05.tgz"))

			// Error on an unparseable filename
			payload.FileName = "badFileName"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("error parsing name from sample filename"))
		})
		It("should upload to custom azure blob", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
				ApptioConfig: cldy.ApptioConfig{
					SecretManager:                cldy.NewKeyValueSecretManager("", ""),
					EnvID:                        "1",
					CustomAzureBlobContainerName: "a",
					CustomAzureBlobUrl:           "testurl",
					CustomAzureTenantID:          "1",
					CustomAzureClientID:          "1",
					CustomAzureClientSecret:      cldy.NewValueSecretManager("1"),
				},
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			uploadClient := &MockBlobUploadService{}
			service := cldy.CustomBlobClient{
				UploadClient: uploadClient,
			}
			actualUploader.StorageServices[0] = &service

			// Succeed on a good filename
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(uploadClient.UploadedSampleName).To(Equal("production/data/metrics-agent/2025/05/05/good-cluster/good-cluster-20250505-18-05.tgz"))

			// Error on an unparseable filename
			payload.FileName = "badFileName"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("error parsing name from sample filename"))
		})
	})
})

// copies the entire directory
func copyCompleteData(destination, source string) error {
	return os.CopyFS(destination, os.DirFS(source))
}

// copies directory set and removes files in provided list for incomplete data set testing purposes
func copyIncompleteData(destination, source string, filesToRemove []string) error {
	err := os.CopyFS(destination, os.DirFS(source))
	if err != nil {
		return err
	}
	for _, file := range filesToRemove {
		fErr := os.Remove(destination + "/" + file)
		if fErr != nil {
			return fErr
		}
	}
	return nil
}

// sample timestamps need to be unique otherwise .tgz file names will be the same and cause overwrite which would
// never occur in real data collection/uploading
func updateAgentTimestamp(filePath string, ts int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}
	measure := testAgentMeasure{}
	err = json.Unmarshal(data, &measure)
	if err != nil {
		return err
	}
	measure.Timestamp = ts
	jsonInfo, err := json.Marshal(&measure)
	if err != nil {
		return err
	}
	return os.WriteFile(filePath, jsonInfo, 0644)
}

type testAgentMeasure struct {
	Timestamp int64  `json:"ts"`
	Name      string `json:"name"`
}

func checkCollectionAndConstruction(tempDir string, uploader cldy.Uploader, actualUploader *cldy.CldyUploader) {
	err := copyCompleteData(tempDir+"/scratch/temp_test_data", "testdata")
	Expect(err).ToNot(HaveOccurred())
	uploader.AddSample(tempDir + "/scratch/temp_test_data")

	path, err := actualUploader.ConstructPayload(time.Now())
	Expect(err).ToNot(HaveOccurred())
	fileInfo, err := os.Stat(path)
	Expect(err).ToNot(HaveOccurred())
	Expect(fileInfo.Size()).To(BeNumerically(">", 0))
}

func checkScratchEmpty(dir string) {
	f, err := os.Open(dir)
	Expect(err).To(Not(HaveOccurred()))
	defer f.Close()
	_, err = f.Readdir(1)
	Expect(err).To(BeEquivalentTo(io.EOF))
}

type mockClientService struct {
	countByPath map[string]int
}

type mockfrontdoorRequestBody struct {
	KeyAccess string `json:"KeyAccess"`
	KeySecret string `json:"KeySecret"`
}

type mockGetURLRequestBody struct {
	ClusterUID   string `json:"ClusterUID"`
	FileName     string `json:"FileName"`
	AgentVersion string `json:"AgentVersion"`
	UploadHash   string `json:"UploadHash"`
}

func (mcs *mockClientService) Do(r *http.Request, _ string) (res *http.Response, err error) {
	if mcs.countByPath == nil {
		mcs.countByPath = map[string]int{}
	}
	mcs.countByPath[r.URL.Path] += 1
	// request to login to Frontdoor
	if strings.Contains(r.URL.Path, "apikeylogin") {
		var body mockfrontdoorRequestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.KeyAccess == "bad-key" {
			return &http.Response{StatusCode: 403, Body: r.Body}, nil
		} else {
			resp := http.Response{StatusCode: 200, Body: r.Body, Header: http.Header{}}
			resp.Header.Set("Apptio-Opentoken", "happytoken")
			if body.KeyAccess == "short-lived-token" {
				resp.Header.Set("valid_till", strconv.FormatInt(time.Now().UnixMilli(), 10))
			} else {
				resp.Header.Set("valid_till", strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10))
			}
			return &resp, nil
		}
	}
	// request to acquire s3 url from Cloudability
	if strings.Contains(r.URL.Path, "clusters/upload") {
		var body mockGetURLRequestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.ClusterUID == "bad-cluster" {
			return &http.Response{StatusCode: 400, Body: r.Body}, nil
		} else {
			responseBody, _ := json.Marshal(cldy.CloudabilityClustersUploadResponse{
				Result: cldy.CloudabilityClustersUploadInfo{
					Location: "somewhere/valid-location",
				}})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(responseBody))}, nil
		}
	}
	// request to upload data to S3
	if strings.Contains(r.URL.Path, "valid-location") {
		hash := md5.New()
		// cannot close body here as it will close the underlying file
		if _, err = io.Copy(hash, r.Body); err != nil {
			return &http.Response{}, fmt.Errorf("invalid body: %s", err.Error())
		}
		fileHash := base64.StdEncoding.EncodeToString(hash.Sum(nil))
		if fileHash != r.Header.Get("Content-MD5") {
			return &http.Response{}, fmt.Errorf("invalid hash: calculated %s, expected %s",
				fileHash, r.Header.Get("Content-MD5"))
		}
		return &http.Response{StatusCode: 200, Body: r.Body, Header: http.Header{}}, nil
	}
	return &http.Response{}, fmt.Errorf("unknown request")
}

type mockS3UploadService struct {
	UploadedSampleName string
}

func (mcs *mockS3UploadService) Do(sampleToUpload *s3manager.UploadInput) error {
	if sampleToUpload.Body == nil {
		return fmt.Errorf("No sample detected")
	}

	mcs.UploadedSampleName = *sampleToUpload.Key
	return nil
}

type MockBlobUploadService struct {
	UploadedSampleName string
}

func (mcs *MockBlobUploadService) Do(sampleToUpload *cldy.BlobUploadInput) error {
	if sampleToUpload.Body == nil {
		return fmt.Errorf("No sample detected")
	}

	mcs.UploadedSampleName = sampleToUpload.BlobName
	return nil
}
