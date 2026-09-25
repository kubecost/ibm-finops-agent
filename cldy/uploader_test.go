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
	var scratch *prodScratch
	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "")
		Expect(err).ToNot(HaveOccurred())
		scratch = newProdScratch(GinkgoT(), tempDir, "test_id")
	})
	AfterEach(func() {
		err := os.RemoveAll(tempDir)
		Expect(err).ToNot(HaveOccurred())
	})
	Context("TestBuildTar", func() {
		It("should build Tar", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)

			sample := scratch.AddCompleteSample(GinkgoT(), time.Now(), 0)

			uploader.SetClusterID("test_id")
			uploader.AddSample(sample)
			actualUploader := uploader.(*cldy.CldyUploader)
			path, err := actualUploader.ConstructPayload(time.Now())
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(path)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
		It("should clean old tars on exceeded disk", func() {
			config := defaultConfig(tempDir)
			config.RecoveryPeriod = time.Hour

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
			err = actualUploader.ClearOldUploadSamples()
			Expect(err).ToNot(HaveOccurred())
			files, err = os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 1))

			// change file mod time to be very old
			filePath := filepath.Join(actualUploader.UploadPathDir, files[0].Name())
			err = os.Chtimes(filePath, time.Now(), time.Date(1, 1, 1, 1, 1, 1, 1, time.Local))
			Expect(err).ToNot(HaveOccurred())

			// purge old upload
			err = actualUploader.ClearOldUploadSamples()
			Expect(err).ToNot(HaveOccurred())

			// check there are no files in the upload path
			files, err = os.ReadDir(actualUploader.UploadPathDir)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(files)).To(BeNumerically("==", 0))
		})
	})
	Context("TestTarCleanup", func() {
		It("should cleanup Tar", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)

			sample := scratch.AddCompleteSample(GinkgoT(), time.Now(), 0)

			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			mockService := cldy.ApptioServiceImpl{}
			actualUploader.StorageServices = append(actualUploader.StorageServices, &mockService)
			uploader.AddSample(sample)
			time.Sleep(time.Second)
			fileInfo, err := os.Stat(tempDir + "/upload")
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestUpload", func() {
		It("should upload", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			sample := scratch.AddCompleteSample(GinkgoT(), time.Now(), 0)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mockClientService{},
				SecretManager:    cldy.NewKeyValueSecretManager("bad-key", ""),
			}
			actualUploader.StorageServices = append(actualUploader.StorageServices, &service)
			payload := cldy.UploadPayload{
				ClusterUID:   "bad-cluster",
				FileName:     "temp_test_data",
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     sample + "daemonsets.jsonl",
			}
			// upload with bad froontdoor credentials
			err := actualUploader.StorageServices[0].Upload(payload)
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
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("good-key", ""),
			}
			// Drive upload cycles directly rather than racing uploadLoop's ticker.
			uploader := cldy.NewUploaderForTest(defaultConfig(tempDir), []cldy.StorageService{&service}, nil)
			uploader.SetClusterID("test_id")

			uploader.AddSample(scratch.AddCompleteSample(GinkgoT(), time.Now(), 0))
			uploader.UploadCycleForTest()
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			// a second sample; the pause lets a short-lived token (valid until its login time) expire
			uploader.AddSample(scratch.AddCompleteSample(GinkgoT(), time.Now().Add(time.Second), 1))
			time.Sleep(10 * time.Millisecond)
			uploader.UploadCycleForTest()
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})

		It("should log back in if required", func() {
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("short-lived-token", ""),
			}
			// Drive upload cycles directly rather than racing uploadLoop's ticker.
			uploader := cldy.NewUploaderForTest(defaultConfig(tempDir), []cldy.StorageService{&service}, nil)
			uploader.SetClusterID("test_id")

			uploader.AddSample(scratch.AddCompleteSample(GinkgoT(), time.Now(), 0))
			uploader.UploadCycleForTest()
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			// a second sample; the pause lets a short-lived token (valid until its login time) expire
			uploader.AddSample(scratch.AddCompleteSample(GinkgoT(), time.Now().Add(time.Second), 1))
			time.Sleep(10 * time.Millisecond)
			uploader.UploadCycleForTest()
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(2))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})
		It("should upload via metrics-collector api key", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			sample := scratch.AddCompleteSample(GinkgoT(), time.Now(), 0)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := cldy.MetricsCollectorServiceImpl{
				APIKey:           "goodkey123",
				BaseURL:          "https://metrics-collector.example.com/metricsample",
				UserAgent:        "cldy-client/test",
				CldyUploadClient: &mockClientService{},
			}
			actualUploader.StorageServices = append(actualUploader.StorageServices, &service)
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     sample + "daemonsets.jsonl",
			}
			err := actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should upload to custom s3 bucket", func() {
			config := defaultConfig(tempDir)
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			sample := scratch.AddCompleteSample(GinkgoT(), time.Now(), 0)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			uploadClient := &mockS3UploadService{}
			service := cldy.CustomS3Client{
				UploadClient: uploadClient,
			}
			actualUploader.StorageServices = append(actualUploader.StorageServices, &service)

			// Succeed on a good filename
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     sample + "daemonsets.jsonl",
			}
			err := actualUploader.StorageServices[0].Upload(payload)
			Expect(err).ToNot(HaveOccurred())
			Expect(uploadClient.UploadedSampleName).To(Equal("production/data/metrics-agent/2025/05/05/good-cluster/good-cluster-20250505-18-05.tgz"))

			// Error on an unparseable filename
			payload.FileName = "badFileName"
			err = actualUploader.StorageServices[0].Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("error parsing name from sample filename"))
		})
		It("should upload to custom azure blob", func() {
			config := defaultConfig(tempDir)
			config.CustomAzureBlobContainerName = "a"
			config.CustomAzureBlobUrl = "testurl"
			config.CustomAzureTenantID = "1"
			config.CustomAzureClientID = "1"
			config.CustomAzureClientSecret = cldy.NewValueSecretManager("1")

			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			sample := scratch.AddCompleteSample(GinkgoT(), time.Now(), 0)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			uploadClient := &MockBlobUploadService{}
			service := cldy.CustomBlobClient{
				UploadClient: uploadClient,
			}
			actualUploader.StorageServices = append(actualUploader.StorageServices, &service)

			// Succeed on a good filename
			payload := cldy.UploadPayload{
				ClusterUID:   "good-cluster",
				FileName:     goodFileName,
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     sample + "daemonsets.jsonl",
			}
			err := actualUploader.StorageServices[0].Upload(payload)
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

func defaultConfig(tempDir string) cldy.UploaderConfig {
	return cldy.UploaderConfig{
		UploadFrequency: time.Hour,
		ScratchDir:      tempDir,
		SecretManager:   cldy.NewKeyValueSecretManager("", ""),
		EnvID:           "1",
	}
}

// sample timestamps need to be unique otherwise .tgz file names will be the same and cause overwrite which would
// never occur in real data collection/uploading
func updateAgentTimestamp(filePath string, ts int64) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer safeClose(file.Close)
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
	// request to acquire s3 url from metrics-collector
	if strings.Contains(r.URL.Path, "metricsample") {
		if r.Header.Get("x-api-key") == "bad-key" {
			return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader(""))}, nil
		}
		responseBody, _ := json.Marshal(map[string]string{
			"location": "somewhere/valid-location",
		})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(responseBody))}, nil
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
