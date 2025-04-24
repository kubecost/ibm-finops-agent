package cldy_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var _ = Describe("Uploader", func() {
	var tempDir string
	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "")
		Expect(err).ToNot(HaveOccurred())
		err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
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
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			actualUploader := uploader.(*cldy.CldyUploader)
			path, err := actualUploader.ConstructPayload(time.Now())
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(path)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestStartupRecovery", func() {
		FIt("should recover complete sample", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			actualUploader := uploader.(*cldy.CldyUploader)
			path, err := actualUploader.ConstructPayload(time.Now())
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(path)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestTarCleanup", func() {
		It("should cleanup Tar", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			mockService := cldy.ApptioServiceImpl{}
			actualUploader.StorageService = &mockService
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(time.Second)
			fileInfo, err := os.Stat(tempDir + "/upload")
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestUpload", func() {
		It("should upload", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			actualUploader := uploader.(*cldy.CldyUploader)
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mockClientService{},
				SecretManager:    cldy.NewKeyValueSecretManager("bad-key", ""),
			}
			actualUploader.StorageService = &service
			payload := cldy.UploadPayload{
				ClusterUID:   "bad-cluster",
				FileName:     "temp_test_data",
				AgentVersion: "1.0.0",
				UploadHash:   "aexCzQgBAnRYEZxKy71lAw==",
				FilePath:     tempDir + "/scratch/temp_test_data/daemonsets.jsonl",
			}
			// upload with bad froontdoor credentials
			err := actualUploader.StorageService.Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("frontdoor service login call failed"))
			service.SecretManager = cldy.NewKeyValueSecretManager("good-key", "")
			// upload with good key but bad clusterUID
			err = actualUploader.StorageService.Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cloudability clusters/upload request call failed with status"))
			// upload with successful login and successful url generation
			payload.ClusterUID = "good-cluster"
			err = actualUploader.StorageService.Upload(payload)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should only login once", func() {
			config := cldy.UploaderConfig{
				UploadFrequency: 250 * time.Millisecond,
				ScratchDir:      tempDir,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")

			actualUploader := uploader.(*cldy.CldyUploader)
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("good-key", ""),
			}
			actualUploader.StorageService = &service

			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
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
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")

			actualUploader := uploader.(*cldy.CldyUploader)
			mcs := mockClientService{}
			service := cldy.ApptioServiceImpl{
				CldyUploadClient: &mcs,
				SecretManager:    cldy.NewKeyValueSecretManager("short-lived-token", ""),
			}
			actualUploader.StorageService = &service

			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(1))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(1))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(1))

			err := os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			uploader.AddSample(tempDir + "/scratch/temp_test_data")
			time.Sleep(500 * time.Millisecond)
			Expect(mcs.countByPath["/service/apikeylogin"]).To(Equal(2))
			Expect(mcs.countByPath["/v3/internal/containers/clusters/upload"]).To(Equal(2))
			Expect(mcs.countByPath["somewhere/valid-location"]).To(Equal(2))
		})
	})
})

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
