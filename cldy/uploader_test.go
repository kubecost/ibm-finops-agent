package cldy_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

var _ = Describe("Uploader", func() {
	Context("TestBuildTar", func() {
		It("should build Tar", func() {
			err := os.CopyFS("temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
			tempDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
			config := cldy.UploaderConfig{
				UploadFrequency: time.Hour,
				ScratchDir:      tempDir,
			}
			stopCh := make(chan struct{})
			defer close(stopCh)
			uploader := cldy.NewCldyUploader(config, stopCh)
			uploader.SetClusterID("test_id")
			uploader.AddSample("temp_test_data")
			actualUploader := uploader.(*cldy.CldyUploader)
			path, err := actualUploader.ConstructPayload()
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(path)
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestTarCleanup", func() {
		It("should cleanup Tar", func() {
			tempDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
			err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
			Expect(err).ToNot(HaveOccurred())
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
			Expect(err).ToNot(HaveOccurred())
			fileInfo, err := os.Stat(tempDir + "/upload")
			Expect(err).ToNot(HaveOccurred())
			Expect(fileInfo.Size()).To(BeNumerically(">", 0))
		})
	})
	Context("TestUpload", func() {
		It("should upload", func() {
			tempDir, err := ioutil.TempDir("", "")
			Expect(err).ToNot(HaveOccurred())
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
				KeyAccess:        "bad-key",
			}
			actualUploader.StorageService = &service
			payload := cldy.UploadPayload{
				ClusterUID:   "bad-cluster",
				FileName:     "temp_test_data",
				AgentVersion: "1.0.0",
				UploadHash:   "testing_hash",
				FilePath:     tempDir + "/scratch/temp_test_data",
			}
			// upload with bad froontdoor credentials
			err = actualUploader.StorageService.Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("frontdoor service login call failed"))
			service.KeyAccess = "good-key"
			// upload with good key but bad clusterUID
			err = actualUploader.StorageService.Upload(payload)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cloudability clusters/upload request call failed with status"))
			// upload with successful login and successful url generation
			payload.ClusterUID = "good-cluster"
			err = actualUploader.StorageService.Upload(payload)
			// Expect(err).ToNot(HaveOccurred())
			// TODO fix the final unit test, currently need to update what file it attempts to open since it does not exist
		})
	})
})

type mockClientService struct{}

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

func (mcs *mockClientService) Do(r *http.Request, _ string) (*http.Response, error) {
	// request to login to Frontdoor
	if strings.Contains(r.URL.Path, "apikeylogin") {
		var body mockfrontdoorRequestBody
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.KeyAccess == "bad-key" {
			return &http.Response{StatusCode: 403, Body: r.Body}, nil
		} else {
			resp := http.Response{StatusCode: 200, Body: r.Body, Header: http.Header{}}
			resp.Header.Set("Apptio-Opentoken", "happytoken")
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
					Location: "good-location",
				}})
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(responseBody))}, nil
		}
	}
	// request to upload data to S3
	if r.Header.Get("Content-MD5") != "" {

	}
	return &http.Response{}, fmt.Errorf("unknown request")
}
