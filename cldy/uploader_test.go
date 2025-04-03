package cldy_test

import (
	"github.com/ibm/finops-agent/cldy"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestBuildTar(t *testing.T) {
	err := os.CopyFS("temp_test_data", os.DirFS("testdata"))
	if err != nil {
		t.Errorf("Error copying test data: %s", err)
	}
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Errorf("Error creating temp dir: %s", err)
	}
	config := cldy.UploaderConfig{
		UploadFrequency: time.Minute,
		ScratchDir:      tempDir,
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	uploader := cldy.NewCldyUploader(config, stopCh)
	uploader.SetClusterID("test_id")
	uploader.AddSample("temp_test_data")
	actualUploader := uploader.(*cldy.CldyUploader)
	path, err := actualUploader.ConstructPayload()
	if err != nil {
		t.Errorf("Error building payload: %s", err)
	}
	fileInfo, err := os.Stat(path)
	if err != nil {
		t.Errorf("Error getting file info: %s", err)
	}
	if fileInfo.Size() == 0 {
		t.Errorf("File size is zero")
	}
}

func TestTarCleanup(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Errorf("Error creating temp dir: %s", err)
	}
	err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
	if err != nil {
		t.Errorf("Error copying test data: %s", err)
	}
	config := cldy.UploaderConfig{
		UploadFrequency: time.Nanosecond,
		ScratchDir:      tempDir,
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	uploader := cldy.NewCldyUploader(config, stopCh)
	uploader.SetClusterID("test_id")
	actualUploader := uploader.(*cldy.CldyUploader)
	mockService := mockApptioService{}
	actualUploader.StorageService = &mockService
	uploader.AddSample(tempDir + "/scratch/temp_test_data")
	time.Sleep(time.Second)
	if err != nil {
		t.Errorf("Error building payload: %s", err)
	}
	fileInfo, err := os.Stat(tempDir + "/upload")
	if err != nil {
		t.Errorf("Error getting file info: %s", err)
	}
	if fileInfo.Size() == 0 {
		t.Errorf("File size is zero")
	}
}

func TestUpload(t *testing.T) {
	tempDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Errorf("Error creating temp dir: %s", err)
	}
	err = os.CopyFS(tempDir+"/scratch/temp_test_data", os.DirFS("testdata"))
	if err != nil {
		t.Errorf("Error copying test data: %s", err)
	}
	config := cldy.UploaderConfig{
		UploadFrequency: time.Nanosecond,
		ScratchDir:      tempDir,
	}
	stopCh := make(chan struct{})
	defer close(stopCh)
	uploader := cldy.NewCldyUploader(config, stopCh)
	uploader.SetClusterID("test_id")
	actualUploader := uploader.(*cldy.CldyUploader)
	mockService := mockApptioService{}
	actualUploader.StorageService = &mockService
	uploader.AddSample(tempDir + "/scratch/temp_test_data")
	time.Sleep(time.Second)
	if err != nil {
		t.Errorf("Error building payload: %s", err)
	}
	payload := cldy.UploadPayload{
		ClusterUID:   "test_id",
		FileName:     "temp_test_data",
		AgentVersion: "1.0.0",
		UploadHash:   "testing_hash",
		FilePath:     tempDir + "/scratch/temp_test_data",
	}
	err = actualUploader.StorageService.Upload(payload)
	if err != nil {
		t.Errorf("Error uploading payload: %s", err)
	}
	if mockService.uploadCt != 1 {
		t.Errorf("Error uploading payload: expected 1, got %d", mockService.uploadCt)
	}
}

type mockApptioService struct {
	uploadCt int
}

func (mas *mockApptioService) Upload(payload cldy.UploadPayload) error {
	mas.uploadCt++
	return nil
}
