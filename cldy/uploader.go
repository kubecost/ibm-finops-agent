package cldy

import (
	"archive/tar"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Uploader interface {
	AddSample(sample string)
	SetClusterID(id string)
}

type CldyUploader struct {
	config         UploaderConfig
	tickerCh       time.Ticker
	sampleSet      *set
	uploadSet      *set
	stop           chan struct{}
	clusterID      string
	agentVersion   string
	uploadPathDir  string
	StorageService StorageService
}

func NewCldyUploader(config UploaderConfig, stop chan struct{}) Uploader {
	uploadPathDir := config.ScratchDir + "/" + uploadPath
	err := createIfNotExists(uploadPathDir)
	if err != nil {
		panic("failed to create upload directory: " + err.Error())
	}
	uploader := CldyUploader{
		config:        config,
		sampleSet:     newSet(),
		uploadSet:     newSet(),
		stop:          stop,
		uploadPathDir: uploadPathDir,
		// TODO: dynamically pick client based upon upload config
		StorageService: NewApptioSerivce(config.ApptioConfig),
	}
	// TODO: check scratch dir for existing samples on startup, add them to upload sample pool
	// TODO: cleanup old samples (> 72 hrs?)
	go uploader.uploadLoop()
	return &uploader
}

type UploaderConfig struct {
	ApptioConfig
	UploadFrequency time.Duration
	ScratchDir      string
}

func (ce *CldyUploader) AddSample(sample string) {
	ce.sampleSet.add(sample)
}

func (ce *CldyUploader) SetClusterID(id string) {
	ce.clusterID = id
}

func (ce *CldyUploader) uploadLoop() {
	ticker := time.Tick(ce.config.UploadFrequency)
	for {
		select {
		case <-ce.stop:
			return
		case <-ticker:
			if ce.sampleSet.length() == 0 {
				continue
			}
			path, err := ce.ConstructPayload()
			if err != nil {
				// TODO: general error handling, maybe this can just be logged
				panic("failed to construct cldy payload: " + err.Error())
			}
			ce.uploadSet.add(path)
			err = ce.uploadSet.operateAndRemove(ce.uploadData)
			if err != nil {
				// TODO: logging
			}
		}
	}
}

func (ce *CldyUploader) ConstructPayload() (path string, rerr error) {
	files := make([]*os.File, 0)
	for _, samplePath := range ce.sampleSet.contents() {
		file, err := os.Open(SafePath(samplePath))
		if err != nil {
			return "", err
		}
		files = append(files, file)
	}
	defer safeCloseFiles(files, &rerr)

	path = SafePath(
		ce.uploadPathDir,
		fmt.Sprintf(
			"%s_%s.tgz",
			ce.clusterID,
			time.Now().Format("2006-01-02-15-04-05"),
		),
	)
	tw, err := os.Create(path)
	defer safeClose(tw.Close, &rerr)
	if err != nil {
		return "", err
	}
	err = createTGZ(ce.clusterID, tw, files...)
	if err != nil {
		return "", err
	}
	for _, file := range files {
		ce.sampleSet.remove(file.Name())
		err = os.RemoveAll(file.Name())
		// TODO: eval this case, shouldn't happen
		if err != nil {
			return "", err
		}
	}
	return path, nil
}

func (ce *CldyUploader) uploadData(path string) error {
	fileName, hash, err := getFileNameAndHash(path)
	if err != nil {
		return err
	}
	payload := UploadPayload{
		ClusterUID:   ce.clusterID,
		FileName:     fileName,
		AgentVersion: ce.agentVersion,
		UploadHash:   hash,
		FilePath:     path,
	}
	err = ce.StorageService.Upload(payload)
	if err != nil {
		return err
	}
	// uploads data, then removes tar from path if successful
	return os.Remove(path)
}

// createTGZ takes a source and variable writers and walks 'source' writing each file
// found to the tar writer; the purpose for accepting multiple writers is to allow
// for multiple outputs
func createTGZ(clusterID string, writer io.Writer, srcs ...*os.File) (rerr error) {
	gzw, _ := gzip.NewWriterLevel(writer, flate.BestCompression)
	defer safeClose(gzw.Close, &rerr)
	tw := tar.NewWriter(gzw)
	defer safeClose(tw.Close, &rerr)
	for _, src := range srcs {
		// ensure the src actually exists before trying to tar it
		if _, err := os.Stat(src.Name()); err != nil {
			return fmt.Errorf("Unable to tar files - %v", err.Error())
		}

		// walk path
		err := filepath.Walk(src.Name(), func(file string, fileInfo os.FileInfo, err error) (rerr error) {

			// return on any error
			if err != nil {
				return err
			}

			// create a new dir/file header
			header, err := tar.FileInfoHeader(fileInfo, fileInfo.Name())
			if err != nil {
				return err
			}

			// return on directories since there will be no content to tar
			if fileInfo.Mode().IsDir() {
				return nil
			}

			// if not a directory update the name to correctly reflect the desired destination when untaring
			if !fileInfo.Mode().IsDir() {
				header.Name = filepath.Join(filepath.Base(src.Name()), clusterID, strings.TrimPrefix(file, src.Name()))
			}
			// write the header
			if err := tw.WriteHeader(header); err != nil {
				return err
			}

			// open files for taring
			//nolint gosec
			f, err := os.Open(file)
			if err != nil {
				return err
			}

			defer safeClose(f.Close, &rerr)

			// copy file data into tar writer
			if _, err := io.Copy(tw, f); err != nil {
				return err
			}

			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}
