package logexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podV1 "k8s.io/api/core/v1"
)

// LogExporter writes logs to PVC and uploads closed log files to bucket storage.
type LogExporter struct {
	config   *Config
	writer   *FileWriter
	store    storage.Storage
	mu       sync.Mutex
	uploadWG sync.WaitGroup
	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once
}

const (
	flagFormat       = "log-format"
	flagLevel        = "log-level"
	flagDisableColor = "disable-log-color"
)

func InitializeLogExporter() *LogExporter {
	if !env.IsLogExportEnabled() {
		return nil
	}

	var logExporter *LogExporter
	logExporterConfig := NewConfigFromEnv()

	var bucketStore storage.Storage
	bucketConfigFile := kcenv.GetExportBucketConfigFile()
	if bucketConfig, readErr := os.ReadFile(bucketConfigFile); readErr != nil {
		log.Warnf("Log export: failed to read bucket config %s: %v. Logs will be written to PVC only.", bucketConfigFile, readErr)
	} else if store, newErr := storage.NewBucketStorage(bucketConfig); newErr != nil {
		log.Warnf("Log export: failed to create bucket storage: %v. Logs will be written to PVC only.", newErr)
	} else {
		bucketStore = store
	}

	if bucketStore != nil {
		logExporter, err := NewLogExporter(logExporterConfig, bucketStore)
		if err == nil {
			logExporter.Start()
			log.Infof("Log export enabled: local directory %s, upload every %s to bucket %s at path %s", logExporterConfig.LogDirPath, logExporterConfig.UploadInterval, bucketConfigFile, logExporterConfig.PathPrefix)
		} else {
			log.Errorf("Failed to initialize log exporter: %s. Log export disabled.", err)
		}
	}

	return logExporter
}

func NewLogExporter(config *Config, store storage.Storage) (*LogExporter, error) {
	// Validate configuration first
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	
	logger := log.GetLogger()

	fileWriter, err := NewFileWriter(config.LogDirPath, config.BufferSize, config.SyncInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create file writer: %w", err)
	}

	multiWriter := io.MultiWriter(os.Stderr, fileWriter)

	if strings.ToLower(viper.GetString(flagFormat)) != "json" {
		disableColor := viper.GetBool(flagDisableColor)
		consoleWriter := zerolog.ConsoleWriter{
			Out:        multiWriter,
			TimeFormat: time.RFC3339Nano,
			NoColor:    disableColor,
		}
		wrappedLogger := logger.Output(consoleWriter)
		log.SetLogger(&wrappedLogger)
	} else {
		wrappedLogger := logger.Output(zerolog.SyncWriter(multiWriter))
		log.SetLogger(&wrappedLogger)
	}

	exporter := &LogExporter{
		config: config,
		writer: fileWriter,
		store:  store,
		stopCh: make(chan struct{}),
	}

	return exporter, nil
}

func (le *LogExporter) Start() {
	if err := pastEventsTracker(le.config.LogDirPath); err != nil {
		log.Errorf("Failed to track past events: %v", err)
	}
	// Uploading anything remaining in the log directory
	le.uploadPending()
	le.ticker = time.NewTicker(le.config.UploadInterval)
	go le.uploadLoop()
}

func (le *LogExporter) Stop() error {
	le.stopOnce.Do(func() { close(le.stopCh) })
	
	// Lock after closing stopCh to avoid deadlock with uploadLoop
	le.mu.Lock()
	writer := le.writer
	le.mu.Unlock()

	if writer != nil {
		if err := writer.Sync(); err != nil {
			log.Warnf("Failed to sync log file during stop: %v", err)
		}
	}

	return nil
}

func (le *LogExporter) uploadLoop() {
	for {
		select {
		case <-le.ticker.C:
			if err := le.writer.Rotate(); err != nil {
				log.Warnf("Log export: rotate: %v", err)
			}
			le.uploadPending()
		case <-le.stopCh:
			return
		}
	}
}

func (le *LogExporter) uploadPending() {
	pending, err := le.writer.GetPendingFiles()
	if err != nil {
		log.Errorf("Log export: failed to get pending files: %v", err)
		return
	}
	for _, filePath := range pending {
		le.uploadWG.Add(1)
		go func(fp string) {
			defer le.uploadWG.Done()
			if err := le.uploadFile(fp); err != nil {
				log.Errorf("Log export: failed to upload %s: %v", fp, err)
			}
		}(filePath)
	}
	le.uploadWG.Wait()
}

// uploadFile reads a log file, compresses it, uploads to bucket at logs/<cluster>/YYYY/MM/DD/HH/<timestamp>.log.gz, then deletes the local file.
func (le *LogExporter) uploadFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	if len(data) == 0 {
		err := os.Remove(filePath)
		if err != nil {
			log.Warnf("Failed to delete file %s: %v", filePath, err)
		}
		return nil
	}

	compressed, err := gzipCompress(data)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}

	// logs/<cluster>/<basename>.log.gz
	base := filepath.Base(filePath) // e.g. log-20060102150405-1706630400123456789.log
	objectPath := path.Join(
		le.config.PathPrefix,
		le.config.ClusterName,
		base+".gz",
	)

	if err := le.store.Write(objectPath, compressed); err != nil {
		return fmt.Errorf("write to bucket: %w", err)
	}

	sizeMiB := float64(len(compressed)) / (1024 * 1024)
	log.Debugf("writing new binary data to storage %s %.2f MiB", objectPath, sizeMiB)

	if err := os.Remove(filePath); err != nil {
		log.Warnf("Log export: failed to delete after upload %s: %v", filePath, err)
	}
	return nil
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func pastEventsTracker(logDir string) error {
	client, err := kubeconfig.LoadKubeClient("")
	if err != nil {
		return fmt.Errorf("failed to build Kubernetes client: %w", err)
	}
	log.Infof("Kubernetes client built successfully")
	namespace := coreenv.GetInstallNamespace("finops-agent")
	pods, err := client.CoreV1().Pods(namespace).List(context.Background(), v1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	var pastStatuses []podV1.PodStatus
	for _, pod := range pods.Items {
		pastStatuses = append(pastStatuses, pod.Status)
	}

	file, err := os.OpenFile(path.Join(logDir, "log-past-events.log"), os.O_CREATE|os.O_WRONLY, filePermissions)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", path.Join(logDir, "past-events.log"), err)
	}
	defer file.Close()

	file.WriteString(fmt.Sprintf("%v", pastStatuses))
	file.Sync()
	log.Infof("Past statuses tracked successfully")
	return nil
}