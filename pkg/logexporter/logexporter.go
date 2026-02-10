package logexporter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/datasourcehealth"
	kcenv "github.com/ibm/finops-agent/kubecost/env"
	"github.com/ibm/finops-agent/pkg/env"
	coreenv "github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/kubeconfig"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	podV1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LogExporter writes logs to log files in the disk and uploads log files to bucket storage.
type LogExporter struct {
	config   *Config
	writer   *FileWriter
	store    storage.Storage
	mu       sync.Mutex
	uploadWG sync.WaitGroup
	ticker   *time.Ticker
	stopCh   chan struct{}
}

const (
	flagFormat       = "log-format"
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
			log.Infof("Log export enabled: local directory %s, upload every %s to bucket %s at path %s", logExporterConfig.LogDirPath, logExporterConfig.ExportInterval, bucketConfigFile, logExporterConfig.PathPrefix)
		} else {
			log.Errorf("Failed to initialize log exporter: %s. Log export disabled.", err)
		}
	}

	return logExporter
}

func NewLogExporter(config *Config, store storage.Storage) (*LogExporter, error) {
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
	// Exporting anything remaining in the log directory before we start
	le.exportPending()
	le.ticker = time.NewTicker(le.config.ExportInterval)
	go le.exportLoop()
}

func (le *LogExporter) Stop() error {
	close(le.stopCh)

	// Lock after closing stopCh to avoid deadlock with exportLoop
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

func (le *LogExporter) exportLoop() {
	for {
		select {
		case <-le.ticker.C:
			if err := le.writer.Rotate(); err != nil {
				log.Warnf("Log export: rotate: %v", err)
			}
			le.exportPending()
			le.exportMetricsSnapshot()
		case <-le.stopCh:
			return
		}
	}
}

func (le *LogExporter) exportPending() {
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

	// finops-agent-logs/<cluster>/<basename>.log.gz
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

// exportMetricsSnapshot captures and uploads current data source health metrics
func (le *LogExporter) exportMetricsSnapshot() {
	snapshot, err := datasourcehealth.CaptureMetricsSnapshot()
	if err != nil {
		log.Errorf("Failed to capture metrics snapshot: %v", err)
		return
	}

	jsonData, err := snapshot.ToJSON()
	if err != nil {
		log.Errorf("Failed to marshal metrics snapshot: %v", err)
		return
	}

	compressed, err := gzipCompress(jsonData)
	if err != nil {
		log.Errorf("Failed to compress metrics snapshot: %v", err)
		return
	}

	// Create filename with timestamp: dataSourceMetrics-20060102150405.json.gz
	fileName := fmt.Sprintf("dataSourceMetrics-%s.json.gz",
		snapshot.Timestamp.Format("20060102150405"))
	
	objectPath := path.Join(
		le.config.PathPrefix,
		le.config.ClusterName,
		fileName,
	)

	if err := le.store.Write(objectPath, compressed); err != nil {
		log.Errorf("Failed to write metrics snapshot to bucket: %v", err)
		return
	}

	sizeMiB := float64(len(compressed)) / (1024 * 1024)
	log.Debugf("Exported data source metrics snapshot to %s (%.2f MiB)", objectPath, sizeMiB)
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
	namespace := coreenv.GetInstallNamespace("finops-agent")
	pods, err := client.CoreV1().Pods(namespace).List(context.Background(), v1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	var pastStatuses []podV1.PodStatus
	for _, pod := range pods.Items {
		pastStatuses = append(pastStatuses, pod.Status)
	}

	data, err := json.MarshalIndent(pastStatuses, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal past statuses: %w", err)
	}

	fileName := fmt.Sprintf("log-past-events-%s.log", time.Now().UTC().Format("20060102150405"))
	file, err := os.OpenFile(path.Join(logDir, fileName), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePermissions)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", path.Join(logDir, fileName), err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Warnf("Failed to close file %s: %v", fileName, err)
		}
	}()

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write log file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}
	log.Infof("Past statuses tracked successfully")
	return nil
}
