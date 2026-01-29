package logexporter

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/storage"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	kcenv "github.com/ibm/finops-agent/kubecost/env"
)

// LogExporter writes logs to PVC and uploads closed log files to bucket storage.
type LogExporter struct {
	config         *Config
	writer         *FileWriter
	store          storage.Storage 
	mu             sync.Mutex
	uploadWG       sync.WaitGroup
	ticker         *time.Ticker
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
			log.Infof("Log export enabled: PVC directory %s, bucket upload every %s", logExporterConfig.LogDirPath, logExporterConfig.UploadInterval)
		} else {
			log.Errorf("Failed to initialize log exporter: %s. Log export disabled.", err)
		}
	}

	return logExporter
}

func NewLogExporter(config *Config, store storage.Storage) (*LogExporter, error) {
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
	le.ticker = time.NewTicker(le.config.UploadInterval)
	go le.uploadLoop()
}


func (le *LogExporter) Stop() error {
	le.mu.Lock()
	defer le.mu.Unlock()
	le.stopOnce.Do(func() { close(le.stopCh) })

	if le.writer != nil {
		if err := le.writer.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to sync log file during stop: %v\n", err)
		}
	}

	return nil
}

func (le *LogExporter) uploadLoop() {
	for {
		select {
		case <-le.ticker.C:
			le.uploadPending()
		case <-le.stopCh:
			return
		}
	}
}

func (le *LogExporter) uploadPending() {
	pending, err := le.writer.GetPendingFiles()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Log export: failed to get pending files: %v\n", err)
		return
	}
	for _, filePath := range pending {
		le.uploadWG.Add(1)
		go func(fp string) {
			defer le.uploadWG.Done()
			if err := le.uploadFile(fp); err != nil {
				fmt.Fprintf(os.Stderr, "Log export: failed to upload %s: %v\n", fp, err)
			}
		}(filePath)
	}
}

// uploadFile reads a log file, compresses it, uploads to bucket at logs/<cluster>/YYYY/MM/DD/HH/<timestamp>.log.gz, then deletes the local file.
func (le *LogExporter) uploadFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	if len(data) == 0 {
		os.Remove(filePath)
		return nil
	}

	compressed, err := gzipCompress(data)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}

	info, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	t := info.ModTime()
	// logs/<cluster>/YYYY/MM/DD/HH/<basename>.log.gz (basename from file keeps uniqueness)
	base := filepath.Base(filePath) // e.g. log-20060102150405-1706630400123456789.log
	objectPath := path.Join(
		le.config.PathPrefix,
		le.config.ClusterName,
		t.Format("2006/01/02"),
		t.Format("15"),
		base+".gz",
	)

	if err := le.store.Write(objectPath, compressed); err != nil {
		return fmt.Errorf("write to bucket: %w", err)
	}

	if err := os.Remove(filePath); err != nil {
		fmt.Fprintf(os.Stderr, "Log export: failed to delete after upload %s: %v\n", filePath, err)
	}
	return nil
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
