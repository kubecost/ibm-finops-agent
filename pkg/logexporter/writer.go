package logexporter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

const (
	dirPermissions  = 0755
	filePermissions = 0644
	
	// Log file pattern for matching rotated log files
	logFilePattern = "log-*.log"
)

// FileWriter is an io.Writer that writes log data to rotating files on disk.
// Files are rotated when they exceed maxSize. Sync runs periodically as per the syncInterval.
type FileWriter struct {
	logDir       string
	maxSize      int64
	syncInterval time.Duration
	currentFile  *os.File
	currentPath  string
	fileSize     int64
	lastSyncTime time.Time
	mu           sync.Mutex
}

func NewFileWriter(logDir string, maxSize int64, syncInterval time.Duration) (*FileWriter, error) {
	if err := os.MkdirAll(logDir, dirPermissions); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	fw := &FileWriter{
		logDir:       logDir,
		maxSize:      maxSize,
		syncInterval: syncInterval,
		lastSyncTime: time.Time{},
	}

	fw.mu.Lock()
	err := fw.rotateFile()
	fw.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("failed to create initial log file: %w", err)
	}

	return fw, nil
}

func (fw *FileWriter) Write(p []byte) (n int, err error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	// Strip ANSI escape codes to ensure we don't write any color codes to the log file(stripping them makes the logs more readable)
	cleaned := ansiEscapeRegex.ReplaceAll(p, nil)

	if fw.fileSize+int64(len(cleaned)) > fw.maxSize {
		if err := fw.rotateFile(); err != nil {
			return 0, fmt.Errorf("failed to rotate log file: %w", err)
		}
	}

	written, err := fw.currentFile.Write(cleaned)
	if err != nil {
		return written, err
	}

	fw.fileSize += int64(written)

	if fw.syncInterval > 0 && time.Since(fw.lastSyncTime) >= fw.syncInterval {
		if err := fw.currentFile.Sync(); err != nil {
			log.Warnf("Failed to sync log file: %v", err)
		} else {
			fw.lastSyncTime = time.Now()
		}
	}

	return len(p), nil
}

func (fw *FileWriter) rotateFile() error {
	if fw.currentFile != nil {
		syncErr := fw.currentFile.Sync()
		closeErr := fw.currentFile.Close()
		
		if syncErr != nil || closeErr != nil {
			if syncErr != nil {
				log.Warnf("Failed to sync log file during rotation: %v", syncErr)
			}
			if closeErr != nil {
				log.Warnf("Failed to close log file during rotation: %v", closeErr)
			}
			return errors.Join(syncErr, closeErr)
		}
	}

	now := time.Now().UTC()
	// Timestamp + UnixNano to ensure unique file names for multiple rotations in the same second
	fileName := fmt.Sprintf("log-%s-%d.log", now.Format("20060102150405"), now.Nanosecond())
	fw.currentPath = filepath.Join(fw.logDir, fileName)

	file, err := os.OpenFile(fw.currentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, filePermissions)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", fw.currentPath, err)
	}

	fw.currentFile = file
	fw.fileSize = 0
	fw.lastSyncTime = now

	return nil
}

func (fw *FileWriter) Rotate() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	return fw.rotateFile()
}

func (fw *FileWriter) Sync() error {
	fw.mu.Lock()
	defer fw.mu.Unlock()

	if fw.currentFile != nil {
		return fw.currentFile.Sync()
	}

	return nil
}

func (fw *FileWriter) GetPendingFiles() ([]string, error) {
	fw.mu.Lock()
	currentPath := fw.currentPath
	fw.mu.Unlock()

	entries, err := os.ReadDir(fw.logDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read log directory: %w", err)
	}

	var files []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if matched, _ := filepath.Match(logFilePattern, entry.Name()); !matched {
			continue
		}
		fullPath := filepath.Join(fw.logDir, entry.Name())
		if fullPath == currentPath {
			continue
		}
		files = append(files, fullPath)
	}
	return files, nil
}
