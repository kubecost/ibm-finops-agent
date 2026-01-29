package logexporter

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileWriter is a thread-safe io.Writer that writes log data to rotating files on disk (PVC).
// Files are rotated when they exceed maxSize. Sync runs periodically (syncInterval), not after every write.
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
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	now := time.Now()
	fw := &FileWriter{
		logDir:       logDir,
		maxSize:      maxSize,
		syncInterval: syncInterval,
		lastSyncTime: now,
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

	if fw.fileSize+int64(len(p)) > fw.maxSize {
		if err := fw.rotateFile(); err != nil {
			return 0, fmt.Errorf("failed to rotate log file: %w", err)
		}
	}

	n, err = fw.currentFile.Write(p)
	if err != nil {
		return n, err
	}

	fw.fileSize += int64(n)

	if fw.syncInterval > 0 && time.Since(fw.lastSyncTime) >= fw.syncInterval {
		if err := fw.currentFile.Sync(); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to sync log file: %v\n", err)
		} else {
			fw.lastSyncTime = time.Now()
		}
	}

	return n, nil
}


func (fw *FileWriter) rotateFile() error {
	if fw.currentFile != nil {
		if err := fw.currentFile.Close(); err != nil {
			return fmt.Errorf("failed to close current log file: %w", err)
		}
	}

	now := time.Now()
	// Timestamp + UnixNano so multiple rotations in the same second get unique names
	fileName := fmt.Sprintf("log-%s-%d.log", now.Format("20060102150405"), now.UnixNano())
	fw.currentPath = filepath.Join(fw.logDir, fileName)

	file, err := os.OpenFile(fw.currentPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %w", fw.currentPath, err)
	}

	fw.currentFile = file
	fw.fileSize = 0
	fw.lastSyncTime = now

	return nil
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
		if matched, _ := filepath.Match("log-*.log", entry.Name()); !matched {
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


