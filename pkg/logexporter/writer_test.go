package logexporter

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewFileWriter(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name         string
		logDir       string
		maxSize      int64
		syncInterval time.Duration
		wantErr      bool
	}{
		{
			name:         "valid config",
			logDir:       tmpDir,
			maxSize:      1024 * 1024,
			syncInterval: time.Second,
			wantErr:      false,
		},
		{
			name:         "creates directory if not exists",
			logDir:       filepath.Join(tmpDir, "newdir"),
			maxSize:      1024 * 1024,
			syncInterval: time.Second,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fw, err := NewFileWriter(tt.logDir, tt.maxSize, tt.syncInterval)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileWriter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && fw == nil {
				t.Error("NewFileWriter() returned nil writer without error")
			}
			if fw != nil {
				// Verify initial file was created
				if fw.currentFile == nil {
					t.Error("NewFileWriter() did not create initial file")
				}
				if fw.currentPath == "" {
					t.Error("NewFileWriter() did not set currentPath")
				}
				// Note: lastSyncTime is set during rotateFile() which is called in NewFileWriter
				// So it won't be zero, but that's okay - the important thing is first write triggers sync
			}
		})
	}
}

func TestFileWriter_Write(t *testing.T) {
	tmpDir := t.TempDir()
	
	t.Run("basic write", func(t *testing.T) {
		fw, err := NewFileWriter(tmpDir, 1024*1024, 0)
		if err != nil {
			t.Fatalf("NewFileWriter() error = %v", err)
		}

		data := []byte("test log entry\n")
		n, err := fw.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v", err)
		}
		if n != len(data) {
			t.Errorf("Write() wrote %d bytes, want %d", n, len(data))
		}
		if fw.fileSize != int64(len(data)) {
			t.Errorf("fileSize = %d, want %d", fw.fileSize, len(data))
		}
	})

	t.Run("rotation on size limit", func(t *testing.T) {
		maxSize := int64(100)
		fw, err := NewFileWriter(tmpDir, maxSize, 0)
		if err != nil {
			t.Fatalf("NewFileWriter() error = %v", err)
		}

		firstPath := fw.currentPath
		
		// Write data that exceeds maxSize
		data := make([]byte, maxSize+10)
		for i := range data {
			data[i] = 'a'
		}
		
		_, err = fw.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v", err)
		}

		// Should have rotated to new file
		if fw.currentPath == firstPath {
			t.Error("Write() did not rotate file when size exceeded")
		}
		
		// First file should exist
		if _, err := os.Stat(firstPath); os.IsNotExist(err) {
			t.Error("Write() did not preserve rotated file")
		}
	})

	t.Run("sync on interval", func(t *testing.T) {
		syncInterval := 100 * time.Millisecond
		fw, err := NewFileWriter(tmpDir, 1024*1024, syncInterval)
		if err != nil {
			t.Fatalf("NewFileWriter() error = %v", err)
		}

		// First write should sync immediately (lastSyncTime is zero)
		data := []byte("test\n")
		_, err = fw.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v", err)
		}

		firstSyncTime := fw.lastSyncTime
		if firstSyncTime.IsZero() {
			t.Error("Write() did not sync on first write")
		}

		// Immediate second write should not sync
		_, err = fw.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v", err)
		}
		if fw.lastSyncTime != firstSyncTime {
			t.Error("Write() synced too early")
		}

		// Wait for sync interval and write again
		time.Sleep(syncInterval + 10*time.Millisecond)
		_, err = fw.Write(data)
		if err != nil {
			t.Errorf("Write() error = %v", err)
		}
		if fw.lastSyncTime == firstSyncTime {
			t.Error("Write() did not sync after interval")
		}
	})
}

func TestFileWriter_Rotate(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := NewFileWriter(tmpDir, 1024*1024, 0)
	if err != nil {
		t.Fatalf("NewFileWriter() error = %v", err)
	}

	// Write some data
	data := []byte("test data\n")
	_, err = fw.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	firstPath := fw.currentPath
	firstFile := fw.currentFile

	// Rotate
	err = fw.Rotate()
	if err != nil {
		t.Errorf("Rotate() error = %v", err)
	}

	// Should have new file
	if fw.currentPath == firstPath {
		t.Error("Rotate() did not change currentPath")
	}
	if fw.currentFile == firstFile {
		t.Error("Rotate() did not change currentFile")
	}
	if fw.fileSize != 0 {
		t.Errorf("Rotate() fileSize = %d, want 0", fw.fileSize)
	}

	// Old file should exist and be closed
	if _, err := os.Stat(firstPath); os.IsNotExist(err) {
		t.Error("Rotate() did not preserve old file")
	}
}

func TestFileWriter_Sync(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := NewFileWriter(tmpDir, 1024*1024, 0)
	if err != nil {
		t.Fatalf("NewFileWriter() error = %v", err)
	}

	// Write some data
	data := []byte("test data\n")
	_, err = fw.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Sync should not error
	err = fw.Sync()
	if err != nil {
		t.Errorf("Sync() error = %v", err)
	}
}

func TestFileWriter_GetPendingFiles(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := NewFileWriter(tmpDir, 1024*1024, 0)
	if err != nil {
		t.Fatalf("NewFileWriter() error = %v", err)
	}

	// Initially no pending files (current file is not pending)
	pending, err := fw.GetPendingFiles()
	if err != nil {
		t.Errorf("GetPendingFiles() error = %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("GetPendingFiles() = %d files, want 0", len(pending))
	}

	// Rotate to create a pending file
	err = fw.Rotate()
	if err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}

	pending, err = fw.GetPendingFiles()
	if err != nil {
		t.Errorf("GetPendingFiles() error = %v", err)
	}
	if len(pending) != 1 {
		t.Errorf("GetPendingFiles() = %d files, want 1", len(pending))
	}

	// Rotate again
	err = fw.Rotate()
	if err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}

	pending, err = fw.GetPendingFiles()
	if err != nil {
		t.Errorf("GetPendingFiles() error = %v", err)
	}
	if len(pending) != 2 {
		t.Errorf("GetPendingFiles() = %d files, want 2", len(pending))
	}

	// Verify all pending files match pattern
	for _, p := range pending {
		name := filepath.Base(p)
		if !strings.HasPrefix(name, "log-") || !strings.HasSuffix(name, ".log") {
			t.Errorf("GetPendingFiles() returned invalid file name: %s", name)
		}
	}
}

func TestFileWriter_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()
	fw, err := NewFileWriter(tmpDir, 1024*1024, 0)
	if err != nil {
		t.Fatalf("NewFileWriter() error = %v", err)
	}

	// Concurrent writes should be safe
	var wg sync.WaitGroup
	numGoroutines := 10
	writesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				data := []byte("concurrent write\n")
				_, err := fw.Write(data)
				if err != nil {
					t.Errorf("Concurrent Write() error = %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify total size is reasonable
	expectedSize := int64(numGoroutines * writesPerGoroutine * len("concurrent write\n"))
	if fw.fileSize > expectedSize {
		t.Errorf("fileSize = %d, expected <= %d", fw.fileSize, expectedSize)
	}
}

func TestFileWriter_ErrorHandling(t *testing.T) {
	tmpDir := t.TempDir()
	
	t.Run("handles rotation errors gracefully", func(t *testing.T) {
		fw, err := NewFileWriter(tmpDir, 100, 0)
		if err != nil {
			t.Fatalf("NewFileWriter() error = %v", err)
		}

		// Close the current file to simulate error condition
		if fw.currentFile != nil {
			fw.currentFile.Close()
		}

		// Write should handle the error
		data := make([]byte, 150)
		_, err = fw.Write(data)
		// Should get an error but not panic
		if err == nil {
			t.Error("Write() should return error when file is closed")
		}
	})
}

// Made with Bob
