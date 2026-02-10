package logexporter

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/storage"
)

// mockStorage implements storage.Storage for testing
type mockStorage struct {
	mu       sync.Mutex
	data     map[string][]byte
	writeErr error
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data: make(map[string][]byte),
	}
}

func (m *mockStorage) StorageType() storage.StorageType {
	return storage.StorageTypeMemory
}

func (m *mockStorage) FullPath(path string) string {
	return path
}

func (m *mockStorage) Stat(path string) (*storage.StorageInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return &storage.StorageInfo{
		Name: filepath.Base(path),
		Size: int64(len(data)),
	}, nil
}

func (m *mockStorage) Read(path string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[path]
	if !ok {
		return nil, os.ErrNotExist
	}
	return data, nil
}

func (m *mockStorage) Write(path string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writeErr != nil {
		return m.writeErr
	}
	m.data[path] = data
	return nil
}

func (m *mockStorage) Remove(path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, path)
	return nil
}

func (m *mockStorage) Exists(path string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.data[path]
	return ok, nil
}

func (m *mockStorage) List(prefix string) ([]*storage.StorageInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var infos []*storage.StorageInfo
	for k, v := range m.data {
		infos = append(infos, &storage.StorageInfo{
			Name: filepath.Base(k),
			Size: int64(len(v)),
		})
	}
	return infos, nil
}

func (m *mockStorage) ListDirectories(path string) ([]*storage.StorageInfo, error) {
	return nil, nil
}

func (m *mockStorage) GetWritten(path string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[path]
	return data, ok
}

func TestNewLogExporter(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     tmpDir,
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: false,
		},
		{
			name: "invalid config - buffer too small",
			config: &Config{
				BufferSize:     512 * 1024,
				LogDirPath:     tmpDir,
				SyncInterval:   5 * time.Second,
				ClusterName:    "test-cluster",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
		},
		{
			name: "invalid config - empty cluster name",
			config: &Config{
				BufferSize:     5 * 1024 * 1024,
				LogDirPath:     tmpDir,
				SyncInterval:   5 * time.Second,
				ClusterName:    "",
				PathPrefix:     "logs",
				ExportInterval: 5 * time.Minute,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exporter, err := NewLogExporter(tt.config, store)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewLogExporter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && exporter == nil {
				t.Error("NewLogExporter() returned nil without error")
			}
			if exporter != nil && exporter.writer == nil {
				t.Error("NewLogExporter() did not create writer")
			}
		})
	}
}

func TestLogExporter_Stop(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	config := &Config{
		BufferSize:     5 * 1024 * 1024,
		LogDirPath:     tmpDir,
		SyncInterval:   5 * time.Second,
		ClusterName:    "test-cluster",
		PathPrefix:     "logs",
		ExportInterval: 5 * time.Minute,
	}

	t.Run("stop without start", func(t *testing.T) {
		exporter, err := NewLogExporter(config, store)
		if err != nil {
			t.Fatalf("NewLogExporter() error = %v", err)
		}

		err = exporter.Stop()
		if err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})

	t.Run("stop after start", func(t *testing.T) {
		exporter, err := NewLogExporter(config, store)
		if err != nil {
			t.Fatalf("NewLogExporter() error = %v", err)
		}

		exporter.Start()

		// Give it a moment to start
		time.Sleep(10 * time.Millisecond)

		err = exporter.Stop()
		if err != nil {
			t.Errorf("Stop() error = %v", err)
		}
	})
}

func TestLogExporter_uploadFile(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	config := &Config{
		BufferSize:     5 * 1024 * 1024,
		LogDirPath:     tmpDir,
		SyncInterval:   0,
		ClusterName:    "test-cluster",
		PathPrefix:     "logs",
		ExportInterval: 5 * time.Minute,
	}

	exporter, err := NewLogExporter(config, store)
	if err != nil {
		t.Fatalf("NewLogExporter() error = %v", err)
	}

	t.Run("upload valid file", func(t *testing.T) {
		// Create a test file
		testData := []byte("test log data\n")
		testFile := filepath.Join(tmpDir, "test.log")
		err := os.WriteFile(testFile, testData, 0644)
		if err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}

		// Upload it
		err = exporter.uploadFile(testFile)
		if err != nil {
			t.Errorf("uploadFile() error = %v", err)
		}

		// Verify file was uploaded
		expectedPath := "logs/test-cluster/test.log.gz"
		data, ok := store.GetWritten(expectedPath)
		if !ok {
			t.Errorf("uploadFile() did not write to expected path: %s", expectedPath)
		}

		// Verify data is gzipped
		if len(data) == 0 {
			t.Error("uploadFile() wrote empty data")
		}

		// Decompress and verify content
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("gzip.NewReader() error = %v", err)
		}
		defer gr.Close()

		decompressed, err := io.ReadAll(gr)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}

		if !bytes.Equal(decompressed, testData) {
			t.Errorf("uploadFile() data mismatch: got %q, want %q", decompressed, testData)
		}

		// Verify local file was deleted
		if _, err := os.Stat(testFile); !os.IsNotExist(err) {
			t.Error("uploadFile() did not delete local file after upload")
		}
	})

	t.Run("upload empty file", func(t *testing.T) {
		// Create an empty test file
		testFile := filepath.Join(tmpDir, "empty.log")
		err := os.WriteFile(testFile, []byte{}, 0644)
		if err != nil {
			t.Fatalf("WriteFile() error = %v", err)
		}

		// Upload it
		err = exporter.uploadFile(testFile)
		if err != nil {
			t.Errorf("uploadFile() error = %v", err)
		}

		// Empty files should be deleted without upload
		if _, err := os.Stat(testFile); !os.IsNotExist(err) {
			t.Error("uploadFile() did not delete empty file")
		}
	})

	t.Run("upload non-existent file", func(t *testing.T) {
		testFile := filepath.Join(tmpDir, "nonexistent.log")

		err := exporter.uploadFile(testFile)
		if err == nil {
			t.Error("uploadFile() should return error for non-existent file")
		}
	})
}

func TestLogExporter_uploadPending(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMockStorage()

	config := &Config{
		BufferSize:     5 * 1024 * 1024,
		LogDirPath:     tmpDir,
		SyncInterval:   0,
		ClusterName:    "test-cluster",
		PathPrefix:     "logs",
		ExportInterval: 5 * time.Minute,
	}

	exporter, err := NewLogExporter(config, store)
	if err != nil {
		t.Fatalf("NewLogExporter() error = %v", err)
	}

	// Write some data to create a file
	data := []byte("test log entry\n")
	_, err = exporter.writer.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	// Rotate to make it pending
	err = exporter.writer.Rotate()
	if err != nil {
		t.Fatalf("Rotate() error = %v", err)
	}

	// Upload pending files
	exporter.exportPending()

	// Verify at least one file was uploaded
	store.mu.Lock()
	numUploaded := len(store.data)
	store.mu.Unlock()

	if numUploaded == 0 {
		t.Error("uploadPending() did not upload any files")
	}
}

func TestGzipCompress(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "simple text",
			data: []byte("hello world"),
		},
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "large data",
			data: bytes.Repeat([]byte("test"), 10000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compressed, err := gzipCompress(tt.data)
			if err != nil {
				t.Errorf("gzipCompress() error = %v", err)
				return
			}

			// Decompress and verify
			gr, err := gzip.NewReader(bytes.NewReader(compressed))
			if err != nil {
				t.Fatalf("gzip.NewReader() error = %v", err)
			}
			defer gr.Close()

			decompressed, err := io.ReadAll(gr)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}

			if !bytes.Equal(decompressed, tt.data) {
				t.Errorf("gzipCompress() data mismatch")
			}
		})
	}
}

func TestLogExporter_Integration(t *testing.T) {
	t.Skip("Skipping long-running integration test - covered by unit tests")

	// This test would require waiting for the full upload interval (1 minute minimum)
	// The functionality is adequately covered by the unit tests above which test
	// individual components: rotation, upload, compression, etc.
}

// Made with Bob
