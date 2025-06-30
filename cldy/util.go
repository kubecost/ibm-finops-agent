package cldy

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/opencost/opencost/core/pkg/log"
)

func safeClose(closer func() error, err *error) {
	if closeErr := closer(); closeErr != nil && *err == nil {
		*err = closeErr
	}
}

func safeCloseFiles(closer []*os.File, err *error) {
	for _, file := range closer {
		safeClose(file.Close, err)
	}
}

type set struct {
	data  map[string]struct{}
	mutex *sync.RWMutex
}

func (s *set) add(data string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[data] = struct{}{}
}

func (s *set) remove(data string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.data, data)
}

func (s *set) contents() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	var data []string
	for key := range s.data {
		data = append(data, key)
	}
	return data
}

func (s *set) length() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.data)
}

func (s *set) operateAndRemove(f func(string) error) error {
	toRemove := make([]string, 0)
	s.mutex.RLock()
	for k := range s.data {
		err := f(k)
		if err != nil {
			s.mutex.RUnlock()
			return err
		}
		toRemove = append(toRemove, k)
	}
	s.mutex.RUnlock()
	for _, k := range toRemove {
		s.remove(k)
	}
	return nil
}

func newSet() *set {
	return &set{
		data:  make(map[string]struct{}),
		mutex: &sync.RWMutex{},
	}
}

func getFileNameAndHash(filePath string) (string, string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", err
	}
	defer safeClose(file.Close, &err)

	fileName := path.Base(filePath)
	hash := md5.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", "", err
	}
	return fileName, base64.StdEncoding.EncodeToString(hash.Sum(nil)), nil
}

// SafePath joins elements and creates a path that prevents file traversal while maintaining trailing separators
func SafePath(elements ...string) string {
	path := strings.Join(elements, string(filepath.Separator))
	path = strings.ReplaceAll(path, "..", "")
	path = filepath.Clean(path)
	if len(elements) != 0 && strings.HasSuffix(elements[len(elements)-1], string(filepath.Separator)) {
		return path + string(filepath.Separator)
	}
	return path
}

func IsAvailableDiskSpace(dataSize uint64, dir string) bool {
	var stat syscall.Statfs_t
	err := syscall.Statfs(dir, &stat)
	if err != nil {
		log.Errorf("error retrieving available disk space.")
		return false
	}

	// Check if adding the new data will not exceed available space
	return stat.Bavail*uint64(stat.Bsize) >= dataSize
}
