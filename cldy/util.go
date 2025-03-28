package cldy

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"sync"
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

	hash := md5.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", "", err
	}

	hashInBytes := hash.Sum(nil)
	return file.Name(), hex.EncodeToString(hashInBytes), nil
}
