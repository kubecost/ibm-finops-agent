package cldy

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

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

// operateAndRemove applies f to a snapshot of the set and removes every entry f succeeded on, so
// partial progress is kept even when a later entry fails. Errors are joined and returned.
//
//   - budget, when positive, is the wall clock after which no further entry is started; it does
//     not interrupt a running entry.
//   - abandon, when non-nil, marks an error every remaining entry would hit identically, so the
//     rest of the pass is skipped. Other errors do not stop the pass.
//
// f runs outside the lock: holding it would deadlock if f touched the set, and would pin a read
// lock across a whole network upload per entry.
func (s *set) operateAndRemove(f func(string) error, budget time.Duration, abandon func(error) bool) error {
	s.mutex.RLock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	s.mutex.RUnlock()

	var deadline time.Time
	if budget > 0 {
		deadline = time.Now().Add(budget)
	}

	toRemove := make([]string, 0, len(keys))
	errs := make([]error, 0)
	for i, k := range keys {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			log.Warnf("operation exhausted its %s budget after %d of %d entries, retaining the "+
				"remaining %d for the next pass", budget, i, len(keys), len(keys)-i)
			break
		}
		if err := f(k); err != nil {
			errs = append(errs, err)
			if abandon != nil && abandon(err) {
				log.Warnf("abandoning the remaining %d of %d entries, every one of them would "+
					"fail the same way: %v", len(keys)-i-1, len(keys), err)
				break
			}
			continue
		}
		toRemove = append(toRemove, k)
	}
	for _, k := range toRemove {
		s.remove(k)
	}
	return errors.Join(errs...)
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

// SafePath joins elements into a path that cannot escape the first element,
// while maintaining trailing separators.
//
// Parent-directory segments are dropped rather than resolved, so
// "test/scratch/../file" yields "test/scratch/file" and not "test/file".
//
// The removal is segment-aware. A previous implementation deleted every literal
// ".." substring before cleaning, which was defeated by any sequence that
// re-formed a traversal after deletion -- "....//" collapses to "../" once the
// inner ".." is removed, so "....//....//etc/passwd" escaped to "/etc/passwd".
// It also corrupted legitimate names, rewriting "v1..2-cluster" to
// "v1 2-cluster". Splitting on the separator and discarding only segments that
// are exactly ".." avoids both: "...." and "v1..2-cluster" are ordinary names
// and are preserved intact.
func SafePath(elements ...string) string {
	sep := string(filepath.Separator)
	joined := strings.Join(elements, sep)

	// Drop traversal and no-op segments. Only an exact ".." is traversal; a
	// segment merely containing dots is a valid filename.
	segments := strings.Split(joined, sep)
	kept := segments[:0]
	for _, s := range segments {
		if s == ".." || s == "." {
			continue
		}
		kept = append(kept, s)
	}
	path := filepath.Clean(strings.Join(kept, sep))

	// Defence in depth: the segment filter above should make escape impossible,
	// but verify containment explicitly rather than relying on that reasoning.
	// Falling back to the base is safe because the base is always a directory
	// the agent owns.
	if len(elements) > 1 {
		base := filepath.Clean(elements[0])
		if base != "." && path != base && !strings.HasPrefix(path, base+sep) {
			log.Warnf("refusing to build a path outside %q from elements %q; using base", base, elements)
			path = base
		}
	}

	if len(elements) != 0 && strings.HasSuffix(elements[len(elements)-1], sep) {
		return path + sep
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
