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

// operateAndRemove applies f to the entries in the set and removes each entry that f
// succeeded on, so partial progress is always retained: an entry f has already acted on is
// never left behind because a later entry failed. Errors from every attempted entry are
// joined and returned together.
//
// A failing entry does not stop the pass. The only caller, uploadLoop, does nothing with the
// returned error but log it, so stopping on the first failure buys nothing; it would instead
// strand the remaining entries behind a single failing one whose position in the map
// iteration order is random, leaving the set unable to drain.
//
// The pass is bounded, though, because f is a blocking network upload and the caller runs on
// a fixed tick. Attempting every entry on every pass makes a pass cost O(len(s.data)), so a
// deep queue against a failing destination overruns the tick, and the ticks that are missed
// while it overruns stop the rest of the loop from running on schedule. Two bounds apply:
//
//   - budget, when positive, is the wall clock after which no further entry is started. It
//     does not interrupt an entry already running, so a pass costs up to budget plus one entry. Entries that were never attempted are simply left in the set for
//     the next pass, exactly like entries that failed.
//   - abandon, when non-nil, classifies an error as one that every remaining entry would hit
//     identically, so the rest of the pass is skipped: attempting it can only spend the same
//     failure again. Errors abandon does not claim - a corrupt or vanished entry, say - do
//     not stop the pass, so one bad entry can never starve the entries behind it.
//
// f runs against a snapshot taken under the read lock rather than while the read lock is
// held. Calling f under the read lock would deadlock the moment f touched this set at all,
// since add and remove take the write lock, and it would hold a read lock across blocking
// network I/O - a whole upload per entry - for the length of an entire drain. The removals
// below take the write lock, which is why they happen after the snapshot has been released
// rather than inside the loop.
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
