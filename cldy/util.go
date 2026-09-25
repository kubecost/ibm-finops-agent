package cldy

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// retryBackoff returns the wait after the given attempt: capped exponential backoff with jitter,
// a random wait between half and all of 2s, 4s, 8s, ... up to maxRetryBackoff. It is a variable
// so tests can shorten it.
var retryBackoff = func(attempt int) time.Duration {
	d := min(time.Duration(1)<<min(attempt, 10)*time.Second, maxRetryBackoff)
	return d/2 + rand.N(d/2+1)
}

func safeClose(closer func() error, err *error) {
	if closeErr := closer(); closeErr != nil && *err == nil {
		*err = closeErr
	}
}

func getFileNameAndHash(filePath string) (fileName, hash string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", "", err
	}
	defer safeClose(file.Close, &err)

	digest := md5.New()
	if _, err = io.Copy(digest, file); err != nil {
		return "", "", err
	}
	return path.Base(filePath), base64.StdEncoding.EncodeToString(digest.Sum(nil)), nil
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

// diskAvailable returns the bytes available to unprivileged users on the filesystem holding
// dir. It is a variable so tests can simulate a full disk or a statfs failure.
var diskAvailable = func(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
