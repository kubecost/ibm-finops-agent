package cldy

// Crash-consistent startup recovery (FINDINGS.md chunk 01).
//
// Recovery runs once, in newCldyUploader, before the emitter's first snapshot and before
// uploadLoop starts, so it races with neither. It lists directories rather than walking them and
// decides each entry on its own:
//
//	upload/.<name>.partial             a payload a crash interrupted. Removed: its samples are
//	                                   removed only after the rename, so they are still in
//	                                   scratch/. An unfinalised discard, not a drop.
//	upload/<clusterID>_<ts>.tgz        queued, unless older than the recovery period (a drop,
//	                                   recovery_expired). Its cluster ID comes from its name and
//	                                   is checked against the live one in SetClusterID.
//	upload/<anything else>             quarantined (invalid_payload). It never stops the listing.
//	scratch/_quarantine/               skipped.
//	scratch/<clusterID>/.inprogress-*  staging, never finalised. Removed as an unfinalised
//	                                   discard, not a drop. The emitter's sweep covers only
//	                                   staging directories its own process leaves.
//	scratch/<clusterID>/<ts>_<n>/      a finalised sample: its manifest and every file's hash are
//	                                   checked and it is packaged into its own payload named for
//	                                   the manifest's timestamp and the directory's cluster ID,
//	                                   unless older than the recovery period (recovery_expired).
//	scratch/<clusterID>/<anything else>  quarantined (invalid_sample): a sample written before
//	                                   the manifest existed, or one torn by the filesystem.
//
// Payloads are verified end to end just before upload (uploadData), not here, so startup reads
// no payload and its cost grows only with the number of directory entries.

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// defaultRecoveryPeriod is the default for CLOUDABILITY_RECOVERY_PERIOD (decision D8).
const defaultRecoveryPeriod = 72 * time.Hour

const (
	// payloadTimeFormat is the timestamp in a payload's file name.
	payloadTimeFormat = "2006-01-02-15-04-05"
	// partialSuffix marks a payload being written: upload/.<name>.partial.
	partialSuffix = ".partial"
	// maxPayloadNameAttempts bounds how many seconds a colliding payload name is moved on.
	maxPayloadNameAttempts = 60
)

// Quarantine bounds: past either, the oldest quarantined items are evicted and counted as drops.
// They are variables so tests can shrink them.
var (
	quarantineMaxBytes int64 = 100 << 20
	quarantineMaxAge         = 7 * 24 * time.Hour
)

// errCorruptPayload marks a payload whose gzip or tar stream doesn't read to the end.
var errCorruptPayload = errors.New("corrupt payload")

// payloadName is the file name of the payload for clusterID at ts.
func payloadName(clusterID string, ts time.Time) string {
	return fmt.Sprintf("%s_%s.tgz", clusterID, ts.UTC().Format(payloadTimeFormat))
}

// parsePayloadName parses a payload file name. ok is false unless it is
// <clusterID>_<YYYY-MM-DD-HH-MM-SS>.tgz with a non-empty cluster ID.
func parsePayloadName(name string) (clusterID string, ts time.Time, ok bool) {
	base, found := strings.CutSuffix(name, ".tgz")
	i := strings.LastIndex(base, "_")
	if !found || i <= 0 {
		return "", time.Time{}, false
	}
	ts, err := time.Parse(payloadTimeFormat, base[i+1:])
	if err != nil {
		return "", time.Time{}, false
	}
	return base[:i], ts, true
}

// isPartialPayload reports whether name is a payload being written.
func isPartialPayload(name string) bool {
	return strings.HasPrefix(name, ".") && strings.HasSuffix(name, partialSuffix)
}

func (cu *CldyUploader) recoverDataOnStartup() error {
	// Uploads first, so that payloads built from recovered samples are queued once.
	err := cu.recoverUploadFiles()
	err = errors.Join(err, cu.recoverCompleteSamples())
	cu.trimQuarantine()
	return err
}

// recoverUploadFiles queues the payloads in upload/. See the file comment.
func (cu *CldyUploader) recoverUploadFiles() error {
	entries, err := os.ReadDir(cu.UploadPathDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(cu.UploadPathDir, name)
		if isPartialPayload(name) {
			if err := os.Remove(path); err != nil {
				log.Errorf("failed to remove unfinished Cloudability payload %s: %v", name, err)
				continue
			}
			log.Infof("Removed unfinished Cloudability payload %s left by a crash; its samples are recovered from scratch", name)
			cu.events.UnfinalizedDiscarded(1)
			continue
		}
		_, ts, ok := parsePayloadName(name)
		if !ok || !e.Type().IsRegular() {
			cu.quarantine(path, dropReasonInvalidPayload, "not a <clusterID>_<timestamp>.tgz payload")
			continue
		}
		if age := cu.clock().Sub(ts); age > cu.recoveryPeriod {
			if err := os.Remove(path); err != nil {
				log.Errorf("failed to remove expired Cloudability payload %s: %v", name, err)
				continue
			}
			dropData(cu.events, dropReasonRecoveryExpired, 1,
				fmt.Sprintf("payload %s is %s old, past the recovery period of %s", name, age.Round(time.Second), cu.recoveryPeriod))
			continue
		}
		cu.uploadSet.add(path)
		cu.RecoveredUploads++
	}
	return nil
}

// recoverCompleteSamples packages the finalised samples under every scratch/<clusterID>/. See
// the file comment.
func (cu *CldyUploader) recoverCompleteSamples() error {
	root := SafePath(cu.config.ScratchDir, scratchPath)
	clusters, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var errs error
	for _, c := range clusters {
		if c.Name() == quarantineDirName {
			continue
		}
		if !c.IsDir() {
			log.Warnf("ignoring %s in the Cloudability scratch directory: not a cluster directory", c.Name())
			continue
		}
		errs = errors.Join(errs, cu.recoverClusterSamples(c.Name(), filepath.Join(root, c.Name())))
	}
	return errs
}

// recoverClusterSamples decides each entry of clusterDir, the scratch directory of clusterID.
func (cu *CldyUploader) recoverClusterSamples(clusterID, clusterDir string) error {
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		return err
	}
	var errs error
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(clusterDir, name)
		if strings.HasPrefix(name, stagingPrefix) {
			if err := os.RemoveAll(path); err != nil {
				log.Errorf("failed to remove unfinalised Cloudability sample %s: %v", name, err)
				continue
			}
			log.Infof("Removed unfinalised Cloudability sample %s left by the previous process", name)
			cu.events.UnfinalizedDiscarded(1)
			continue
		}
		m, err := validateSample(path, true)
		if err != nil {
			cu.quarantine(path, dropReasonInvalidSample, fmt.Sprintf("not a valid finalised sample: %v", err))
			continue
		}
		if age := cu.clock().Sub(m.Timestamp); age > cu.recoveryPeriod {
			if err := os.RemoveAll(path); err != nil {
				log.Errorf("failed to remove expired Cloudability sample %s: %v", name, err)
				continue
			}
			dropData(cu.events, dropReasonRecoveryExpired, 1,
				fmt.Sprintf("sample %s is %s old, past the recovery period of %s", name, age.Round(time.Second), cu.recoveryPeriod))
			continue
		}
		payload, err := cu.buildPayload(clusterID, m.Timestamp, []string{path + string(filepath.Separator)})
		if err != nil {
			// Left in place: the next start retries it.
			errs = errors.Join(errs, fmt.Errorf("packaging sample %s: %w", name, err))
			continue
		}
		cu.RecoveredSamples++
		cu.uploadSet.add(payload)
		cu.RecoveredUploads++
	}
	return errs
}

// quarantineDir is <SCRATCH_DIR>/scratch/_quarantine.
func (cu *CldyUploader) quarantineDir() string {
	return SafePath(cu.config.ScratchDir, scratchPath, quarantineDirName)
}

// quarantine moves path into the quarantine and counts a drop for reason: the data is not
// uploaded. If it can't be moved it is removed. Callers bound the quarantine afterwards with
// trimQuarantine.
func (cu *CldyUploader) quarantine(path, reason, detail string) {
	now := cu.clock()
	dest := filepath.Join(cu.quarantineDir(), fmt.Sprintf("%d_%s_%s", now.UnixNano(), reason, filepath.Base(path)))
	err := os.MkdirAll(cu.quarantineDir(), os.ModePerm)
	if err == nil {
		err = os.Rename(path, dest)
	}
	if err != nil {
		log.Errorf("failed to quarantine %s, removing it instead: %v", path, err)
		if rmErr := os.RemoveAll(path); rmErr != nil {
			log.Errorf("failed to remove %s: %v", path, rmErr)
		}
		dest = "nowhere"
	} else if err := os.Chtimes(dest, now, now); err != nil {
		// The quarantine's age bound counts from the move.
		log.Warnf("failed to stamp quarantined %s: %v", dest, err)
	}
	dropData(cu.events, reason, 1, fmt.Sprintf("%s: %s; quarantined to %s", filepath.Base(path), detail, dest))
}

// trimQuarantine evicts the oldest quarantined items until none is older than quarantineMaxAge
// and together they take at most quarantineMaxBytes. Each eviction is a counted drop.
func (cu *CldyUploader) trimQuarantine() {
	dir := cu.quarantineDir()
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err != nil {
		log.Errorf("failed to list the Cloudability quarantine: %v", err)
		return
	}
	type item struct {
		name string
		mod  time.Time
		size int64
	}
	var items []item
	var total int64
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		it := item{name: e.Name(), mod: info.ModTime(), size: diskUsage(filepath.Join(dir, e.Name()))}
		items = append(items, it)
		total += it.size
	}
	sort.Slice(items, func(i, j int) bool { return items[i].mod.Before(items[j].mod) })
	now := cu.clock()
	for _, it := range items {
		expired := now.Sub(it.mod) > quarantineMaxAge
		if !expired && total <= quarantineMaxBytes {
			break
		}
		if err := os.RemoveAll(filepath.Join(dir, it.name)); err != nil {
			log.Errorf("failed to evict %s from the Cloudability quarantine: %v", it.name, err)
			continue
		}
		total -= it.size
		dropData(cu.events, dropReasonQuarantineEvicted, 1,
			fmt.Sprintf("evicted %s from the quarantine (%d bytes, quarantined %s ago)", it.name, it.size, now.Sub(it.mod).Round(time.Second)))
	}
}

// diskUsage returns the total size of the regular files at or under path.
func diskUsage(path string) int64 {
	var n int64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || !d.Type().IsRegular() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			n += info.Size()
		}
		return nil
	})
	return n
}

// verifyPayload reads a payload's gzip and tar streams to the end, bodies and gzip checksum
// included. A stream that doesn't read wraps errCorruptPayload; an I/O error is returned as is.
func verifyPayload(path string) (rerr error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer safeClose(f.Close, &rerr)
	corrupt := func(err error) error {
		var pathErr *fs.PathError
		if errors.As(err, &pathErr) {
			return err
		}
		return fmt.Errorf("%w: %v", errCorruptPayload, err)
	}
	gz, err := gzip.NewReader(f)
	if err != nil {
		return corrupt(err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return corrupt(err)
		}
		if _, err := io.Copy(io.Discard, tr); err != nil {
			return corrupt(fmt.Errorf("entry %s: %w", hdr.Name, err))
		}
	}
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return corrupt(err)
	}
	return nil
}
