package cldy

// The Cloudability upload queue (FINDINGS.md chunk 02).
//
// The queue is the scratch volume itself. Nothing in memory lists its entries, so no entry can
// outlive its file (F-03). It has two stages:
//
//	scratch/<clusterID>/<unixMilli>_<n>/          finalised samples (sample.go), waiting to be
//	                                              packaged
//	upload/<clusterID>_<timestamp>.tgz            payloads, waiting to be uploaded
//	upload/deferred/<clusterID>_<timestamp>.tgz   payloads the head-of-line rule moved behind
//	                                              the rest
//
// Each upload cycle packages every finalised sample of the live cluster into one payload, then
// uploads the payloads in upload/ oldest first by the timestamp in their name (F-23), then those
// in upload/deferred/, oldest first. A payload is removed only after every storage service
// accepted it, so a crash between the upload and the removal uploads it again: delivery is
// at-least-once (decision D5). Cloudability receives the same file name and MD5 again.
//
// Every removal of queued data holds diskQueue.mu: packaging samples, removing a delivered
// payload, quarantining, deferring, and evicting for the disk budget or the backlog bounds. The
// emitter's disk budget and the uploader share one diskQueue, so eviction never races with
// packaging, and it skips the payload being uploaded (inFlight). The upload itself runs without
// the lock. Packaging holds it while it hashes and compresses the samples, so an Emit that needs
// to evict waits for that local disk work; a statfs that hangs (a stuck network filesystem)
// holds it too, which stalls both loops for chunk 08's heartbeats to catch.
//
// Duplicates, beyond a crash between a delivery and its removal: a crash after a payload is
// renamed into place but before its samples are removed packages those samples again, under a
// new name and MD5, so Cloudability can't recognise the second copy by name or hash.

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// deferredDirName is the directory under upload/ holding payloads moved behind the rest of the
// queue by the head-of-line rule.
const deferredDirName = "deferred"

// defaultBacklogMaxBytes is the default for CLOUDABILITY_BACKLOG_MAX_MB.
const defaultBacklogMaxBytes int64 = 2 << 30

type diskQueue struct {
	// mu is held for every change to a queued item.
	mu         sync.Mutex
	scratchDir string // <SCRATCH_DIR>
	uploadDir  string // <SCRATCH_DIR>/upload
	events     EventSink
	now        func() time.Time

	// inFlight is the payload being uploaded, which eviction skips. Guarded by mu.
	inFlight string
	// noUploader is set while no storage service is configured: evictions then count as
	// no_uploader, because that data could never have been uploaded.
	noUploader atomic.Bool
}

func newDiskQueue(scratchDir string, events EventSink, now func() time.Time) *diskQueue {
	return &diskQueue{
		scratchDir: scratchDir,
		uploadDir:  SafePath(scratchDir, uploadPath),
		events:     events,
		now:        now,
	}
}

func (q *diskQueue) clock() time.Time {
	if q.now == nil {
		return time.Now()
	}
	return q.now()
}

func (q *diskQueue) deferredDir() string {
	return filepath.Join(q.uploadDir, deferredDirName)
}

// clusterDir is scratch/<clusterID>, where the emitter finalises clusterID's samples.
func (q *diskQueue) clusterDir(clusterID string) string {
	return SafePath(q.scratchDir, scratchPath, clusterID)
}

// withLock runs f holding q.mu.
func (q *diskQueue) withLock(f func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	f()
}

// queuedPayload is a payload file in upload/ or upload/deferred/.
type queuedPayload struct {
	path      string
	clusterID string
	ts        time.Time // from the file name
	mod       time.Time // for a deferred payload, when it was deferred
	size      int64
	deferred  bool
}

// payloads lists the queued payloads in upload order: upload/ oldest first, then
// upload/deferred/ oldest first. invalid holds the files in either that aren't payloads.
// Payloads being written (.partial) are in neither.
func (q *diskQueue) payloads() (items []queuedPayload, invalid []string, err error) {
	items, invalid, err = listPayloads(q.uploadDir, false)
	if err != nil {
		return nil, nil, err
	}
	deferred, invalidDeferred, err := listPayloads(q.deferredDir(), true)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, nil, err
	}
	return append(items, deferred...), append(invalid, invalidDeferred...), nil
}

func listPayloads(dir string, deferred bool) (items []queuedPayload, invalid []string, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		name := e.Name()
		if isPartialPayload(name) || (!deferred && name == deferredDirName && e.IsDir()) {
			continue
		}
		path := filepath.Join(dir, name)
		clusterID, ts, ok := parsePayloadName(name)
		if !ok || !e.Type().IsRegular() {
			invalid = append(invalid, path)
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue // removed since the listing
		}
		items = append(items, queuedPayload{path: path, clusterID: clusterID, ts: ts, mod: info.ModTime(), size: info.Size(), deferred: deferred})
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].ts.Equal(items[j].ts) {
			return items[i].ts.Before(items[j].ts)
		}
		return items[i].path < items[j].path
	})
	return items, invalid, nil
}

// backlogItem is a queued payload or finalised sample, for the backlog bounds and the disk
// budget.
type backlogItem struct {
	path   string
	ts     time.Time
	size   int64
	sample bool
	// quarantined: an item in scratch/_quarantine, which the disk budget evicts before any
	// deliverable data.
	quarantined bool
}

// backlogLocked lists clusterID's queued samples and every queued payload, oldest first, except
// the payload being uploaded. The caller holds q.mu.
func (q *diskQueue) backlogLocked(clusterID string) ([]backlogItem, error) {
	payloads, _, err := q.payloads()
	if err != nil {
		return nil, err
	}
	var items []backlogItem
	for _, p := range payloads {
		if p.path != q.inFlight {
			items = append(items, backlogItem{path: p.path, ts: p.ts, size: p.size})
		}
	}
	if clusterID != "" {
		samples, _, err := listFinalisedSamples(q.clusterDir(clusterID))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
		for _, s := range samples {
			items = append(items, backlogItem{path: s.Path, ts: s.Manifest.Timestamp, size: s.Manifest.totalBytes(), sample: true})
		}
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ts.Before(items[j].ts) })
	return items, nil
}

// quarantineItems lists scratch/_quarantine, oldest quarantined first.
func (q *diskQueue) quarantineItems() []backlogItem {
	dir := SafePath(q.scratchDir, scratchPath, quarantineDirName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			log.Errorf("failed to list the Cloudability quarantine: %v", err)
		}
		return nil
	}
	var items []backlogItem
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(dir, e.Name())
		items = append(items, backlogItem{path: path, ts: info.ModTime(), size: diskUsage(path), quarantined: true})
	}
	sort.SliceStable(items, func(i, j int) bool { return items[i].ts.Before(items[j].ts) })
	return items
}

// stats returns the number and total size of clusterID's queued samples and every queued
// payload.
func (q *diskQueue) stats(clusterID string) (files int, bytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items, err := q.backlogLocked(clusterID)
	if err != nil {
		log.Errorf("failed to list the Cloudability upload queue: %v", err)
	}
	for _, it := range items {
		bytes += it.size
	}
	if q.inFlight != "" {
		if info, err := os.Stat(q.inFlight); err == nil {
			files++
			bytes += info.Size()
		}
	}
	return files + len(items), bytes
}

// evictLocked removes a queued item and counts it as a drop for reason, or for no_uploader
// while no storage service is configured. The caller holds q.mu.
// The drop is counted before the removal, so a kill in between over-counts rather than losing
// data silently. A quarantined item is counted as quarantine_evicted.
func (q *diskQueue) evictLocked(it backlogItem, reason, detail string) bool {
	kind := "payload"
	switch {
	case it.quarantined:
		kind, reason = "quarantined item", dropReasonQuarantineEvicted
	case it.sample:
		kind = "sample"
	}
	if q.noUploader.Load() && !it.quarantined {
		reason = dropReasonNoUploader
		detail += "; no storage service is configured"
	}
	dropData(q.events, reason, 1, fmt.Sprintf("evicting %s %s (%d bytes, from %s): %s",
		kind, filepath.Base(filepath.Clean(it.path)), it.size, it.ts.UTC().Format(time.RFC3339), detail))
	if err := os.RemoveAll(it.path); err != nil {
		log.Errorf("failed to evict queued Cloudability data %s, counted as dropped but still on disk: %v", it.path, err)
		return false
	}
	return true
}

// makeRoom makes sure the scratch volume has need bytes free, evicting first the quarantine and
// then the oldest queued data, samples of clusterID and payloads alike, until it has (F-18). pressure reports whether space
// was short to begin with, ok whether there is room now. A statfs error is returned as is: free
// space is unknown, which is not pressure, so nothing is evicted (F-49).
func (q *diskQueue) makeRoom(clusterID string, need uint64) (pressure, ok bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.makeRoomLocked(clusterID, need)
}

// makeRoomLocked is makeRoom for a caller that holds q.mu.
func (q *diskQueue) makeRoomLocked(clusterID string, need uint64) (pressure, ok bool, err error) {
	avail, err := diskAvailable(q.scratchDir)
	if err != nil {
		return false, true, err
	}
	if avail >= need {
		return false, true, nil
	}
	items, err := q.backlogLocked(clusterID)
	if err != nil {
		log.Errorf("failed to list the Cloudability upload queue to make room: %v", err)
		return true, false, nil
	}
	for _, it := range append(q.quarantineItems(), items...) {
		if !q.evictLocked(it, dropReasonDiskPressure, fmt.Sprintf("the scratch volume has %d bytes free and %d are needed", avail, need)) {
			continue
		}
		if avail, err = diskAvailable(q.scratchDir); err != nil || avail >= need {
			return true, true, nil
		}
	}
	return true, false, nil
}

// enforceBounds evicts clusterID's queued samples and the queued payloads that are older than
// maxAge (backlog_age), then the oldest until the rest take at most maxBytes (backlog_bytes).
func (q *diskQueue) enforceBounds(clusterID string, maxAge time.Duration, maxBytes int64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	items, err := q.backlogLocked(clusterID)
	if err != nil {
		log.Errorf("failed to list the Cloudability upload queue: %v", err)
		return
	}
	now := q.clock()
	var kept []backlogItem
	var total int64
	for _, it := range items {
		if age := now.Sub(it.ts); age > maxAge &&
			q.evictLocked(it, dropReasonBacklogAge, fmt.Sprintf("%s old, past the recovery period of %s", age.Round(time.Second), maxAge)) {
			continue
		}
		kept = append(kept, it)
		total += it.size
	}
	for _, it := range kept {
		if total <= maxBytes {
			break
		}
		if q.evictLocked(it, dropReasonBacklogBytes, fmt.Sprintf("the backlog of %d bytes is over its limit of %d", total, maxBytes)) {
			total -= it.size
		}
	}
}

// startUpload marks path as being uploaded, so that eviction leaves it alone. It reports false
// if path is no longer queued.
func (q *diskQueue) startUpload(path string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, err := os.Stat(path); err != nil {
		return false
	}
	q.inFlight = path
	return true
}

// finishUpload clears the in-flight mark, running then first (if not nil) with q.mu held, so
// that nothing can evict the payload in between.
func (q *diskQueue) finishUpload(then func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if then != nil {
		then()
	}
	q.inFlight = ""
}
