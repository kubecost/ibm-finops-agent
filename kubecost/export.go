package kubecost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ibm/finops-agent/kubecost/adapters"
	"github.com/ibm/finops-agent/pkg/condition"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/opencost/exporter"
	"github.com/opencost/opencost/core/pkg/source"
	"github.com/opencost/opencost/core/pkg/storage"
)

// Conditions raised by the Kubecost emitter. Chunk 08 folds them into readiness.
const (
	// ConditionBucketUnavailable: the bucket canary's last write, read or delete failed.
	ConditionBucketUnavailable = "bucket_unavailable"
	// ConditionWALUnavailable mirrors the collector WAL's condition of the same name; Init
	// refuses to start the export controllers while it is raised.
	ConditionWALUnavailable = "wal_unavailable"
)

// errStopped is returned for exports attempted after Stop.
var errStopped = errors.New("kubecost emitter is stopping")

// quietPeriod is how long Stop waits with no compute, existence check or write in flight before
// it considers the controllers drained. It covers the gap between a compute returning and its
// write starting (the Exists check is tracked; encoding the set is not).
const quietPeriod = 250 * time.Millisecond

// exportActivity tracks the export controllers' computes and bucket writes, so Stop can wait for
// work in flight. The controllers themselves have no way to wait for their loop to exit.
type exportActivity struct {
	mu         sync.Mutex
	stopping   bool // no new computes
	closed     bool // no new writes: set only when Stop's bound expires
	active     int
	lastChange time.Time

	writesTotal, writeFailuresTotal, rejectedAfterStopTotal atomic.Uint64
}

func (a *exportActivity) begin(isWrite bool) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || (!isWrite && a.stopping) {
		return false
	}
	a.active++
	a.lastChange = time.Now()
	return true
}

func (a *exportActivity) end() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active--
	a.lastChange = time.Now()
}

// drain refuses new computes and waits until nothing has been in flight for quietPeriod. Writes
// stay allowed, so a computation that finished just before Stop still gets its window written
// (a closed window is exported only once). If ctx ends first, new writes are refused from then on.
func (a *exportActivity) drain(ctx context.Context) error {
	a.mu.Lock()
	a.stopping = true
	a.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		a.mu.Lock()
		quiet := a.active == 0 && time.Since(a.lastChange) >= quietPeriod
		active := a.active
		a.mu.Unlock()
		if quiet {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			a.mu.Lock()
			a.closed = true
			a.mu.Unlock()
			return fmt.Errorf("%d Kubecost exports still in flight: %w", active, ctx.Err())
		}
	}
}

// exportStore is the export controllers' view of the bucket. It counts writes and their
// failures, lets Stop wait for writes in flight, and refuses writes after Stop.
type exportStore struct {
	storage.Storage
	activity *exportActivity
}

func (s *exportStore) begin(path string) error {
	if s.activity.begin(true) {
		return nil
	}
	s.activity.rejectedAfterStopTotal.Add(1)
	log.Errorf("Kubecost export to %s refused: the emitter has stopped. The window is exported again after restart only if it is still current.", path)
	return errStopped
}

func (s *exportStore) result(err error) {
	s.activity.writesTotal.Add(1)
	if err != nil {
		s.activity.writeFailuresTotal.Add(1)
	}
}

func (s *exportStore) Write(path string, data []byte) error {
	if err := s.begin(path); err != nil {
		return err
	}
	defer s.activity.end()
	err := s.Storage.Write(path, data)
	s.result(err)
	return err
}

func (s *exportStore) WriteStream(path string) (io.WriteCloser, error) {
	if err := s.begin(path); err != nil {
		return nil, err
	}
	w, err := s.Storage.WriteStream(path)
	if err != nil {
		s.result(err)
		s.activity.end()
		return nil, err
	}
	return &exportStreamWriter{WriteCloser: w, store: s}, nil
}

// Exists is the first bucket call of every export, so it counts as activity for Stop.
func (s *exportStore) Exists(path string) (bool, error) {
	if err := s.begin(path); err != nil {
		return false, err
	}
	defer s.activity.end()
	return s.Storage.Exists(path)
}

func (s *exportStore) Remove(path string) error {
	if err := s.begin(path); err != nil {
		return err
	}
	defer s.activity.end()
	return s.Storage.Remove(path)
}

// exportStreamWriter ends a streamed write's activity when it is closed.
type exportStreamWriter struct {
	io.WriteCloser
	store  *exportStore
	failed bool
	once   sync.Once
}

func (w *exportStreamWriter) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	if err != nil {
		w.failed = true
	}
	return n, err
}

func (w *exportStreamWriter) Close() error {
	err := w.WriteCloser.Close()
	w.once.Do(func() {
		result := err
		if result == nil && w.failed {
			result = errors.New("stream write failed")
		}
		w.store.result(result)
		w.store.activity.end()
	})
	return err
}

// pinnedComputeSource runs each computation against one pinned snapshot of the adapters, so a
// window never mixes data from two snapshots (F-19), and refuses computations after Stop.
type pinnedComputeSource struct {
	exporter.ComputePipelineSource
	dataSource *adapters.OpenCostDataSourceAdapter
	activity   *exportActivity
}

func (p *pinnedComputeSource) enter() (func(), error) {
	if !p.activity.begin(false) {
		return nil, errStopped
	}
	release := p.dataSource.Pin()
	return func() {
		release()
		p.activity.end()
	}, nil
}

func (p *pinnedComputeSource) ComputeAllocation(start, end time.Time) (*opencost.AllocationSet, error) {
	exit, err := p.enter()
	if err != nil {
		return nil, err
	}
	defer exit()
	return p.ComputePipelineSource.ComputeAllocation(start, end)
}

func (p *pinnedComputeSource) ComputeAssets(start, end time.Time) (*opencost.AssetSet, error) {
	exit, err := p.enter()
	if err != nil {
		return nil, err
	}
	defer exit()
	return p.ComputePipelineSource.ComputeAssets(start, end)
}

func (p *pinnedComputeSource) ComputeNetworkInsights(start, end time.Time) (*opencost.NetworkInsightSet, error) {
	exit, err := p.enter()
	if err != nil {
		return nil, err
	}
	defer exit()
	return p.ComputePipelineSource.ComputeNetworkInsights(start, end)
}

func (p *pinnedComputeSource) ComputeKubeModelSet(start, end time.Time) (*kubemodel.KubeModelSet, error) {
	exit, err := p.enter()
	if err != nil {
		return nil, err
	}
	defer exit()
	return p.ComputePipelineSource.ComputeKubeModelSet(start, end)
}

func (p *pinnedComputeSource) GetDataSource() source.OpenCostDataSource {
	return p.ComputePipelineSource.GetDataSource()
}

// bucketCanary probes the export bucket with a write, read and delete every interval, like
// ValidateConfig does once at startup, and raises bucket_unavailable while probes fail. It is
// the interim signal for export failures until OpenCost reports them (U-3).
type bucketCanary struct {
	store      storage.Storage
	path       string
	interval   time.Duration
	conditions *condition.Set

	runs, failures atomic.Uint64
	lastSuccess    atomic.Int64 // unix nanoseconds

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newBucketCanary(store storage.Storage, clusterName, podName string, interval time.Duration, conditions *condition.Set) *bucketCanary {
	return &bucketCanary{
		store: store,
		// Under ValidateConfig's write-test prefix, one object per pod: pods overlapping in a
		// rolling update would otherwise read and delete each other's probes (a delete of a
		// missing blob fails on Azure).
		path:       path.Join(clusterName, "write-test", "canary-"+podName+".txt"),
		interval:   interval,
		conditions: conditions,
	}
}

func (c *bucketCanary) start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Go(func() { c.loop(ctx) })
}

// stop ends the loop and waits for it, and for a probe in flight, within ctx.
func (c *bucketCanary) stop(ctx context.Context) error {
	if c.cancel == nil {
		return nil
	}
	c.cancel()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("bucket canary probe still in flight: %w", ctx.Err())
	}
}

func (c *bucketCanary) loop(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		c.probeWithDeadline(ctx)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// probeWithDeadline runs one probe, bounded by the interval: the storage API takes no context,
// so a probe that hangs is reported as failed and waited for before the next one starts.
func (c *bucketCanary) probeWithDeadline(ctx context.Context) {
	result := make(chan error, 1)
	c.wg.Go(func() { result <- c.probe() })

	timer := time.NewTimer(c.interval)
	defer timer.Stop()
	c.runs.Add(1)
	select {
	case err := <-result:
		c.record(err)
		return
	case <-timer.C:
		c.record(fmt.Errorf("probe timed out after %s", c.interval))
	case <-ctx.Done():
		return
	}
	// don't pile up probes behind a hung one
	select {
	case err := <-result:
		if err == nil {
			c.record(nil)
		}
	case <-ctx.Done():
	}
}

func (c *bucketCanary) probe() error {
	payload := []byte("finops-agent bucket canary " + time.Now().UTC().Format(time.RFC3339Nano))
	if err := c.store.Write(c.path, payload); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	data, err := c.store.Read(c.path)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if string(data) != string(payload) {
		return errors.New("read: content differs from what was written")
	}
	if err := c.store.Remove(c.path); err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	return nil
}

func (c *bucketCanary) record(err error) {
	if err != nil {
		c.failures.Add(1)
		c.conditions.Raise(ConditionBucketUnavailable, "probe_failed", "export bucket probe failed; Kubecost exports are probably failing too: "+err.Error())
		return
	}
	c.lastSuccess.Store(time.Now().UnixNano())
	c.conditions.Clear(ConditionBucketUnavailable)
}
