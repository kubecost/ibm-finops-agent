package nodes

import (
	"fmt"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/util/atomic"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type NodeStatsSummaryProvider struct {
	client              StatSummaryClient
	runState            atomic.AtomicRunState
	statsLock           sync.RWMutex
	stats               []*stats.Summary
	lastRecordedSummary time.Time
	staleTTL            time.Duration
}

// NewNodeStatsSummaryProvider creates a NodeStatsSummaryProvider. staleTTL is the maximum age
// of the cached node stats before GetNodeData returns an error instead of stale data. A value
// of zero disables the TTL check.
func NewNodeStatsSummaryProvider(client StatSummaryClient, staleTTL time.Duration) *NodeStatsSummaryProvider {
	return &NodeStatsSummaryProvider{
		client:   client,
		staleTTL: staleTTL,
	}
}

// Start begins recording the results of node stats summary queries against the node kubelets on a specific interval. Note
// that the interval begins _after_ a response is received, so it works as more of a "wait time" where between each node stats
// summary request made, there is a wait time = interval.
func (nssp *NodeStatsSummaryProvider) Start(interval time.Duration) bool {
	nssp.runState.WaitForReset()

	if !nssp.runState.Start() {
		return false
	}

	// Make an initial request for this data synchronously
	stats, err := nssp.client.GetNodeData()
	if err != nil {
		// log each node's error as a warning, as we still may have gotten a partial response
		for _, e := range unwrapNodeError(err) {
			log.Warnf("%s", e)
		}
	}

	if len(stats) != 0 {
		nssp.statsLock.Lock()
		nssp.stats = stats
		nssp.lastRecordedSummary = time.Now().UTC()
		nssp.statsLock.Unlock()
	}

	go func() {
		for {
			select {
			// explicit Stop()
			case <-nssp.runState.OnStop():
				nssp.runState.Reset()
				return // exit go routine

			// After our interval elapses, fall through
			case <-time.After(interval):
			}

			stats, err := nssp.client.GetNodeData()
			if err != nil {
				// log each node's error as a warning, as we still may have gotten a partial response
				for _, e := range unwrapNodeError(err) {
					log.Warnf("%s", e)
				}

				// do not overwrite previous results with a failed lookup
				if len(stats) == 0 {
					log.Debugf("All node stats summaries failed, not updating internal cache.")
					continue
				}
			}

			nssp.statsLock.Lock()
			nssp.stats = stats
			nssp.lastRecordedSummary = time.Now().UTC()
			nssp.statsLock.Unlock()
		}
	}()

	return true
}

// Stop stops the node stats client from refreshing the internal stats data, but the pre-recorded data stays present. This
// service can also be restarted.
func (nssp *NodeStatsSummaryProvider) Stop() {
	nssp.runState.Stop()
}

// GetNodeData will return the last node stats summary data recorded. If a newer request is in-progress, it will _not_ wait
// for that request to complete. Instead, it will return the previously recorded data.
// If the cached data is older than staleTTL (and staleTTL > 0), an error is returned so the
// emitter pipeline does not silently emit frozen data when collection has been broken.
func (nssp *NodeStatsSummaryProvider) GetNodeData() ([]*stats.Summary, error) {
	nssp.statsLock.RLock()
	defer nssp.statsLock.RUnlock()

	// no valid node stats recording has taken place
	if nssp.lastRecordedSummary.IsZero() {
		return nil, fmt.Errorf("no node stats summary data has been recorded")
	}

	sinceLastRecord := time.Since(nssp.lastRecordedSummary)

	// log warning if the stats summary being returned is older than 10m (this is a very reasonable data integrity threshold)
	if sinceLastRecord > (10 * time.Minute) {
		log.Warnf("Node Stats Summary being emitted is %d seconds old.", int64(sinceLastRecord.Seconds()))
	}

	// Cache has exceeded its TTL — return an error so the emitter pipeline knows collection is
	// broken, rather than silently emitting stale data indefinitely.
	if nssp.staleTTL > 0 && sinceLastRecord > nssp.staleTTL {
		return nil, fmt.Errorf("node stats summary cache is stale: last successful collection was %d seconds ago", int64(sinceLastRecord.Seconds()))
	}

	return nssp.stats, nil
}

// if the error returned from node stats summary is a multi-error, unwrap and return the inner errors,
// otherwise, just wrap the error in a slice
func unwrapNodeError(err error) []error {
	if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
		return multiErr.Unwrap()
	}
	return []error{err}
}
