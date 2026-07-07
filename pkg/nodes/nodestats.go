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
	lastResults         []NodeCollectionResult
	lastRecordedSummary time.Time
}

func NewNodeStatsSummaryProvider(client StatSummaryClient) *NodeStatsSummaryProvider {
	return &NodeStatsSummaryProvider{
		client: client,
	}
}

// SecondsSinceLastSuccess returns how many seconds have elapsed since the last successful collection.
// Returns 0 if no collection has ever succeeded.
func (nssp *NodeStatsSummaryProvider) SecondsSinceLastSuccess() float64 {
	nssp.statsLock.RLock()
	defer nssp.statsLock.RUnlock()
	if nssp.lastRecordedSummary.IsZero() {
		return 0
	}
	return time.Since(nssp.lastRecordedSummary).Seconds()
}

// HasEverSucceeded returns true if at least one successful collection has occurred.
func (nssp *NodeStatsSummaryProvider) HasEverSucceeded() bool {
	nssp.statsLock.RLock()
	defer nssp.statsLock.RUnlock()
	return !nssp.lastRecordedSummary.IsZero()
}

// LastCollectionResults returns the per-node results from the most recent collection cycle.
func (nssp *NodeStatsSummaryProvider) LastCollectionResults() []NodeCollectionResult {
	nssp.statsLock.RLock()
	defer nssp.statsLock.RUnlock()
	return nssp.lastResults
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
	stats, results, err := nssp.client.GetNodeData()
	if err != nil {
		// log each node's error as a warning, as we still may have gotten a partial response
		for _, e := range unwrapNodeError(err) {
			log.Warnf("%s", e)
		}
	}

	nssp.statsLock.Lock()
	nssp.lastResults = results
	if len(stats) != 0 {
		nssp.stats = stats
		nssp.lastRecordedSummary = time.Now().UTC()
	}
	nssp.statsLock.Unlock()

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

			stats, results, err := nssp.client.GetNodeData()
			if err != nil {
				// log each node's error as a warning, as we still may have gotten a partial response
				for _, e := range unwrapNodeError(err) {
					log.Warnf("%s", e)
				}

				// do not overwrite previous results with a failed lookup
				if len(stats) == 0 {
					log.Debugf("All node stats summaries failed, not updating internal cache.")
					nssp.statsLock.Lock()
					nssp.lastResults = results
					nssp.statsLock.Unlock()
					continue
				}
			}

			nssp.statsLock.Lock()
			nssp.stats = stats
			nssp.lastResults = results
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
func (nssp *NodeStatsSummaryProvider) GetNodeData() ([]*stats.Summary, []NodeCollectionResult, error) {
	nssp.statsLock.RLock()
	defer nssp.statsLock.RUnlock()

	// no valid node stats recording has taken place
	if nssp.lastRecordedSummary.IsZero() {
		return nil, nssp.lastResults, fmt.Errorf("no node stats summary data has been recorded")
	}

	// log warning if the stats summary being returned is older than 10m (this is a very reasonable data integrity threshold)
	sinceLastRecord := time.Since(nssp.lastRecordedSummary)
	if sinceLastRecord > (10 * time.Minute) {
		log.Warnf("Node Stats Summary being emitted is %d seconds old.", int64(sinceLastRecord.Seconds()))
	}

	return nssp.stats, nssp.lastResults, nil
}

// if the error returned from node stats summary is a multi-error, unwrap and return the inner errors,
// otherwise, just wrap the error in a slice
func unwrapNodeError(err error) []error {
	if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
		return multiErr.Unwrap()
	}
	return []error{err}
}
