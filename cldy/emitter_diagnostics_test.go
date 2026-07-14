package cldy

import (
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/emitter"
	statsv1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	neturl "net/url"
)

// fakeMultiError implements the Unwrap() []error shape used by errors.Join, allowing us to
// construct a joined error whose components include a nil entry (which errors.Join itself
// filters out) to exercise the defensive nil-check in the diagnostics path.
type fakeMultiError struct{ errs []error }

func (f fakeMultiError) Error() string   { return "multi" }
func (f fakeMultiError) Unwrap() []error { return f.errs }

func TestUnwrapNodeErrors(t *testing.T) {
	errA := errors.New("node a failed")
	errB := errors.New("node b failed")

	tests := map[string]struct {
		in      error
		wantLen int
		wantNil bool
	}{
		"nil error returns nil":         {in: nil, wantLen: 0, wantNil: true},
		"single error wrapped in slice": {in: errA, wantLen: 1},
		"joined errors unwrapped":       {in: errors.Join(errA, errB), wantLen: 2},
		"joined with duplicates":        {in: errors.Join(errA, errB, errA), wantLen: 3},
		"custom multi-error unwrapped":  {in: fakeMultiError{errs: []error{errA, errB}}, wantLen: 2},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := unwrapNodeErrors(tt.in)
			if len(got) != tt.wantLen {
				t.Fatalf("unwrapNodeErrors() len = %d, want %d", len(got), tt.wantLen)
			}
			if tt.wantNil && got != nil {
				t.Fatalf("unwrapNodeErrors(nil) = %v, want nil", got)
			}
		})
	}
}

func TestHealthy(t *testing.T) {
	restartThreshold := time.Duration(MaxStaleUploadCycles) * UploadFrequencyDuration

	tests := map[string]struct {
		lastSuccess time.Time
		want        bool
	}{
		"startup grace when never collected": {lastSuccess: time.Time{}, want: true},
		"recent success is healthy":          {lastSuccess: time.Now().UTC(), want: true},
		"within threshold is healthy":        {lastSuccess: time.Now().UTC().Add(-(restartThreshold - time.Minute)), want: true},
		"beyond threshold is unhealthy":      {lastSuccess: time.Now().UTC().Add(-(restartThreshold + time.Minute)), want: false},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			ce := &Emitter{lastSuccessfulNodeCollection: tt.lastSuccess}
			if got := ce.Healthy(); got != tt.want {
				t.Fatalf("Healthy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordNodeStats(t *testing.T) {
	sampleStats := []*statsv1.Summary{{}}
	collErr := errors.New("collection boom")

	t.Run("nil summary is a no-op", func(t *testing.T) {
		ce := &Emitter{}
		ce.recordNodeStats(nil)
		if !ce.lastSuccessfulNodeCollection.IsZero() {
			t.Fatalf("timestamp = %v, want zero", ce.lastSuccessfulNodeCollection)
		}
		if ce.lastNodeCollectionErr != nil {
			t.Fatalf("err = %v, want nil", ce.lastNodeCollectionErr)
		}
	})

	t.Run("non-empty stats advance timestamp and store error", func(t *testing.T) {
		ce := &Emitter{}
		before := time.Now().UTC()
		ce.recordNodeStats(&emitter.NodeStatsSummary{Stats: sampleStats, CollectionErr: collErr})
		if ce.lastSuccessfulNodeCollection.Before(before) {
			t.Fatalf("timestamp not advanced: %v", ce.lastSuccessfulNodeCollection)
		}
		if !errors.Is(ce.lastNodeCollectionErr, collErr) {
			t.Fatalf("err = %v, want %v", ce.lastNodeCollectionErr, collErr)
		}
	})

	t.Run("empty stats preserve prior timestamp but update error", func(t *testing.T) {
		prior := time.Now().UTC().Add(-time.Hour)
		ce := &Emitter{lastSuccessfulNodeCollection: prior}
		ce.recordNodeStats(&emitter.NodeStatsSummary{Stats: nil, CollectionErr: collErr})
		if !ce.lastSuccessfulNodeCollection.Equal(prior) {
			t.Fatalf("timestamp changed on empty stats: got %v, want %v", ce.lastSuccessfulNodeCollection, prior)
		}
		if !errors.Is(ce.lastNodeCollectionErr, collErr) {
			t.Fatalf("err = %v, want %v", ce.lastNodeCollectionErr, collErr)
		}
	})
}

func TestWriteAgentFileIncludesProxyURL(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "agentfile")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()

	clusterID := "test-cluster-uid"
	ce := &Emitter{
		startTime:         time.Now().UTC(),
		emissionInterval:  time.Minute,
		agentVersion:      "test-version",
		currentSamplePath: tempDir + "/",
		ClusterID:         &clusterID,
	}
	ce.config.ProxyURL = &neturl.URL{Path: "proxy.example.com"}

	if err := ce.writeAgentFile(); err != nil {
		t.Fatalf("writeAgentFile() error = %v", err)
	}

	raw, err := os.ReadFile(tempDir + "/agent-measurement.json")
	if err != nil {
		t.Fatalf("reading agent file: %v", err)
	}
	var agent agentData
	if err := json.Unmarshal(raw, &agent); err != nil {
		t.Fatalf("unmarshal agent file: %v", err)
	}
	if got := agent.Values["outbound_proxy_url"]; got != "proxy.example.com" {
		t.Errorf("outbound_proxy_url = %q, want proxy.example.com", got)
	}
}

func TestWriteAgentFileNodeDiagnostics(t *testing.T) {
	tests := map[string]struct {
		lastSuccess     time.Time
		collectionErr   error
		wantAgeZero     bool
		wantMinAgeSecs  int64 // when wantAgeZero is false, the reported age must be at least this
		wantNodesFailed int
		wantErrorCounts map[string]int
	}{
		"healthy fresh collection": {
			lastSuccess:     time.Now().UTC(),
			collectionErr:   nil,
			wantAgeZero:     true,
			wantNodesFailed: 0,
			wantErrorCounts: map[string]int{},
		},
		"never collected reports zero age": {
			lastSuccess:     time.Time{},
			collectionErr:   nil,
			wantAgeZero:     true,
			wantNodesFailed: 0,
			wantErrorCounts: map[string]int{},
		},
		"stale collection reports growing age": {
			lastSuccess:     time.Now().UTC().Add(-(UploadFrequencyDuration + time.Minute)),
			collectionErr:   nil,
			wantAgeZero:     false,
			wantMinAgeSecs:  int64(UploadFrequencyDuration.Seconds()),
			wantNodesFailed: 0,
			wantErrorCounts: map[string]int{},
		},
		"partial failures deduplicated by message": {
			lastSuccess:     time.Now().UTC(),
			collectionErr:   errors.Join(errors.New("node a: timeout"), errors.New("node b: refused"), errors.New("node a: timeout")),
			wantAgeZero:     true,
			wantNodesFailed: 3,
			wantErrorCounts: map[string]int{"node a: timeout": 2, "node b: refused": 1},
		},
		"nil error entry handled defensively": {
			lastSuccess:     time.Now().UTC(),
			collectionErr:   fakeMultiError{errs: []error{nil, errors.New("node c: eof")}},
			wantAgeZero:     true,
			wantNodesFailed: 2,
			wantErrorCounts: map[string]int{"": 1, "node c: eof": 1},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			tempDir, err := os.MkdirTemp("", "agentfile")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.RemoveAll(tempDir) }()

			clusterID := "test-cluster-uid"
			ce := &Emitter{
				startTime:                    time.Now().UTC(),
				emissionInterval:             time.Minute,
				agentVersion:                 "test-version",
				currentSamplePath:            tempDir + "/",
				ClusterID:                    &clusterID,
				lastSuccessfulNodeCollection: tt.lastSuccess,
				lastNodeCollectionErr:        tt.collectionErr,
			}

			if err := ce.writeAgentFile(); err != nil {
				t.Fatalf("writeAgentFile() error = %v", err)
			}

			raw, err := os.ReadFile(tempDir + "/agent-measurement.json")
			if err != nil {
				t.Fatalf("reading agent file: %v", err)
			}
			var agent agentData
			if err := json.Unmarshal(raw, &agent); err != nil {
				t.Fatalf("unmarshal agent file: %v", err)
			}

			// node_collection_failed was dropped; ensure it is no longer emitted.
			if _, ok := agent.Values["node_collection_failed"]; ok {
				t.Errorf("node_collection_failed should no longer be emitted")
			}

			ageStr, ok := agent.Values["node_stats_age_seconds"]
			if !ok {
				t.Fatalf("missing node_stats_age_seconds")
			}
			age, err := strconv.ParseInt(ageStr, 10, 64)
			if err != nil {
				t.Fatalf("node_stats_age_seconds = %q, not an int: %v", ageStr, err)
			}
			if tt.wantAgeZero {
				if age != 0 {
					t.Errorf("node_stats_age_seconds = %d, want 0", age)
				}
			} else if age < tt.wantMinAgeSecs {
				t.Errorf("node_stats_age_seconds = %d, want >= %d", age, tt.wantMinAgeSecs)
			}
			if got := agent.Metrics["nodes_failed"]; got != tt.wantNodesFailed {
				t.Errorf("nodes_failed = %d, want %d", got, tt.wantNodesFailed)
			}

			gotCounts := map[string]int{}
			for _, e := range agent.Errors {
				gotCounts[e.Message] = e.Count
				if e.Type != "node_error" {
					t.Errorf("error detail type = %q, want node_error", e.Type)
				}
			}
			if len(gotCounts) != len(tt.wantErrorCounts) {
				t.Errorf("error detail entries = %d (%v), want %d", len(gotCounts), gotCounts, len(tt.wantErrorCounts))
			}
			for msg, want := range tt.wantErrorCounts {
				if gotCounts[msg] != want {
					t.Errorf("error %q count = %d, want %d", msg, gotCounts[msg], want)
				}
			}
		})
	}
}
