package cldy

// agent-measurement.json compatibility (docs/reliability/FINDINGS.md chunk 09, §11). The file is
// what IBM receives with every Cloudability payload, so its fields may only be added to.
//
// testdata/agent-measurement.legacy.golden.json is the file as the agent wrote it before chunk 09,
// for fixed inputs. Every field in it must still be written, with the same value and type.
// testdata/agent-measurement.golden.json is the current file for the same inputs. Regenerate it
// with `go test ./cldy -run TestAgentMeasurementGolden -update`; never regenerate the legacy one.

import (
	"encoding/json"
	"errors"
	"flag"
	neturl "net/url"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ibm/finops-agent/pkg/telemetry"
	"github.com/ibm/finops-agent/pkg/telemetry/dropevent"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/agent-measurement.golden.json")

// goldenEmitter is an emitter whose agent-measurement.json depends only on fixed inputs.
func goldenEmitter(t *testing.T) *Emitter {
	t.Helper()
	now := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	clusterID := "12345678-1234-1234-1234-123456789012"
	ce := &Emitter{
		now:                          func() time.Time { return now },
		startTime:                    now.Add(-18*24*time.Hour - 14*time.Hour),
		emissionInterval:             3 * time.Minute,
		agentVersion:                 "2.11.17",
		currentSamplePath:            t.TempDir() + "/",
		ClusterID:                    &clusterID,
		lastSuccessfulNodeCollection: now.Add(-42 * time.Second),
		lastNodeCollectionErr:        errors.Join(errors.New("node a: timeout"), errors.New("node a: timeout")),
		unsyncedResources:            []string{"apps/deployments"},
	}
	ce.config.ClusterName = "my-test-cluster"
	ce.config.ClusterVersion = "1.30"
	ce.config.ClusterVersionGit = "v1.30.10-eks-bc803b4"
	ce.config.ClusterVersionMajor = "1"
	ce.config.ClusterVersionMinor = "30+"
	ce.config.ProxyURL = &neturl.URL{Path: "proxy.example.com"}
	ce.config.Region = "us-west-2"
	ce.config.CustomS3UploadBucket = "bucket"
	ce.config.CustomS3UploadRegion = "us-east-1"
	gaps := uint64(4)
	ce.config.StatusSummary = func() telemetry.Summary {
		return telemetry.Summary{
			SchemaVersion: telemetry.SummarySchemaVersion,
			Ready:         false,
			Phase:         "running",
			ActiveConditions: []telemetry.SummaryCondition{
				{Component: "cldy-emitter", Type: "uploads_failing", SinceTS: now.Add(-time.Hour).Unix()},
				{Component: "exporter", Type: "emitter_failing", Reason: "kubecost_emitter", SinceTS: now.Add(-2 * time.Hour).Unix()},
			},
			DataDroppedTotal: 5,
			DataDropped: map[string]map[string]uint64{
				telemetry.EmitterCloudability: {telemetry.ReasonBacklogAge: 3},
				telemetry.EmitterExporter:     {telemetry.ReasonSnapshotFailed: 2},
			},
			QuarantineEvictedTotal:    1,
			WindowGapsTotal:           &gaps,
			EmissionSlotsSkippedTotal: 6,
			BacklogFiles:              7,
			BacklogBytes:              8192,
			LastUploadSuccessTS:       now.Add(-45 * time.Minute).Unix(),
			CountersSinceTS:           now.Add(-18 * time.Hour).Unix(),
		}
	}
	return ce
}

func writeGoldenAgentFile(t *testing.T, ce *Emitter) map[string]any {
	t.Helper()
	if err := ce.writeAgentFile(); err != nil {
		t.Fatalf("writeAgentFile: %v", err)
	}
	return readJSON(t, ce.currentSamplePath+"agent-measurement.json")
}

func readJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return doc
}

// containsAll reports the first path at which got lacks, or differs from, a value in want.
// Objects in got may have more keys than in want; everything else must be equal.
func containsAll(want, got any, path string) (string, bool) {
	wm, ok := want.(map[string]any)
	if !ok {
		return path, reflect.DeepEqual(want, got)
	}
	gm, ok := got.(map[string]any)
	if !ok {
		return path, false
	}
	for k, wv := range wm {
		gv, present := gm[k]
		if !present {
			return path + "." + k + " (missing)", false
		}
		if p, ok := containsAll(wv, gv, path+"."+k); !ok {
			return p, false
		}
	}
	return "", true
}

func TestAgentMeasurementGolden(t *testing.T) {
	got := writeGoldenAgentFile(t, goldenEmitter(t))

	t.Run("legacy fields unchanged", func(t *testing.T) {
		legacy := readJSON(t, filepath.Join("testdata", "agent-measurement.legacy.golden.json"))
		if p, ok := containsAll(legacy, got, "$"); !ok {
			t.Fatalf("agent-measurement.json is not backwards compatible at %s", p)
		}
		for k := range got {
			if _, ok := legacy[k]; !ok && k != telemetry.SummaryKey {
				t.Errorf("new top-level field %q: new fields go under %s", k, telemetry.SummaryKey)
			}
		}
	})

	// I6: what IBM receives says how healthy the agent is: backlog, drops, gaps, conditions.
	t.Run("reports agent health", func(t *testing.T) {
		h, ok := got[telemetry.SummaryKey].(map[string]any)
		if !ok {
			t.Fatalf("agent-measurement.json has no %s object", telemetry.SummaryKey)
		}
		for _, k := range []string{"ready", "data_dropped_total", "backlog_files", "backlog_bytes", "last_upload_success_ts",
			"active_conditions", "counters_since_ts"} {
			if _, ok := h[k]; !ok {
				t.Errorf("%s.%s missing", telemetry.SummaryKey, k)
			}
		}
	})

	t.Run("current file", func(t *testing.T) {
		path := filepath.Join("testdata", "agent-measurement.golden.json")
		if *updateGolden {
			body, err := json.MarshalIndent(got, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		want := readJSON(t, path)
		if !reflect.DeepEqual(want, got) {
			body, _ := json.MarshalIndent(got, "", "  ")
			t.Fatalf("agent-measurement.json differs from %s; got:\n%s", path, body)
		}
	})
}

// Without a status summary from main, the emitter reports its own conditions, drops and upload
// queue; quarantine evictions are not drops (they were counted when quarantined).
func TestAgentMeasurementOwnSummary(t *testing.T) {
	ce := goldenEmitter(t)
	ce.config.StatusSummary = nil
	ce.counts, ce.conditions = NewEventCounts(), newConditionStore(ce.now)
	ce.events = ce.counts
	ce.drop(dropReasonDiskPressure, 2, "test")
	dropData(ce.events, dropevent.Drop{Reason: dropReasonQuarantineEvicted, Count: 1})
	ce.setCondition(conditionDiskPressure, true, "test")

	got := writeGoldenAgentFile(t, ce)
	h := got[telemetry.SummaryKey].(map[string]any)
	if h["data_dropped_total"] != float64(2) || h["quarantine_evicted_total"] != float64(1) || h["ready"] != false {
		t.Errorf("%s = %v", telemetry.SummaryKey, h)
	}
	conds, _ := h["active_conditions"].([]any)
	if len(conds) != 1 || conds[0].(map[string]any)["type"] != conditionDiskPressure {
		t.Errorf("active_conditions = %v", conds)
	}
}
