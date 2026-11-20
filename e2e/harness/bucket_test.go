package harness

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

const (
	testClusterID = "e2e"
	testUID       = "272d6ef3-4943-46dd-9115-2dead85de10b"
	awsPrefix     = "kubectl exec --namespace " + testNS + " localstack-0 -- awslocal s3 "
)

// Two windows per resolution, so tests can make the newest one empty.
var (
	newHour = time.Date(2026, 11, 11, 18, 0, 0, 0, time.UTC)
	oldHour = newHour.Add(-time.Hour)
)

// windowsFor returns the newest and previous window start for res.
func windowsFor(res string) (newest, previous time.Time) {
	switch res {
	case "10m":
		return newHour.Add(10 * time.Minute), newHour
	case "1h":
		return newHour, oldHour
	default: // 1d
		day := newHour.Truncate(24 * time.Hour)
		return day, day.Add(-24 * time.Hour)
	}
}

func resDuration(res string) time.Duration {
	return map[string]time.Duration{"10m": 10 * time.Minute, "1h": time.Hour, "1d": 24 * time.Hour}[res]
}

// setKey returns the bucket key the agent writes for one window.
func setKey(pipeline, res string, start time.Time) string {
	if pipeline == PipelineKubeModel {
		return fmt.Sprintf("%s/%s/kubemodel/%s/%s/%s", bucketApp, testUID, res, start.Format("2006/01/02"), start.Format(eventTimeFormat))
	}
	end := start.Add(resDuration(res))
	return fmt.Sprintf("federated/%s/etl/bingen/%s/%s/%d-%d", testClusterID, pipeline, res, start.Unix(), end.Unix())
}

// encodeSet returns a real bingen-encoded set for one window, holding one
// entry named "stress" unless empty.
func encodeSet(t *testing.T, pipeline string, start, end time.Time, empty bool) []byte {
	t.Helper()
	var b []byte
	var err error
	switch pipeline {
	case PipelineAllocations:
		s := opencost.NewAllocationSet(start, end)
		if !empty {
			s.Set(opencost.NewMockUnitAllocation("stress", start, end.Sub(start), nil))
		}
		b, err = s.MarshalBinary()
	case PipelineAssets:
		s := opencost.NewAssetSet(start, end)
		if !empty {
			w := opencost.NewWindow(&start, &end)
			s.Insert(opencost.NewNode("stress-node", testClusterID, "kind://node", start, end, w), nil)
		}
		b, err = s.MarshalBinary()
	case PipelineKubeModel:
		s := kubemodel.NewKubeModelSet(start, end)
		if !empty {
			s.Cluster = &kubemodel.Cluster{UID: testUID}
			s.Namespaces["ns-uid"] = &kubemodel.Namespace{UID: "ns-uid", Name: "stress"}
		}
		b, err = s.MarshalBinary()
	}
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// bucketFixture is a fake bucket: objects by key, written to files the fake
// kubectl serves with `cat`, plus the matching `aws s3 ls` listing.
type bucketFixture struct {
	t       *testing.T
	dir     string
	objects map[string][]byte
	fail    map[string]string // key -> stderr; fetching it fails
	// listingErr, if set, makes `ls` fail with this stderr.
	listingErr string
}

// newBucketFixture holds both windows of every pipeline and resolution, all
// non-empty, plus two heartbeats and two diagnostics reports.
func newBucketFixture(t *testing.T) *bucketFixture {
	f := &bucketFixture{t: t, dir: t.TempDir(), objects: map[string][]byte{}, fail: map[string]string{}}
	for _, p := range bucketPipelines {
		for _, res := range BucketResolutions {
			newest, previous := windowsFor(res)
			for _, start := range []time.Time{previous, newest} {
				f.objects[setKey(p, res, start)] = encodeSet(t, p, start, start.Add(resDuration(res)), false)
			}
		}
	}
	for i, ts := range []string{"20261111180010", "20261111180020"} {
		hb, _ := json.Marshal(heartbeat.Heartbeat{Application: bucketApp, Version: fmt.Sprintf("v%d", i), Timestamp: newHour})
		f.objects[bucketApp+"/"+testClusterID+"/heartbeat/"+ts+".json"] = hb

		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		_ = json.NewEncoder(zw).Encode(diagnostics.DiagnosticsRunReport{Application: bucketApp, Results: []*diagnostics.DiagnosticResult{{Name: fmt.Sprintf("run %d", i)}}})
		_ = zw.Close()
		f.objects[bucketApp+"/"+testClusterID+"/diagnostics/"+ts+".json.gz"] = gz.Bytes()
	}
	// Objects the reader must ignore.
	f.objects[bucketApp+"/"+testClusterID+"/collector/20261111180025.bingen"] = []byte("wal")
	f.objects["federated/"+testClusterID+"/etl/bingen/networkinsights/1h/1794423600-1794427200"] = []byte("ni")
	f.objects["federated/other/etl/bingen/allocations/1h/1794430800-1794434400"] = []byte("other cluster")
	return f
}

func (f *bucketFixture) listing() string {
	var b strings.Builder
	for _, k := range slices.Sorted(func(yield func(string) bool) {
		for k := range f.objects {
			if !yield(k) {
				return
			}
		}
	}) {
		fmt.Fprintf(&b, "2026-11-11 18:00:00 %10d %s\n", len(f.objects[k]), k)
	}
	return b.String()
}

// handlers returns fake kubectl handlers for `ls` and one `cp` per object.
// Any other exec (e.g. a fetch of an unknown key) fails.
func (f *bucketFixture) handlers() map[string]string {
	f.t.Helper()
	listing := filepath.Join(f.dir, "listing")
	if err := os.WriteFile(listing, []byte(f.listing()), 0o644); err != nil {
		f.t.Fatal(err)
	}
	ls := fmt.Sprintf("cat '%s'", listing)
	if f.listingErr != "" {
		ls = fmt.Sprintf("echo '%s' >&2; exit 1", f.listingErr)
	}
	h := map[string]string{
		"kubectl exec": `echo "unexpected exec: $*" >&2; exit 1`,
		awsPrefix + "ls s3://" + Bucket + " --recursive": ls,
	}
	for i, k := range slices.Sorted(func(yield func(string) bool) {
		for k := range f.objects {
			if !yield(k) {
				return
			}
		}
	}) {
		file := filepath.Join(f.dir, fmt.Sprintf("obj-%d", i))
		if err := os.WriteFile(file, f.objects[k], 0o644); err != nil {
			f.t.Fatal(err)
		}
		cmd := fmt.Sprintf("cat '%s'", file)
		if msg, ok := f.fail[k]; ok {
			cmd = fmt.Sprintf("echo '%s' >&2; exit 1", msg)
		}
		h[awsPrefix+"cp s3://"+Bucket+"/"+k+" -"] = cmd
	}
	return h
}

// reader returns a BucketReader over a fake client holding one Ready
// localstack pod, with the fake kubectl serving f. Not parallel (fakeTools
// sets PATH).
func (f *bucketFixture) reader() (BucketReader, func() []string) {
	f.t.Helper()
	calls := fakeTools(f.t, fake{on: f.handlers()})
	pod := readyPodIn("localstack-0", map[string]string{"app.kubernetes.io/name": "localstack"}, true)
	return BucketReader{
		Client:    k8sfake.NewClientset(pod),
		Namespace: testNS,
		Cluster:   &Cluster{Name: "e2e-fake", Kubeconfig: "/tmp/e2e-fake/kubeconfig", runtime: "podman"},
		ClusterID: testClusterID,
	}, calls
}

// cpCalls returns the keys fetched, in order.
func cpCalls(calls []string) []string {
	var keys []string
	for _, c := range calls {
		if _, after, ok := strings.Cut(argsOf(c), "awslocal s3 cp s3://"+Bucket+"/"); ok {
			keys = append(keys, strings.TrimSuffix(after, " -"))
		}
	}
	return keys
}

func TestBucketReaderRead(t *testing.T) {
	t.Run("decodes the newest window of everything", func(t *testing.T) {
		f := newBucketFixture(t)
		r, calls := f.reader()
		snap, err := r.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, res := range BucketResolutions {
			newest, _ := windowsFor(res)
			for _, p := range bucketPipelines {
				if got, want := snap.Keys[p+"/"+res], setKey(p, res, newest); got != want {
					t.Errorf("%s/%s read %q, want the newest window %q", p, res, got, want)
				}
			}
			if a := snap.Allocations[res]; a == nil || a.Allocations["stress"] == nil {
				t.Errorf("allocations/%s = %v, want the stress allocation", res, a)
			}
			if a := snap.Assets[res]; a == nil || a.Length() != 1 {
				t.Errorf("assets/%s = %v, want one asset", res, a)
			}
			if k := snap.KubeModels[res]; k == nil || k.Cluster == nil || k.Cluster.UID != testUID {
				t.Errorf("kubemodel/%s = %v, want the cluster", res, k)
			}
		}
		if snap.Heartbeat.Version != "v1" {
			t.Errorf("heartbeat version = %q, want the newest (v1)", snap.Heartbeat.Version)
		}
		if len(snap.Diagnostics.Results) != 1 || snap.Diagnostics.Results[0].Name != "run 1" {
			t.Errorf("diagnostics = %+v, want the newest (run 1)", snap.Diagnostics)
		}
		// One fetch per pipeline and resolution, plus heartbeat and diagnostics;
		// nothing ignored is fetched.
		got := cpCalls(calls())
		if len(got) != len(bucketPipelines)*len(BucketResolutions)+2 {
			t.Errorf("fetched %d objects, want %d: %v", len(got), len(bucketPipelines)*len(BucketResolutions)+2, got)
		}
		for _, k := range got {
			if strings.Contains(k, "collector/") || strings.Contains(k, "networkinsights/") || strings.Contains(k, "/other/") {
				t.Errorf("fetched ignored object %s", k)
			}
		}
		for _, c := range calls() {
			if field(c, "kubeconfig") != r.Cluster.Kubeconfig {
				t.Errorf("call %q did not use the suite's kubeconfig", c)
			}
		}
	})

	// Just after a boundary the new window is usually still an empty set.
	t.Run("falls back to the previous window when the newest is empty", func(t *testing.T) {
		f := newBucketFixture(t)
		newest, previous := windowsFor("1h")
		f.objects[setKey(PipelineAllocations, "1h", newest)] = encodeSet(t, PipelineAllocations, newest, newest.Add(time.Hour), true)
		r, _ := f.reader()
		snap, err := r.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if got, want := snap.Keys["allocations/1h"], setKey(PipelineAllocations, "1h", previous); got != want {
			t.Fatalf("allocations/1h read %q, want the previous window %q", got, want)
		}
	})

	// The first export of a window can be an empty set; Read fails so the
	// caller's Eventually retries.
	t.Run("both newest windows empty is an error", func(t *testing.T) {
		f := newBucketFixture(t)
		newest, previous := windowsFor("10m")
		for _, start := range []time.Time{newest, previous} {
			f.objects[setKey(PipelineKubeModel, "10m", start)] = encodeSet(t, PipelineKubeModel, start, start.Add(10*time.Minute), true)
		}
		r, _ := f.reader()
		_, err := r.Read(context.Background())
		if err == nil || !strings.Contains(err.Error(), "kubemodel/10m: newest windows are empty sets") {
			t.Fatalf("err = %v, want kubemodel/10m empty", err)
		}
	})

	t.Run("a missing resolution is an error and nothing is fetched", func(t *testing.T) {
		f := newBucketFixture(t)
		for k := range f.objects {
			if strings.Contains(k, "/assets/10m/") || strings.Contains(k, "/heartbeat/") {
				delete(f.objects, k)
			}
		}
		r, calls := f.reader()
		_, err := r.Read(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no objects yet for assets/10m, heartbeat") {
			t.Fatalf("err = %v, want assets/10m and heartbeat missing", err)
		}
		if got := cpCalls(calls()); len(got) != 0 {
			t.Fatalf("fetched %v, want nothing before every object exists", got)
		}
	})

	t.Run("a failed fetch is an error", func(t *testing.T) {
		f := newBucketFixture(t)
		newest, _ := windowsFor("1d")
		f.fail[setKey(PipelineAssets, "1d", newest)] = "download failed: NoSuchKey"
		r, _ := f.reader()
		_, err := r.Read(context.Background())
		if err == nil || !strings.Contains(err.Error(), "assets/1d") || !strings.Contains(err.Error(), "NoSuchKey") {
			t.Fatalf("err = %v, want the assets/1d fetch error", err)
		}
	})

	t.Run("an object that doesn't decode is an error", func(t *testing.T) {
		f := newBucketFixture(t)
		newest, _ := windowsFor("1h")
		f.objects[setKey(PipelineAllocations, "1h", newest)] = []byte("not bingen")
		r, _ := f.reader()
		_, err := r.Read(context.Background())
		if err == nil || !strings.Contains(err.Error(), "allocations/1h: decode") {
			t.Fatalf("err = %v, want a decode error for allocations/1h", err)
		}
	})

	t.Run("a listing failure is reported", func(t *testing.T) {
		f := newBucketFixture(t)
		f.listingErr = "Could not connect to the endpoint URL"
		r, _ := f.reader()
		_, err := r.Read(context.Background())
		if err == nil || !strings.Contains(err.Error(), "Could not connect to the endpoint URL") {
			t.Fatalf("err = %v, want the listing error", err)
		}
	})

	t.Run("no Ready localstack pod makes no kubectl call", func(t *testing.T) {
		f := newBucketFixture(t)
		r, calls := f.reader()
		r.Client = k8sfake.NewClientset(readyPodIn("localstack-0", map[string]string{"app.kubernetes.io/name": "localstack"}, false))
		_, err := r.Read(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no Ready pod") {
			t.Fatalf("err = %v, want no Ready pod", err)
		}
		if got := calls(); len(got) != 0 {
			t.Fatalf("calls = %v, want none", got)
		}
	})

	t.Run("needs a Cluster", func(t *testing.T) {
		if _, err := (BucketReader{}).Read(context.Background()); err == nil {
			t.Fatal("want an error without Cluster")
		}
	})
}

func TestParseListing(t *testing.T) {
	t.Parallel()
	// Lines from a real `awslocal s3 ls --recursive` on kind, plus objects
	// the reader must skip.
	out := `2026-11-11 17:40:09       9510 federated/e2e/etl/bingen/allocations/10m/1794418200-1794418800
2026-11-11 17:58:20       9510 federated/e2e/etl/bingen/allocations/10m/1794419400-1794420000
2026-11-11 17:50:00       9510 federated/e2e/etl/bingen/allocations/10m/1794418800-1794419400
2026-11-11 17:58:20       9510 federated/e2e/etl/bingen/allocations/1h/1794416400-1794420000
2026-11-11 17:58:20         55 federated/e2e/etl/bingen/networkinsights/1h/1794416400-1794420000
2026-11-11 17:58:20       9510 federated/other/etl/bingen/allocations/1h/1794416400-1794420000
2026-11-11 17:50:00      10286 finops-agent/272d6ef3-4943-46dd-9115-2dead85de10b/kubemodel/10m/2026/11/11/20261111174000
2026-11-11 17:58:20      10286 finops-agent/272d6ef3-4943-46dd-9115-2dead85de10b/kubemodel/10m/2026/11/11/20261111175000
2026-11-11 17:58:10        322 finops-agent/e2e/heartbeat/20261111175810.json
2026-11-11 17:58:20        322 finops-agent/e2e/heartbeat/20261111175820.json
2026-11-11 17:58:20        322 finops-agent/other/heartbeat/20261111175830.json
2026-11-11 17:58:10        957 finops-agent/e2e/diagnostics/20261111175810.json.gz
2026-11-11 17:58:20        947 finops-agent/e2e/diagnostics/20261111175820.json.gz
2026-11-11 17:58:25      27812 finops-agent/e2e/collector/20261111175825.bingen
                           PRE federated/
`
	l, err := parseListing(out, "e2e")
	if err != nil {
		t.Fatal(err)
	}
	keys := func(objs []windowObject) []string {
		var out []string
		for _, o := range objs {
			out = append(out, o.key)
		}
		return out
	}
	if got, want := keys(l.sets["allocations/10m"]), []string{
		"federated/e2e/etl/bingen/allocations/10m/1794419400-1794420000",
		"federated/e2e/etl/bingen/allocations/10m/1794418800-1794419400",
		"federated/e2e/etl/bingen/allocations/10m/1794418200-1794418800",
	}; !slices.Equal(got, want) {
		t.Errorf("allocations/10m = %v, want newest first %v", got, want)
	}
	if got := keys(l.sets["allocations/1h"]); len(got) != 1 {
		t.Errorf("allocations/1h = %v, want only this cluster's object", got)
	}
	if got := l.sets["kubemodel/10m"]; len(got) != 2 || !got[0].start.Equal(time.Date(2026, 11, 11, 17, 50, 0, 0, time.UTC)) {
		t.Errorf("kubemodel/10m = %v, want 2 with the 17:50 window first", got)
	}
	if want := []string{"finops-agent/e2e/heartbeat/20261111175820.json", "finops-agent/e2e/heartbeat/20261111175810.json"}; !slices.Equal(l.heartbeats, want) {
		t.Errorf("heartbeats = %v, want %v", l.heartbeats, want)
	}
	if len(l.diagnostics) != 2 || l.diagnostics[0] != "finops-agent/e2e/diagnostics/20261111175820.json.gz" {
		t.Errorf("diagnostics = %v", l.diagnostics)
	}
	if _, ok := l.sets["networkinsights/1h"]; ok {
		t.Error("network insights must be ignored")
	}
	want := []string{"allocations/1d", "assets/10m", "assets/1h", "assets/1d", "kubemodel/1h", "kubemodel/1d"}
	if got := l.missing(); !slices.Equal(got, want) {
		t.Errorf("missing = %v, want %v", got, want)
	}
}

func TestParseListingRejectsTwoClusterUIDs(t *testing.T) {
	t.Parallel()
	out := `2026-11-11 17:50:00 10 finops-agent/uid-a/kubemodel/1h/2026/11/11/20261111170000
2026-11-11 17:50:00 10 finops-agent/uid-b/kubemodel/1h/2026/11/11/20261111170000
`
	if _, err := parseListing(out, "e2e"); err == nil || !strings.Contains(err.Error(), "2 cluster UIDs") {
		t.Fatalf("err = %v, want a two-UID error", err)
	}
}
