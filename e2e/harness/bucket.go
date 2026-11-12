package harness

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/diagnostics"
	"github.com/opencost/opencost/core/pkg/heartbeat"
	"github.com/opencost/opencost/core/pkg/model/kubemodel"
	"github.com/opencost/opencost/core/pkg/opencost"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// The Kubecost emitter writes these to the federated-storage bucket. Paths
// (with <app> finops-agent and <id> the chart's clusterId):
//
//	federated/<id>/etl/bingen/allocations/<res>/<startEpoch>-<endEpoch>
//	federated/<id>/etl/bingen/assets/<res>/<startEpoch>-<endEpoch>
//	<app>/<clusterUID>/kubemodel/<res>/YYYY/MM/DD/<YYYYMMDDhhmmss>
//	<app>/<id>/heartbeat/<YYYYMMDDhhmmss>.json
//	<app>/<id>/diagnostics/<YYYYMMDDhhmmss>.json.gz
//
// Everything else (network insights, the collector's per-scrape WAL under
// <app>/<id>/collector/) is ignored.

// bucketApp is the APP_NAME default the agent writes under.
const bucketApp = "finops-agent"

// BucketResolutions are the windows the Kubecost emitter exports for
// allocations, assets and kubemodel. 10m is there because the chart enables
// MINUTE_METRICS_ENABLED by default.
var BucketResolutions = []string{"10m", "1h", "1d"}

// The set pipelines the reader decodes.
const (
	PipelineAllocations = "allocations"
	PipelineAssets      = "assets"
	PipelineKubeModel   = "kubemodel"
)

var bucketPipelines = []string{PipelineAllocations, PipelineAssets, PipelineKubeModel}

// eventTimeFormat names kubemodel, heartbeat and diagnostics objects.
const eventTimeFormat = "20060102150405"

// BucketReader reads the Kubecost emitter's output from the localstack bucket,
// through `awslocal` in the localstack pod (as EnsureBucket does).
type BucketReader struct {
	Client    kubernetes.Interface
	Namespace string
	Cluster   *Cluster
	// ClusterID is the chart's clusterId ("e2e"), which names the allocation,
	// asset, heartbeat and diagnostics paths.
	ClusterID string
}

// BucketSnapshot is one decoded read of the bucket.
type BucketSnapshot struct {
	// Allocations, Assets and KubeModels hold the newest non-empty set at each
	// of BucketResolutions, keyed by resolution ("10m", "1h", "1d").
	Allocations map[string]*opencost.AllocationSet
	Assets      map[string]*opencost.AssetSet
	KubeModels  map[string]*kubemodel.KubeModelSet
	// Heartbeat and Diagnostics are the newest of each.
	Heartbeat   heartbeat.Heartbeat
	Diagnostics diagnostics.DiagnosticsRunReport
	// Keys maps each entry above to the object it was read from, e.g.
	// "allocations/1h" or "heartbeat", for failure messages.
	Keys map[string]string
}

// Read lists the bucket and decodes the newest non-empty set of each pipeline
// and resolution, plus the newest heartbeat and diagnostics report.
//
// It fails (so a caller's Eventually retries) when anything is missing, or
// when a set's two newest windows are both empty: the first export of a
// window can be an empty set, and just after a boundary the new window is
// usually still empty while the previous one has the data.
func (b BucketReader) Read(ctx context.Context) (*BucketSnapshot, error) {
	if b.Cluster == nil {
		return nil, errors.New("read bucket: BucketReader.Cluster is not set")
	}
	pod, err := b.localstackPod(ctx)
	if err != nil {
		return nil, fmt.Errorf("read bucket: %w", err)
	}
	exec := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(ctx, defaultExecTimeout)
		defer cancel()
		base := []string{"exec", "--namespace", b.Namespace, pod, "--", "awslocal", "s3"}
		return runBytes(ctx, b.Cluster.Env(), "kubectl", append(base, args...)...)
	}

	out, err := exec("ls", "s3://"+Bucket, "--recursive")
	if err != nil {
		return nil, fmt.Errorf("read bucket: %w", err)
	}
	l, err := parseListing(string(out), b.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("read bucket: %w", err)
	}
	if missing := l.missing(); len(missing) > 0 {
		return nil, fmt.Errorf("read bucket: no objects yet for %s", strings.Join(missing, ", "))
	}

	fetch := func(key string) ([]byte, error) {
		return exec("cp", "s3://"+Bucket+"/"+key, "-")
	}
	snap := &BucketSnapshot{
		Allocations: map[string]*opencost.AllocationSet{},
		Assets:      map[string]*opencost.AssetSet{},
		KubeModels:  map[string]*kubemodel.KubeModelSet{},
		Keys:        map[string]string{},
	}
	var empty []error
	for _, res := range BucketResolutions {
		for _, p := range bucketPipelines {
			name := p + "/" + res
			var key string
			var err error
			switch p {
			case PipelineAllocations:
				snap.Allocations[res], key, err = newestNonEmpty[opencost.AllocationSet](fetch, l.sets[name])
			case PipelineAssets:
				snap.Assets[res], key, err = newestNonEmpty[opencost.AssetSet](fetch, l.sets[name])
			case PipelineKubeModel:
				snap.KubeModels[res], key, err = newestNonEmpty[kubemodel.KubeModelSet](fetch, l.sets[name])
			}
			switch {
			case errors.Is(err, errEmptySets):
				empty = append(empty, fmt.Errorf("%s: %w", name, err))
				continue
			case err != nil:
				return nil, fmt.Errorf("read bucket: %s: %w", name, err)
			}
			snap.Keys[name] = key
		}
	}
	if len(empty) > 0 {
		return nil, fmt.Errorf("read bucket: %w", errors.Join(empty...))
	}

	hb, err := fetch(l.heartbeats[0])
	if err == nil {
		err = json.Unmarshal(hb, &snap.Heartbeat)
	}
	if err != nil {
		return nil, fmt.Errorf("read bucket: heartbeat %s: %w", l.heartbeats[0], err)
	}
	snap.Keys["heartbeat"] = l.heartbeats[0]

	diag, err := fetch(l.diagnostics[0])
	if err == nil {
		err = decodeGzipJSON(diag, &snap.Diagnostics)
	}
	if err != nil {
		return nil, fmt.Errorf("read bucket: diagnostics %s: %w", l.diagnostics[0], err)
	}
	snap.Keys["diagnostics"] = l.diagnostics[0]

	return snap, nil
}

// localstackPod returns the name of a Ready, non-terminating localstack pod.
func (b BucketReader) localstackPod(ctx context.Context) (string, error) {
	pods, err := b.Client.CoreV1().Pods(b.Namespace).List(ctx, metav1.ListOptions{LabelSelector: LocalstackSelector})
	if err != nil {
		return "", fmt.Errorf("find localstack pod: %w", err)
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp == nil && isPodReady(p) {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("find localstack pod: no Ready pod matching %q in %s", LocalstackSelector, b.Namespace)
}

var errEmptySets = errors.New("newest windows are empty sets")

// bingenSet is a decodable opencost set (AllocationSet, AssetSet, KubeModelSet).
type bingenSet[T any] interface {
	*T
	UnmarshalBinary([]byte) error
	IsEmpty() bool
}

// newestNonEmpty decodes the newest window in objs, falling back to the one
// before it if the newest is an empty set. objs is newest first.
func newestNonEmpty[T any, P bingenSet[T]](fetch func(string) ([]byte, error), objs []windowObject) (P, string, error) {
	for _, o := range objs[:min(2, len(objs))] {
		body, err := fetch(o.key)
		if err != nil {
			return nil, "", err
		}
		set := P(new(T))
		if err := set.UnmarshalBinary(body); err != nil {
			return nil, "", fmt.Errorf("decode %s (%d bytes): %w", o.key, len(body), err)
		}
		if !set.IsEmpty() {
			return set, o.key, nil
		}
	}
	return nil, "", errEmptySets
}

func decodeGzipJSON(body []byte, v any) error {
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// windowObject is one exported set: its key and window start.
type windowObject struct {
	key   string
	start time.Time
}

// bucketListing is the bucket's objects, grouped and sorted newest first.
type bucketListing struct {
	// sets maps "<pipeline>/<res>" (e.g. "allocations/1h") to its windows.
	sets        map[string][]windowObject
	heartbeats  []string
	diagnostics []string
}

// missing names the pipelines and resolutions with no object, plus
// "heartbeat" and "diagnostics" if those have none.
func (l bucketListing) missing() []string {
	var out []string
	for _, p := range bucketPipelines {
		for _, res := range BucketResolutions {
			if len(l.sets[p+"/"+res]) == 0 {
				out = append(out, p+"/"+res)
			}
		}
	}
	if len(l.heartbeats) == 0 {
		out = append(out, "heartbeat")
	}
	if len(l.diagnostics) == 0 {
		out = append(out, "diagnostics")
	}
	return out
}

var (
	// federated/<id>/etl/bingen/<pipeline>/<res>/<start>-<end>
	setKeyRE = regexp.MustCompile(`^federated/([^/]+)/etl/bingen/(allocations|assets)/(10m|1h|1d)/(\d+)-\d+$`)
	// <app>/<uid>/kubemodel/<res>/YYYY/MM/DD/<YYYYMMDDhhmmss>
	kubeModelKeyRE = regexp.MustCompile(`^` + bucketApp + `/([^/]+)/kubemodel/(10m|1h|1d)/\d{4}/\d{2}/\d{2}/(\d{14})$`)
	// <app>/<id>/heartbeat/<ts>.json and <app>/<id>/diagnostics/<ts>.json.gz
	eventKeyRE = regexp.MustCompile(`^` + bucketApp + `/([^/]+)/(heartbeat|diagnostics)/(\d{14})\.json(\.gz)?$`)
)

// parseListing parses `aws s3 ls --recursive` output ("<date> <time> <size>
// <key>" per line) into the objects for clusterID. Objects of other clusters
// and other pipelines are ignored. Kubemodel is keyed by cluster UID, not
// clusterID, so all kubemodel objects must share one UID.
func parseListing(out, clusterID string) (bucketListing, error) {
	l := bucketListing{sets: map[string][]windowObject{}}
	uids := map[string]bool{}
	for line := range strings.Lines(out) {
		f := strings.Fields(line)
		if len(f) != 4 {
			continue
		}
		key := f[3]
		if m := setKeyRE.FindStringSubmatch(key); m != nil {
			if m[1] != clusterID {
				continue
			}
			sec, err := strconv.ParseInt(m[4], 10, 64)
			if err != nil {
				continue
			}
			name := m[2] + "/" + m[3]
			l.sets[name] = append(l.sets[name], windowObject{key, time.Unix(sec, 0).UTC()})
			continue
		}
		if m := kubeModelKeyRE.FindStringSubmatch(key); m != nil {
			start, err := time.Parse(eventTimeFormat, m[3])
			if err != nil {
				continue
			}
			uids[m[1]] = true
			name := PipelineKubeModel + "/" + m[2]
			l.sets[name] = append(l.sets[name], windowObject{key, start})
			continue
		}
		if m := eventKeyRE.FindStringSubmatch(key); m != nil && m[1] == clusterID {
			// Only the right extension for each event: .json for heartbeats,
			// .json.gz for diagnostics.
			switch {
			case m[2] == "heartbeat" && m[4] == "":
				l.heartbeats = append(l.heartbeats, key)
			case m[2] == "diagnostics" && m[4] == ".gz":
				l.diagnostics = append(l.diagnostics, key)
			}
		}
	}
	if len(uids) > 1 {
		return bucketListing{}, fmt.Errorf("kubemodel objects for %d cluster UIDs, want 1: %v", len(uids), slices.Sorted(func(yield func(string) bool) {
			for u := range uids {
				if !yield(u) {
					return
				}
			}
		}))
	}

	for name := range l.sets {
		slices.SortFunc(l.sets[name], func(a, b windowObject) int { return b.start.Compare(a.start) })
	}
	// The timestamps are fixed-width, so the keys sort by time.
	newestKeyFirst := func(a, b string) int { return cmp.Compare(b, a) }
	slices.SortFunc(l.heartbeats, newestKeyFirst)
	slices.SortFunc(l.diagnostics, newestKeyFirst)
	return l, nil
}
