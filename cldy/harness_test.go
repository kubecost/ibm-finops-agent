package cldy_test

// Shared test harness for package cldy: the production scratch layout, a fake clock and a fake
// storage service. Every cldy test that touches the scratch or upload directories should build
// them with prodScratch so that it exercises the layout production writes.

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ibm/finops-agent/cldy"
)

// testingT is the subset of testing.TB and GinkgoT() the harness needs.
type testingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

// prodScratch is a scratch directory laid out the way production writes it:
//
//	<ScratchDir>/scratch/<clusterID>/<unixMilli>_<n>/   samples (Emitter.Init, newNextSamplePath)
//	<ScratchDir>/upload/<clusterID>_<YYYY-MM-DD-HH-MM-SS>.tgz   payloads (ConstructPayload)
type prodScratch struct {
	Dir       string // the configured CLOUDABILITY_SCRATCH_DIR
	ClusterID string
}

func newProdScratch(t testingT, dir, clusterID string) *prodScratch {
	t.Helper()
	p := &prodScratch{Dir: dir, ClusterID: clusterID}
	for _, d := range []string{p.ClusterScratchDir(), p.UploadDir()} {
		if err := os.MkdirAll(d, os.ModePerm); err != nil {
			t.Fatalf("creating %s: %v", d, err)
		}
	}
	return p
}

// ScratchRoot is <ScratchDir>/scratch, the directory startup recovery walks.
func (p *prodScratch) ScratchRoot() string { return filepath.Join(p.Dir, "scratch") }

// ClusterScratchDir is <ScratchDir>/scratch/<clusterID>, the emitter's ScratchPath.
func (p *prodScratch) ClusterScratchDir() string { return filepath.Join(p.ScratchRoot(), p.ClusterID) }

// UploadDir is <ScratchDir>/upload.
func (p *prodScratch) UploadDir() string { return filepath.Join(p.Dir, "upload") }

// SampleDir returns the sample path the emitter would use for sample n taken at ts, including the
// trailing separator production passes to Uploader.AddSample.
func (p *prodScratch) SampleDir(ts time.Time, n int) string {
	return filepath.Join(p.ClusterScratchDir(), fmt.Sprintf("%d_%d", ts.UTC().UnixMilli(), n)) + string(filepath.Separator)
}

// AddCompleteSample writes a complete sample (all required files, from testdata) for sample n
// taken at ts, with agent-measurement.json stamped with ts. It returns the sample path.
func (p *prodScratch) AddCompleteSample(t testingT, ts time.Time, n int) string {
	t.Helper()
	return p.AddIncompleteSample(t, ts, n)
}

// AddIncompleteSample is AddCompleteSample with the named testdata files left out.
func (p *prodScratch) AddIncompleteSample(t testingT, ts time.Time, n int, omit ...string) string {
	t.Helper()
	dir := p.SampleDir(ts, n)
	if err := os.CopyFS(dir, os.DirFS("testdata")); err != nil {
		t.Fatalf("copying sample to %s: %v", dir, err)
	}
	if err := stampAgentMeasurement(filepath.Join(dir, "agent-measurement.json"), ts); err != nil {
		t.Fatalf("stamping sample %s: %v", dir, err)
	}
	for _, f := range omit {
		if err := os.Remove(filepath.Join(dir, f)); err != nil {
			t.Fatalf("removing %s from sample: %v", f, err)
		}
	}
	return dir
}

// stampAgentMeasurement sets the "ts" of an agent-measurement.json, which recovery uses as the
// sample time. It avoids gomega so the harness also works in plain Go tests.
func stampAgentMeasurement(path string, ts time.Time) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var measure map[string]any
	if err := json.Unmarshal(b, &measure); err != nil {
		return err
	}
	measure["ts"] = ts.Unix()
	b, err = json.Marshal(measure)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// UploadName is the payload file name ConstructPayload uses for a payload built at ts.
func (p *prodScratch) UploadName(ts time.Time) string {
	return fmt.Sprintf("%s_%s.tgz", p.ClusterID, ts.UTC().Format("2006-01-02-15-04-05"))
}

// AddUpload writes a valid payload named for ts into upload/ and returns its path.
func (p *prodScratch) AddUpload(t testingT, ts time.Time) string {
	t.Helper()
	path := filepath.Join(p.UploadDir(), p.UploadName(ts))
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("creating payload %s: %v", path, err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	body := []byte(`{"ts":` + fmt.Sprint(ts.Unix()) + `}`)
	hdr := &tar.Header{Name: filepath.Join("sample", p.ClusterID, "agent-measurement.json"), Mode: 0o644, Size: int64(len(body))}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("writing payload: %v", err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatalf("writing payload: %v", err)
	}
	for _, c := range []io.Closer{tw, gz, f} {
		if err := c.Close(); err != nil {
			t.Fatalf("closing payload: %v", err)
		}
	}
	return path
}

// Uploads returns the file names in upload/, sorted.
func (p *prodScratch) Uploads(t testingT) []string {
	t.Helper()
	entries, err := os.ReadDir(p.UploadDir())
	if err != nil {
		t.Fatalf("reading upload dir: %v", err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	return names
}

// ScratchFileCount returns the number of regular files under scratch/.
func (p *prodScratch) ScratchFileCount() int {
	n := 0
	_ = filepath.WalkDir(p.ScratchRoot(), func(_ string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			n++
		}
		return nil
	})
	return n
}

// UploaderConfig returns the uploader configuration production builds from the environment
// (NewEmitterConfigFromEnv) for this scratch directory, without any storage-service credentials.
// Notably RecoveryPeriod is left at its production value.
func (p *prodScratch) UploaderConfig(t testingT) cldy.UploaderConfig {
	t.Helper()
	prev, had := os.LookupEnv("CLOUDABILITY_SCRATCH_DIR")
	if err := os.Setenv("CLOUDABILITY_SCRATCH_DIR", p.Dir); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	defer func() {
		if had {
			_ = os.Setenv("CLOUDABILITY_SCRATCH_DIR", prev)
		} else {
			_ = os.Unsetenv("CLOUDABILITY_SCRATCH_DIR")
		}
	}()
	config, err := cldy.NewEmitterConfigFromEnv()
	if err != nil {
		t.Fatalf("NewEmitterConfigFromEnv: %v", err)
	}
	return config.UploaderConfig
}

// fakeClock is a manually advanced clock for the cldy clock seams.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock { return &fakeClock{now: start} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

// fakeStorage is a StorageService that records every upload, checks that the payload is a
// complete gzip+tar archive, and fails when failOn returns an error.
type fakeStorage struct {
	mu      sync.Mutex
	calls   int
	failOn  func(call int, payload cldy.UploadPayload) error
	uploads []fakeUpload
}

type fakeUpload struct {
	FileName string
	Entries  []string // tar entry names
	Err      error    // why the archive is invalid, if it is
}

func (s *fakeStorage) Upload(payload cldy.UploadPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.failOn != nil {
		if err := s.failOn(s.calls, payload); err != nil {
			return err
		}
	}
	entries, err := readTGZ(payload.FilePath)
	s.uploads = append(s.uploads, fakeUpload{FileName: payload.FileName, Entries: entries, Err: err})
	return nil
}

func (s *fakeStorage) Uploaded() []fakeUpload {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fakeUpload(nil), s.uploads...)
}

// readTGZ reads a gzip+tar archive to the end and returns its entry names, or an error if the
// archive is truncated or corrupt.
func readTGZ(path string) (names []string, rerr error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return names, fmt.Errorf("tar: %w", err)
		}
		if _, err := io.Copy(io.Discard, tr); err != nil {
			return names, fmt.Errorf("tar entry %s: %w", hdr.Name, err)
		}
		names = append(names, hdr.Name)
	}
	// Read to EOF so the gzip checksum is verified.
	if _, err := io.Copy(io.Discard, gz); err != nil {
		return names, fmt.Errorf("gzip trailer: %w", err)
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("archive has no entries")
	}
	return names, nil
}

// readPodNames returns the metadata.name of every pod in a pods.jsonl sample file.
func readPodNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var names []string
	dec := json.NewDecoder(f)
	for dec.More() {
		var pod struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := dec.Decode(&pod); err != nil {
			return names, err
		}
		names = append(names, pod.Metadata.Name)
	}
	return names, nil
}

// missingFrom returns the elements of want that are not in have.
func missingFrom(have []string, want ...string) []string {
	set := map[string]bool{}
	for _, h := range have {
		set[h] = true
	}
	var missing []string
	for _, w := range want {
		if !set[w] {
			missing = append(missing, w)
		}
	}
	return missing
}
