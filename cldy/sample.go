package cldy

// The finalised-sample contract.
//
// The emitter assembles each sample in a staging directory and publishes it with an atomic
// rename, so every directory under <SCRATCH_DIR>/scratch/<clusterID>/ is in one of two states:
//
//	.inprogress-<unixMilli>_<n>/   staging. Being written, or left behind by a crash or a failed
//	                               Emit. Never packaged. Removing one is an unfinalised discard,
//	                               not a data drop.
//	<unixMilli>_<n>/               finalised. Renamed from staging after MANIFEST.json was
//	                               written last and every file, the manifest and the staging
//	                               directory were fsynced. The parent is fsynced after the rename.
//
// A directory without the staging prefix is a sample only if validateSample accepts its
// manifest: one written by an older agent, or torn by the filesystem, is not. Consumers (upload
// packaging and startup recovery) list samples with listFinalisedSamples and never look inside
// staging directories. MANIFEST.json itself is left out of payloads, so the uploaded tar layout
// is unchanged.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

const (
	// stagingPrefix marks a sample directory that is not finalised.
	stagingPrefix = ".inprogress-"
	// manifestFileName is written last into a staging directory, just before the rename.
	manifestFileName = "MANIFEST.json"
	// manifestSchemaVersion is the only manifest schema validateSample accepts.
	manifestSchemaVersion = 1
	// quarantineDirName is the directory under <SCRATCH_DIR>/scratch/ where startup recovery and
	// the uploader move what they can't recover or upload. It is never a cluster ID.
	quarantineDirName = "_quarantine"
)

// sampleManifest describes a finalised sample. It lists every other file in the sample
// directory.
type sampleManifest struct {
	SchemaVersion int            `json:"schemaVersion"`
	ClusterID     string         `json:"clusterID"`
	Timestamp     time.Time      `json:"timestamp"`
	NodeCount     int            `json:"nodeCount"`
	Files         []manifestFile `json:"files"`
}

type manifestFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// totalBytes is the size of the sample's files, excluding the manifest.
func (m sampleManifest) totalBytes() int64 {
	var n int64
	for _, f := range m.Files {
		n += f.Size
	}
	return n
}

// finalisedSample is a sample directory that passed validateSample.
type finalisedSample struct {
	// Path is the sample directory with a trailing separator, the form Uploader.AddSample takes.
	Path     string
	Manifest sampleManifest
}

// sampleDirName is the finalised name of sample n taken at ts.
func sampleDirName(ts time.Time, n int) string {
	return fmt.Sprintf("%d_%d", ts.UTC().UnixMilli(), n)
}

// parseSampleDirName parses a finalised or staging sample directory name into its timestamp
// (unix milliseconds) and sequence number.
func parseSampleDirName(name string) (unixMilli int64, n int, ok bool) {
	ts, seq, found := strings.Cut(strings.TrimPrefix(name, stagingPrefix), "_")
	if !found {
		return 0, 0, false
	}
	unixMilli, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return 0, 0, false
	}
	n, err = strconv.Atoi(seq)
	if err != nil {
		return 0, 0, false
	}
	return unixMilli, n, true
}

// sampleDirs lists the sample directories directly under clusterDir, each sorted oldest first.
// finalised holds the directories without the staging prefix, validated or not. Entries whose
// names don't parse are ignored.
func sampleDirs(clusterDir string) (finalised, staging []string, err error) {
	entries, err := os.ReadDir(clusterDir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if _, _, ok := parseSampleDirName(e.Name()); !ok {
			continue
		}
		if strings.HasPrefix(e.Name(), stagingPrefix) {
			staging = append(staging, e.Name())
		} else {
			finalised = append(finalised, e.Name())
		}
	}
	sortSampleDirNames(finalised)
	sortSampleDirNames(staging)
	return finalised, staging, nil
}

func sortSampleDirNames(names []string) {
	sort.Slice(names, func(i, j int) bool {
		ti, ni, _ := parseSampleDirName(names[i])
		tj, nj, _ := parseSampleDirName(names[j])
		if ti != tj {
			return ti < tj
		}
		return ni < nj
	})
}

// listFinalisedSamples returns the finalised samples under clusterDir
// (<SCRATCH_DIR>/scratch/<clusterID>), oldest first. It checks manifests and file sizes but not
// hashes. invalid lists the directories without the staging prefix whose manifest is missing or
// doesn't validate; staging directories appear in neither list.
func listFinalisedSamples(clusterDir string) (samples []finalisedSample, invalid []string, err error) {
	names, _, err := sampleDirs(clusterDir)
	if err != nil {
		return nil, nil, err
	}
	for _, name := range names {
		dir := SafePath(clusterDir, name+string(filepath.Separator))
		m, vErr := validateSample(dir, false)
		if vErr != nil {
			log.Debugf("sample %s is not finalised: %v", name, vErr)
			invalid = append(invalid, name)
			continue
		}
		samples = append(samples, finalisedSample{Path: dir, Manifest: m})
	}
	return samples, invalid, nil
}

// validateSample reads dir's manifest and checks that it belongs to the cluster named by dir's
// parent directory and that the directory holds exactly the files it lists, at their sizes. With
// verifyHashes it also checks every file's SHA-256.
func validateSample(dir string, verifyHashes bool) (sampleManifest, error) {
	dir = filepath.Clean(dir)
	if strings.HasPrefix(filepath.Base(dir), stagingPrefix) {
		return sampleManifest{}, errors.New("staging directory")
	}
	b, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		return sampleManifest{}, err
	}
	var m sampleManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return sampleManifest{}, fmt.Errorf("manifest: %w", err)
	}
	if m.SchemaVersion != manifestSchemaVersion {
		return sampleManifest{}, fmt.Errorf("manifest schema version %d, want %d", m.SchemaVersion, manifestSchemaVersion)
	}
	if want := filepath.Base(filepath.Dir(dir)); m.ClusterID == "" || m.ClusterID != want {
		return sampleManifest{}, fmt.Errorf("manifest cluster ID %q, want %q", m.ClusterID, want)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return sampleManifest{}, err
	}
	onDisk := map[string]os.DirEntry{}
	for _, e := range entries {
		if e.Name() != manifestFileName {
			onDisk[e.Name()] = e
		}
	}
	if len(onDisk) != len(m.Files) {
		return sampleManifest{}, fmt.Errorf("manifest lists %d files, directory holds %d", len(m.Files), len(onDisk))
	}
	for _, f := range m.Files {
		e, ok := onDisk[f.Name]
		if !ok || !e.Type().IsRegular() {
			return sampleManifest{}, fmt.Errorf("file %s missing", f.Name)
		}
		info, err := e.Info()
		if err != nil {
			return sampleManifest{}, err
		}
		if info.Size() != f.Size {
			return sampleManifest{}, fmt.Errorf("file %s is %d bytes, manifest says %d", f.Name, info.Size(), f.Size)
		}
		if verifyHashes {
			_, sum, err := hashFile(filepath.Join(dir, f.Name), false)
			if err != nil {
				return sampleManifest{}, err
			}
			if sum != f.SHA256 {
				return sampleManifest{}, fmt.Errorf("file %s SHA-256 mismatch", f.Name)
			}
		}
	}
	return m, nil
}

// writeManifest hashes and fsyncs every file in dir, then writes and fsyncs MANIFEST.json
// listing them. A manifest left by an earlier attempt is replaced. It returns the manifest.
func writeManifest(dir, clusterID string, ts time.Time, nodeCount int) (m sampleManifest, rerr error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return m, err
	}
	m = sampleManifest{
		SchemaVersion: manifestSchemaVersion,
		ClusterID:     clusterID,
		Timestamp:     ts.UTC(),
		NodeCount:     nodeCount,
		Files:         []manifestFile{},
	}
	for _, e := range entries {
		if e.Name() == manifestFileName {
			continue
		}
		if !e.Type().IsRegular() {
			return m, fmt.Errorf("unexpected entry %s in sample directory", e.Name())
		}
		size, sum, err := hashFile(filepath.Join(dir, e.Name()), true)
		if err != nil {
			return m, err
		}
		m.Files = append(m.Files, manifestFile{Name: e.Name(), Size: size, SHA256: sum})
	}
	b, err := json.Marshal(m)
	if err != nil {
		return m, err
	}
	f, err := os.Create(filepath.Join(dir, manifestFileName))
	if err != nil {
		return m, err
	}
	defer safeClose(f.Close, &rerr)
	if _, err := f.Write(b); err != nil {
		return m, err
	}
	return m, f.Sync()
}

// finaliseSample publishes the staging directory staging: it writes the manifest, fsyncs the
// directory, renames it to its finalised name and fsyncs the parent. On error before the rename
// the staging directory is left as it is. Once the rename has happened the sample is finalised
// and no error is returned; a failed parent fsync is only logged. It returns the finalised path,
// with a trailing separator, and the manifest.
func finaliseSample(staging, clusterID string, ts time.Time, nodeCount int) (string, sampleManifest, error) {
	staging = filepath.Clean(staging)
	name := filepath.Base(staging)
	if !strings.HasPrefix(name, stagingPrefix) {
		return "", sampleManifest{}, fmt.Errorf("%s is not a staging directory", staging)
	}
	m, err := writeManifest(staging, clusterID, ts, nodeCount)
	if err != nil {
		return "", m, fmt.Errorf("writing sample manifest: %w", err)
	}
	if err := crashPoint(crashSampleFileWritten); err != nil {
		return "", m, err
	}
	if err := syncDir(staging); err != nil {
		return "", m, fmt.Errorf("syncing sample directory: %w", err)
	}
	if err := crashPoint(crashSampleBeforeRename); err != nil {
		return "", m, err
	}
	parent := filepath.Dir(staging)
	final := filepath.Join(parent, strings.TrimPrefix(name, stagingPrefix))
	if err := os.Rename(staging, final); err != nil {
		return "", m, fmt.Errorf("finalising sample: %w", err)
	}
	if err := crashPoint(crashSampleAfterRename); err != nil {
		log.Debugf("crash point %s: %v", crashSampleAfterRename, err)
	}
	if err := syncDir(parent); err != nil {
		log.Errorf("sample %s is finalised but syncing %s failed; it may not survive a node crash: %v", final, parent, err)
	}
	return final + string(filepath.Separator), m, nil
}

// hashFile returns a file's size and hex SHA-256, fsyncing it first when sync is set.
func hashFile(path string, sync bool) (size int64, sum string, rerr error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer safeClose(f.Close, &rerr)
	if sync {
		if err := f.Sync(); err != nil {
			return 0, "", err
		}
	}
	h := sha256.New()
	size, err = io.Copy(h, f)
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(h.Sum(nil)), nil
}

// syncDir fsyncs a directory so that entries created or renamed in it are durable.
func syncDir(dir string) (rerr error) {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer safeClose(d.Close, &rerr)
	return d.Sync()
}
