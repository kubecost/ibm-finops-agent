package cldy

import (
	"context"
	"testing"
	"time"
)

type nopStorage struct{}

func (nopStorage) Upload(context.Context, UploadPayload) error { return nil }

// Ported from #240: a storage service whose construction failed at startup is rebuilt, at most
// once per UploadFrequency, instead of leaving uploads disabled until the next restart.
func TestRetryStorageServices(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	cu, err := newCldyUploader(UploaderConfig{ScratchDir: t.TempDir(), UploadFrequency: 10 * time.Minute}, nil, clock, nil)
	if err != nil {
		t.Fatal(err)
	}

	builds := 0
	succeed := false
	cu.rebuildServices = func() ([]StorageService, storageServiceProblems) {
		builds++
		if !succeed {
			return nil, storageServiceProblems{misconfigured: true}
		}
		return []StorageService{nopStorage{}}, storageServiceProblems{}
	}
	cu.lastServiceBuild = now

	cu.retryStorageServices()
	if builds != 0 {
		t.Fatalf("rebuilt %d times inside the cooldown, want 0", builds)
	}

	now = now.Add(10 * time.Minute)
	cu.retryStorageServices()
	if builds != 1 || len(cu.StorageServices) != 0 {
		t.Fatalf("after one failed rebuild: builds=%d services=%d, want 1 and 0", builds, len(cu.StorageServices))
	}

	now = now.Add(time.Minute)
	cu.retryStorageServices()
	if builds != 1 {
		t.Fatalf("the cooldown must run from the last build: builds=%d, want 1", builds)
	}

	succeed = true
	now = now.Add(10 * time.Minute)
	cu.retryStorageServices()
	if builds != 2 || len(cu.StorageServices) != 1 {
		t.Fatalf("after a successful rebuild: builds=%d services=%d, want 2 and 1", builds, len(cu.StorageServices))
	}
	if cu.rebuildServices != nil {
		t.Error("rebuildServices should be cleared once a service is built")
	}

	now = now.Add(time.Hour)
	cu.retryStorageServices()
	if builds != 2 {
		t.Errorf("rebuilt a working service: builds=%d, want 2", builds)
	}
}

// An uploader with no rebuild hook (nothing was selected, or construction succeeded) never
// rebuilds.
func TestRetryStorageServicesWithoutHook(t *testing.T) {
	cu, err := newCldyUploader(UploaderConfig{ScratchDir: t.TempDir(), UploadFrequency: time.Minute}, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	cu.retryStorageServices()
	if len(cu.StorageServices) != 0 {
		t.Error("services appeared without a rebuild hook")
	}
}
