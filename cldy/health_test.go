package cldy_test

// Chunk 08 (docs/reliability/FINDINGS.md): the Cloudability emitter's health. Remote,
// credential, disk and config faults make it not ready and never fail liveness (I4).

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ibm/finops-agent/cldy"
	"github.com/ibm/finops-agent/pkg/health"
)

// checkEmitter runs one registry check of an emitter built around cu, as main wires it.
func checkEmitter(t *testing.T, cu *cldy.CldyUploader, config cldy.EmitterConfig, clock *fakeClock) health.Result {
	t.Helper()
	ce := cldy.NewEmitterForTest(config, cu, clock.Now)
	registry := health.NewRegistry()
	registry.SetClock(clock.Now)
	registry.RegisterAny(string(ce.ID()), ce)
	registry.SetPhase(health.PhaseRunning)
	return registry.Check(context.Background())
}

func TestUploadFaultsAreNotReadyNeverNotLive(t *testing.T) {
	tests := map[string]struct {
		services       func() []cldy.StorageService
		cycles         int
		wantConditions []string
	}{
		"no storage service": {
			services:       func() []cldy.StorageService { return nil },
			cycles:         1,
			wantConditions: []string{cldy.ConditionUploaderUnconfigured},
		},
		"credentials refused": {
			services: func() []cldy.StorageService {
				return []cldy.StorageService{apptioService(&scriptedClient{status: stageStatus(stageLogin, http.StatusUnauthorized)})}
			},
			cycles:         1,
			wantConditions: []string{cldy.ConditionUploadAuthFailed},
		},
		"backend unreachable for 3 cycles with a backlog": {
			services: func() []cldy.StorageService {
				return []cldy.StorageService{apptioService(failingClient{err: errConnRefused})}
			},
			cycles:         3,
			wantConditions: []string{"uploads_failing"},
		},
		"backend unreachable for 2 cycles is routine": {
			services: func() []cldy.StorageService {
				return []cldy.StorageService{apptioService(failingClient{err: errConnRefused})}
			},
			cycles: 2,
		},
		"backend rejecting every payload": {
			services: func() []cldy.StorageService {
				return []cldy.StorageService{apptioService(&scriptedClient{status: stageStatus(stagePresign, http.StatusBadRequest)})}
			},
			cycles:         3,
			wantConditions: []string{cldy.ConditionUploadsRejected, "uploads_failing"},
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			clock := newFakeClock(time.Now().Truncate(time.Second))
			scratch := newProdScratch(t, t.TempDir(), "cid-health")
			for i := range 4 {
				scratch.AddUpload(t, clock.Now().Add(time.Duration(i-4)*time.Hour))
			}
			cu := newQueueUploader(t, scratch, tt.services(), clock)
			for range tt.cycles {
				clock.Advance(10 * time.Minute)
				cu.UploadCycleForTest()
			}
			res := checkEmitter(t, cu, cldy.EmitterConfig{UploaderConfig: scratch.UploaderConfig(t)}, clock)
			if !res.Live {
				t.Fatalf("liveness failed for a fault a restart can't fix: %v", res.NotReadyLines())
			}
			var got []string
			for _, c := range res.Components[0].Conditions {
				got = append(got, c.Type)
			}
			if strings.Join(got, ",") != strings.Join(tt.wantConditions, ",") {
				t.Errorf("conditions %v, want %v", got, tt.wantConditions)
			}
			if res.Ready != (len(tt.wantConditions) == 0) {
				t.Errorf("ready = %v with conditions %v", res.Ready, got)
			}
		})
	}
}

// /status carries upload progress but none of the configured secrets.
func TestStatusHasNoSecrets(t *testing.T) {
	clock := newFakeClock(time.Now().Truncate(time.Second))
	scratch := newProdScratch(t, t.TempDir(), "cid-status")
	scratch.AddUpload(t, clock.Now().Add(-time.Hour))
	config := scratch.UploaderConfig(t)
	config.APIKeySecretManager = cldy.NewValueSecretManager("APIKEY-SECRET-1")
	config.SecretManager = cldy.NewKeyValueSecretManager("ACCESS-SECRET-2", "SECRET-SECRET-3")
	config.ProxyAuth = "user:PROXY-SECRET-4"
	config.CustomAzureClientSecret = cldy.NewValueSecretManager("AZURE-SECRET-5")
	config.OpenToken = "TOKEN-SECRET-6"
	cu := newQueueUploader(t, scratch, []cldy.StorageService{apptioService(&scriptedClient{status: stageStatus(stageLogin, http.StatusForbidden)})}, clock)
	cu.UploadCycleForTest()

	ce := cldy.NewEmitterForTest(cldy.EmitterConfig{UploaderConfig: config}, cu, clock.Now)
	registry := health.NewRegistry()
	registry.RegisterAny(string(ce.ID()), ce)
	srv := httptest.NewServer(registry.StatusHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), `"backlogFiles": 1`) || !strings.Contains(string(body), cldy.ConditionUploadAuthFailed) {
		t.Errorf("/status lacks the upload backlog or the auth condition:\n%s", body)
	}
	for _, secret := range []string{"SECRET-", "access", "secret", "token"} {
		if strings.Contains(string(body), secret) {
			t.Errorf("/status contains %q:\n%s", secret, body)
		}
	}
}
