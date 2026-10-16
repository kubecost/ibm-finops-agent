package cldy

import (
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

// F-26: generateSampleKey checked for 5 segments but indexed a sixth, so a file name with
// exactly 5 timestamp segments panicked.
func FuzzGenerateSampleKey(f *testing.F) {
	for _, seed := range []string{
		"cid_2025-05-05-18-05-00.tgz",
		"cid_2025-05-05-18-05",
		"cid_1-2-3-4-5",
		"cid_",
		"_",
		"",
		"a_b_c-d-e-f-g-h",
	} {
		f.Add(seed, "cid")
	}
	f.Fuzz(func(t *testing.T, fileName, clusterUID string) {
		key, err := generateSampleKey(fileName, clusterUID)
		if err == nil && !strings.HasPrefix(key, "production/data/metrics-agent/") {
			t.Errorf("generateSampleKey(%q) = %q", fileName, key)
		}
	})
}

func TestGenerateSampleKeyNeedsSixSegments(t *testing.T) {
	if _, err := generateSampleKey("cid_2025-05-05-18-05", "cid"); err == nil {
		t.Error("a name with 5 timestamp segments must be an error")
	}
	key, err := generateSampleKey("cid_2025-05-05-18-05-00.tgz", "cid")
	if err != nil || key != "production/data/metrics-agent/2025/05/05/cid/cid-20250505-18-05.tgz" {
		t.Errorf("got %q, %v", key, err)
	}
}

// F-46: the upload transports were built from scratch, dropping ProxyFromEnvironment, the dial
// and idle timeouts and HTTP/2. F-28: http.Client.Timeout covered the whole body transfer.
func TestUploadTransportClonesDefault(t *testing.T) {
	client := NewApptioClient(ApptioConfig{Timeout: 10 * time.Second})
	if client.client.Timeout != 0 {
		t.Errorf("F-28: http.Client.Timeout is %s; the upload deadline comes from the request context", client.client.Timeout)
	}
	tr, ok := client.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport is %T", client.client.Transport)
	}
	if tr.Proxy == nil || reflect.ValueOf(tr.Proxy).Pointer() != reflect.ValueOf(http.ProxyFromEnvironment).Pointer() {
		t.Error("F-46: without CLOUDABILITY_OUTBOUND_PROXY the transport must honour HTTPS_PROXY and NO_PROXY")
	}
	if tr.DialContext == nil || tr.IdleConnTimeout == 0 || !tr.ForceAttemptHTTP2 {
		t.Errorf("F-46: the transport lost the default dialer, idle timeout or HTTP/2: dial=%v idle=%s h2=%v",
			tr.DialContext != nil, tr.IdleConnTimeout, tr.ForceAttemptHTTP2)
	}
	if tr.TLSClientConfig != nil && tr.TLSClientConfig.InsecureSkipVerify {
		t.Error("F-24: InsecureSkipVerify is set for every destination")
	}
}

// D3: agent-measurement.json says when the region fell back to the US, so IBM sees it too.
func TestAgentMeasurementReportsRegionFallback(t *testing.T) {
	for region, want := range map[string]string{"mars": "true", "eu": "false"} {
		dir := t.TempDir()
		clusterID := "cid"
		ce := &Emitter{
			startTime:         time.Now().UTC(),
			emissionInterval:  time.Minute,
			currentSamplePath: dir + "/",
			ClusterID:         &clusterID,
		}
		ce.config.EnvID = "env"
		ce.config.Region = region
		if err := ce.writeAgentFile(); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(dir + "/agent-measurement.json")
		if err != nil {
			t.Fatal(err)
		}
		var agent agentData
		if err := json.Unmarshal(raw, &agent); err != nil {
			t.Fatal(err)
		}
		if got := agent.Values["region_fallback"]; got != want {
			t.Errorf("region %q: agent-measurement region_fallback = %q, want %q", region, got, want)
		}
	}
}
