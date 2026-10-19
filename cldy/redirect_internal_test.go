package cldy

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Ported from #240: the upload client must never follow a redirect, or it would replay the
// payload (and the API key) to whatever host the 3xx names.
func TestApptioClientRefusesRedirects(t *testing.T) {
	var elsewhereHits atomic.Int32
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhereHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer elsewhere.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+"/stolen", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	client := NewApptioClient(ApptioConfig{Timeout: 10 * time.Second})
	body := []byte("payload")
	req, err := http.NewRequest(http.MethodPut, origin.URL+"/presigned", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }

	resp, err := client.client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		t.Errorf("status = %d, want the 307 handed back unfollowed", resp.StatusCode)
	}
	if n := elsewhereHits.Load(); n != 0 {
		t.Errorf("the redirect target was hit %d times, want 0", n)
	}
}
