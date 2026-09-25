package http

// The HTTP server hardening of docs/reliability/FINDINGS.md chunk 10 (F-24, server part): every
// listener has timeouts, CORS is not enabled (D6), and pprof is not on the probe port.

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServerHasTimeouts(t *testing.T) {
	srv := NewHttpServer(http.NotFoundHandler(), 9003)
	for name, d := range map[string]time.Duration{
		"ReadHeaderTimeout": srv.ReadHeaderTimeout,
		"ReadTimeout":       srv.ReadTimeout,
		"WriteTimeout":      srv.WriteTimeout,
		"IdleTimeout":       srv.IdleTimeout,
	} {
		if d <= 0 {
			t.Errorf("F-24: %s is %s; every listener needs a timeout", name, d)
		}
	}
	if srv.ReadHeaderTimeout > 30*time.Second {
		t.Errorf("ReadHeaderTimeout %s is too long to stop a slowloris client", srv.ReadHeaderTimeout)
	}
}

// A client that sends part of a request header and then stalls is disconnected once
// ReadHeaderTimeout passes, rather than holding the connection open forever.
func TestSlowlorisIsCutOffByReadHeaderTimeout(t *testing.T) {
	srv := NewHttpServer(http.NotFoundHandler(), 0)
	if srv.ReadHeaderTimeout <= 0 {
		t.Fatalf("F-24: ReadHeaderTimeout is not set, so a slowloris client is never cut off")
	}
	srv.ReadHeaderTimeout = 200 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "GET /healthz HTTP/1.1\r\nHost: agent\r\nX-Slow: "); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = io.ReadAll(conn)
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("connection still open after %s with an incomplete header", time.Since(start).Round(time.Millisecond))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("connection closed after %s, want about the 200ms ReadHeaderTimeout", elapsed)
	}
}

// D6: nothing browser-based calls the agent, so no CORS headers are sent, even for a preflight.
func TestNoCORSHeaders(t *testing.T) {
	srv := NewHttpServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), 0)

	for _, method := range []string{http.MethodOptions, http.MethodGet} {
		req := httptest.NewRequest(method, "/healthz", nil)
		req.Header.Set("Origin", "https://evil.example")
		req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		rec := httptest.NewRecorder()
		srv.Handler.ServeHTTP(rec, req)
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("%s: Access-Control-Allow-Origin = %q, want none (D6)", method, got)
		}
	}
}

// pprof is served on its own loopback-only listener, with a WriteTimeout long enough for a
// profile, and never on the probe port.
func TestPprofServer(t *testing.T) {
	main := NewHttpServer(http.NotFoundHandler(), 0)
	rec := httptest.NewRecorder()
	main.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/cmdline", nil))
	if rec.Code == http.StatusOK {
		t.Errorf("F-24: pprof is served on the main listener")
	}

	pp := NewPprofServer(6060)
	host, _, err := net.SplitHostPort(pp.Addr)
	if err != nil || host != "127.0.0.1" {
		t.Errorf("pprof Addr = %q, want a 127.0.0.1 address", pp.Addr)
	}
	if pp.ReadHeaderTimeout <= 0 || pp.WriteTimeout <= main.WriteTimeout {
		t.Errorf("pprof timeouts: ReadHeaderTimeout %s, WriteTimeout %s; want both set, WriteTimeout longer than the main %s",
			pp.ReadHeaderTimeout, pp.WriteTimeout, main.WriteTimeout)
	}
	rec = httptest.NewRecorder()
	pp.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/cmdline", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("GET /debug/pprof/cmdline on the pprof server = %d, want 200", rec.Code)
	}
}
