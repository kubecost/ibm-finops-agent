package cldy_test

// Upload transport, deadlines, retries, TLS scope and regions (docs/reliability/FINDINGS.md
// chunk 03: F-13, F-17, F-24, F-26, F-28, F-44, F-46). Every server here is local.

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/ibm/finops-agent/cldy"
)

// stalledServer accepts every request, reads its body and never answers until the client
// gives up or the test ends.
func stalledServer(t *testing.T) *httptest.Server {
	t.Helper()
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-done:
		}
	}))
	t.Cleanup(func() { close(done); server.Close() })
	return server
}

// F-13: an upload to a server that accepts the connection and never answers must end within
// the upload deadline, for every service type, so the upload loop keeps cycling. The custom S3
// and Azure uploads had no deadline at all.
func TestStalledServerDoesNotWedgeUploadLoop(t *testing.T) {
	services := map[string]func(server *httptest.Server, config cldy.ApptioConfig) cldy.StorageService{
		"cloudability": func(server *httptest.Server, config cldy.ApptioConfig) cldy.StorageService {
			svc := apptioService(cldy.NewApptioClient(config))
			svc.FrontdoorURL = server.URL
			svc.CloudabilityURL = server.URL
			return svc
		},
		"metrics-collector": func(server *httptest.Server, config cldy.ApptioConfig) cldy.StorageService {
			return &cldy.MetricsCollectorServiceImpl{APIKey: "key", BaseURL: server.URL + "/metricsample",
				UserAgent: "cldy-client/test", CldyUploadClient: cldy.NewApptioClient(config)}
		},
		"custom S3": func(server *httptest.Server, _ cldy.ApptioConfig) cldy.StorageService {
			return cldy.NewCustomS3ClientForTest("bucket", server.URL)
		},
		"custom Azure blob": func(server *httptest.Server, _ cldy.ApptioConfig) cldy.StorageService {
			client, err := azblob.NewClientWithNoCredential(server.URL+"/", nil)
			if err != nil {
				t.Fatal(err)
			}
			return cldy.CustomBlobClient{BlobContainerName: "container", UploadClient: &cldy.CustomBlobUploader{Uploader: client}}
		},
	}
	for name, build := range services {
		t.Run(name, func(t *testing.T) {
			server := stalledServer(t)
			scratch := newProdScratch(t, t.TempDir(), "cid-stall")
			config := scratch.UploaderConfig(t)
			config.Timeout = 200 * time.Millisecond
			config.Retries = 2
			cu := cldy.NewUploaderForTest(config, []cldy.StorageService{build(server, config.ApptioConfig)}, nil)
			cu.SetClusterID(scratch.ClusterID)
			payload := scratch.AddUpload(t, time.Now().Add(-time.Minute))
			info, err := os.Stat(payload)
			if err != nil {
				t.Fatal(err)
			}
			// Generous slack over the deadline for SDK retries and a loaded machine, but far short
			// of forever.
			bound := cldy.UploadDeadlineForTest(config.ApptioConfig, info.Size()) + 5*time.Second

			var last time.Time
			for cycle := 1; cycle <= 2; cycle++ {
				done := make(chan struct{})
				go func() { defer close(done); cu.UploadCycleForTest() }()
				select {
				case <-done:
				case <-time.After(bound):
					t.Fatalf("F-13: upload cycle %d to a server that never answers did not end within %s; the upload loop is wedged", cycle, bound)
				}
				hb := cu.UploadHeartbeat()
				if !hb.LastCycleEnd.After(last) || hb.ConsecutiveFailures != cycle {
					t.Fatalf("after cycle %d the heartbeat is %+v; want a new cycle end and %d consecutive failures", cycle, hb, cycle)
				}
				last = hb.LastCycleEnd
			}
			if got := scratch.Uploads(t); len(got) != 1 {
				t.Errorf("the undelivered payload must stay queued; upload/ holds %v", got)
			}
		})
	}
}

// sparseFile creates a file of size bytes that takes no disk space.
func sparseFile(t *testing.T, size int64) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cid_2026-01-02-03-04-05.tgz")
	f, err := os.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return p
}

// throttledPresignServer is a metrics-collector whose presigned PUT reads the body at no more
// than rate bytes per second (0: as fast as it can), then answers 200.
func throttledPresignServer(t *testing.T, rate int64) *httptest.Server {
	t.Helper()
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metricsample" {
			_ = json.NewEncoder(w).Encode(map[string]string{"location": server.URL + "/put"})
			return
		}
		start := time.Now()
		buf := make([]byte, 1<<20)
		var read int64
		for {
			n, err := r.Body.Read(buf)
			read += int64(n)
			if err != nil {
				break
			}
			if rate > 0 {
				time.Sleep(time.Until(start.Add(time.Duration(float64(read) / float64(rate) * float64(time.Second)))))
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server
}

// F-28: the whole-request HTTPS_CLIENT_TIMEOUT failed any upload slower than size/timeout. The
// deadline is sized to the payload: a 500 MB payload succeeds at the minimum throughput or
// better, and fails with a deadline error below it.
func TestLargeUploadDeadlineSizedToPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("uploads 500 MB twice")
	}
	const size = 500 << 20
	const minThroughput = 200 << 20 // bytes per second: the payload needs 2.5s at the minimum
	path := sparseFile(t, size)
	payload := cldy.UploadPayload{ClusterUID: "cid", FileName: filepath.Base(path), AgentVersion: "1.0.0", UploadHash: "hash", FilePath: path}
	config := cldy.ApptioConfig{Timeout: 500 * time.Millisecond, MinThroughput: minThroughput, Retries: 1}

	upload := func(rate int64) (time.Duration, error) {
		server := throttledPresignServer(t, rate)
		svc := &cldy.MetricsCollectorServiceImpl{APIKey: "key", BaseURL: server.URL + "/metricsample",
			UserAgent: "cldy-client/test", CldyUploadClient: cldy.NewApptioClient(config)}
		ctx, cancel := context.WithTimeout(context.Background(), cldy.UploadDeadlineForTest(config, size))
		defer cancel()
		start := time.Now()
		err := svc.Upload(ctx, payload)
		return time.Since(start), err
	}

	// 25% above the minimum: 2s, well over the 500ms HTTPS_CLIENT_TIMEOUT.
	if took, err := upload(minThroughput * 5 / 4); err != nil {
		t.Errorf("F-28: a 500 MB upload at 1.25x the minimum throughput failed after %s: %v", took, err)
	}
	// Half the minimum: 5s, past the deadline.
	took, err := upload(minThroughput / 2)
	if err == nil {
		t.Fatalf("a 500 MB upload at half the minimum throughput succeeded after %s; want a deadline error", took)
	}
	if result, _ := cldy.ClassifyUploadForTest(err); result != cldy.UploadResultTimeout {
		t.Errorf("a 500 MB upload at half the minimum throughput failed with %v (result %s); want a deadline error", err, result)
	}
	if limit := cldy.UploadDeadlineForTest(config, size) + 2*time.Second; took > limit {
		t.Errorf("the slow upload failed only after %s, past its deadline", took)
	}
}

// F-44: UPLOAD_RETRY_COUNT was normalised and never read; every request got 3 attempts.
func TestUploadRetryCountHonoured(t *testing.T) {
	restore := cldy.SetRetryBackoffForTest(func(int) time.Duration { return 0 })
	defer restore()
	for _, tc := range []struct{ retries, want int }{{1, 1}, {4, 4}, {0, 3}} {
		t.Run(fmt.Sprintf("UPLOAD_RETRY_COUNT=%d", tc.retries), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			client := cldy.NewApptioClient(cldy.ApptioConfig{Timeout: time.Second, Retries: tc.retries})
			req, err := http.NewRequest(http.MethodPost, server.URL+"/v3/internal/containers/clusters/upload", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.Do(req, "test"); err == nil {
				t.Fatal("want an error from a server that always answers 503")
			}
			if got := int(requests.Load()); got != tc.want {
				t.Errorf("F-44: UPLOAD_RETRY_COUNT=%d made %d attempts, want %d", tc.retries, got, tc.want)
			}
		})
	}
}

// Retrying stops at the deadline: a backoff doesn't sleep past it.
func TestRetryBackoffStopsAtDeadline(t *testing.T) {
	restore := cldy.SetRetryBackoffForTest(func(int) time.Duration { return time.Hour })
	defer restore()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := cldy.NewApptioClient(cldy.ApptioConfig{Timeout: time.Second, Retries: 3})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/metricsample", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := client.Do(req, "test")
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("want the deadline in the error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the retry backoff slept past the request's deadline")
	}
}

// isCertError reports whether err is a failed certificate verification.
func isCertError(err error) bool {
	var verifyErr *tls.CertificateVerificationError
	var unknownAuthority x509.UnknownAuthorityError
	return errors.As(err, &verifyErr) || errors.As(err, &unknownAuthority)
}

// connectProxy is an HTTPS proxy with a self-signed certificate. It tunnels CONNECT requests and
// answers any other (absolute-form, plain-http) request itself with 200.
func connectProxy(t *testing.T) (*httptest.Server, *url.URL) {
	t.Helper()
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("X-Via-Proxy", "yes")
			w.WriteHeader(http.StatusOK)
			return
		}
		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		conn, rw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		_, _ = rw.WriteString("HTTP/1.1 200 Connection established\r\n\r\n")
		_ = rw.Flush()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = io.Copy(upstream, rw); _ = upstream.Close() }()
		go func() { defer wg.Done(); _, _ = io.Copy(conn, upstream); _ = conn.Close() }()
		wg.Wait()
	}))
	t.Cleanup(proxy.Close)
	u, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatal(err)
	}
	return proxy, u
}

// F-24: CLOUDABILITY_OUTBOUND_PROXY_INSECURE put InsecureSkipVerify in TLSClientConfig, which Go
// uses for the destination, not the proxy. It must only ever skip verifying the proxy.
func TestProxyInsecureSkipsVerifyingOnlyTheProxy(t *testing.T) {
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer target.Close()
	_, proxyURL := connectProxy(t)
	plainTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer plainTarget.Close()

	put := func(config cldy.ApptioConfig, target string) (*http.Response, error) {
		config.Timeout = 5 * time.Second
		config.Retries = 1
		req, err := http.NewRequest(http.MethodPut, target+"/put/p.tgz", strings.NewReader("payload"))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := cldy.NewApptioClient(config).Do(req, "test")
		if resp != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		return resp, err
	}

	t.Run("direct HTTPS verifies with ProxyInsecure and no proxy", func(t *testing.T) {
		if _, err := put(cldy.ApptioConfig{ProxyInsecure: true}, target.URL); !isCertError(err) {
			t.Errorf("F-24: a direct connection to a server with an untrusted certificate gave %v, want a verification error", err)
		}
	})
	t.Run("presigned PUT under UseProxyForGettingUploadURLOnly verifies", func(t *testing.T) {
		config := cldy.ApptioConfig{ProxyURL: proxyURL, ProxyInsecure: true, UseProxyForGettingUploadURLOnly: true}
		if _, err := put(config, target.URL); !isCertError(err) {
			t.Errorf("F-24: a direct presigned-URL PUT to a server with an untrusted certificate gave %v, want a verification error", err)
		}
	})
	t.Run("the destination behind an insecure HTTPS proxy verifies", func(t *testing.T) {
		if _, err := put(cldy.ApptioConfig{ProxyURL: proxyURL, ProxyInsecure: true}, target.URL); !isCertError(err) {
			t.Errorf("F-24: a tunnel through the proxy to a server with an untrusted certificate gave %v, want a verification error", err)
		}
	})
	t.Run("the HTTPS proxy itself is not verified with ProxyInsecure", func(t *testing.T) {
		resp, err := put(cldy.ApptioConfig{ProxyURL: proxyURL, ProxyInsecure: true}, plainTarget.URL)
		if err != nil || resp.Header.Get("X-Via-Proxy") != "yes" {
			t.Errorf("a request through an HTTPS proxy with an untrusted certificate and ProxyInsecure failed: %v", err)
		}
	})
	t.Run("the HTTPS proxy is verified without ProxyInsecure", func(t *testing.T) {
		if _, err := put(cldy.ApptioConfig{ProxyURL: proxyURL}, plainTarget.URL); !isCertError(err) {
			t.Errorf("a request through an HTTPS proxy with an untrusted certificate gave %v, want a verification error", err)
		}
	})
}

// F-26: the connectivity test never closed its responses, and the JSON responses were closed
// without being drained, so no connection was reused. With every body drained and closed the
// whole test runs on one connection.
func TestConnectivityTestDrainsAndClosesResponses(t *testing.T) {
	var server *httptest.Server
	var conns atomic.Int32
	server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		switch {
		case strings.HasSuffix(r.URL.Path, "/service/apikeylogin"):
			w.Header().Set("Apptio-Opentoken", "token")
			w.Header().Set("valid_till", fmt.Sprint(time.Now().Add(time.Hour).UnixMilli()))
		case strings.HasSuffix(r.URL.Path, "/clusters/upload"), r.URL.Path == "/metricsample":
			loc := server.URL + "/put/p.tgz?X-Amz-Signature=sig"
			_ = json.NewEncoder(w).Encode(map[string]any{"location": loc, "result": map[string]string{"location": loc}})
		default:
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, "<Error><Code>SignatureDoesNotMatch</Code></Error>")
		}
	}))
	server.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			conns.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	config := cldy.ApptioConfig{Timeout: 5 * time.Second}

	t.Run("cloudability", func(t *testing.T) {
		conns.Store(0)
		svc := apptioService(cldy.NewApptioClient(config))
		svc.FrontdoorURL, svc.CloudabilityURL = server.URL, server.URL
		for range 2 {
			if err := svc.ConnectivityTestForTest(); err != nil {
				t.Fatalf("connectivity test: %v", err)
			}
		}
		if n := conns.Load(); n != 1 {
			t.Errorf("F-26: two connectivity tests (login, presign and the test PUT) took %d connections; want 1 (a response body was left open)", n)
		}
	})
	t.Run("metrics-collector", func(t *testing.T) {
		conns.Store(0)
		svc := &cldy.MetricsCollectorServiceImpl{APIKey: "key", BaseURL: server.URL + "/metricsample",
			UserAgent: "cldy-client/test", CldyUploadClient: cldy.NewApptioClient(config)}
		for range 2 {
			if err := svc.ConnectivityTestForTest(); err != nil {
				t.Fatalf("connectivity test: %v", err)
			}
		}
		if n := conns.Load(); n != 1 {
			t.Errorf("F-26: two connectivity tests took %d connections; want 1 (a response body was left open)", n)
		}
	})
}

// presignedLogHelperEnv makes TestPresignedURLNotLogged's child process run the uploads whose
// logs the parent checks.
const presignedLogHelperEnv = "CLDY_PRESIGNED_LOG_HELPER"

// F-24/F-26: a presigned URL's query string is a credential and must never reach the logs. The
// uploads run in a child process so that its whole log output can be checked without swapping
// the global logger under other tests' goroutines.
func TestPresignedURLNotLogged(t *testing.T) {
	if os.Getenv(presignedLogHelperEnv) == "1" {
		presignedLogHelper(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestPresignedURLNotLogged$", "-test.count=1")
	cmd.Env = append(os.Environ(), presignedLogHelperEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "PRESIGNED-HELPER-DONE") {
		t.Fatalf("helper process didn't run the uploads:\n%s", out)
	}
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		if strings.Contains(sc.Text(), "sekrit") || strings.Contains(sc.Text(), "X-Amz-Signature") {
			t.Errorf("a presigned URL's query string is in the logs: %s", sc.Text())
		}
	}
}

func presignedLogHelper(t *testing.T) {
	restore := cldy.SetRetryBackoffForTest(func(int) time.Duration { return 0 })
	defer restore()
	// The presigned URL points at a port nothing listens on, so every PUT fails with a transport
	// error naming the URL.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := l.Addr().String()
	_ = l.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = json.NewEncoder(w).Encode(map[string]string{"location": "http://" + dead + "/put/p.tgz?X-Amz-Signature=sekrit"})
	}))
	defer server.Close()
	config := cldy.ApptioConfig{Timeout: time.Second, Retries: 2}
	svc := &cldy.MetricsCollectorServiceImpl{APIKey: "key", BaseURL: server.URL + "/metricsample",
		UserAgent: "cldy-client/test", CldyUploadClient: cldy.NewApptioClient(config)}

	if err := svc.ConnectivityTestForTest(); err == nil {
		t.Error("the connectivity test against a dead presigned URL passed")
	}
	scratch := newProdScratch(t, t.TempDir(), "cid-presigned-log")
	uc := scratch.UploaderConfig(t)
	uc.ApptioConfig = config
	cu := cldy.NewUploaderForTest(uc, []cldy.StorageService{svc}, nil)
	cu.SetClusterID(scratch.ClusterID)
	scratch.AddUpload(t, time.Now().Add(-time.Minute))
	cu.UploadCycleForTest()
	fmt.Println("PRESIGNED-HELPER-DONE")
}

// F-17: one region table for the three endpoints. The hybrid-* regions deliberately use their
// own Frontdoor and the US Cloudability; an unknown region falls back to the US (D3) and says so;
// a region a path doesn't serve has no URL on that path.
func TestRegionTable(t *testing.T) {
	const (
		us   = "https://api.cloudability.com"
		usFD = "https://frontdoor.apptio.com"
		usMC = "https://metrics-collector.cloudability.com/metricsample"
	)
	fd := func(s string) string { return "https://frontdoor" + s + ".apptio.com" }
	api := func(s string) string { return "https://api" + s + ".cloudability.com" }
	mc := func(s string) string { return "https://metrics-collector" + s + ".cloudability.com/metricsample" }
	type want struct{ frontdoor, cloudability, metricsCollector string }
	known := map[string]want{
		"us":                {usFD, us, usMC},
		"us-west-2":         {usFD, us, usMC},
		"staging":           {fd("-stage"), api("-s"), mc("-staging")},
		"us-west-2-staging": {fd("-stage"), api("-s"), mc("-staging")},
		"eu":                {fd("-eu"), api("-eu"), mc("-eu")},
		"eu-central-1":      {fd("-eu"), api("-eu"), mc("-eu")},
		"au":                {fd("-au"), api("-au"), mc("-au")},
		"ap-southeast-2":    {fd("-au"), api("-au"), mc("-au")},
		"me":                {fd("-me"), api("-me"), mc("-me")},
		"me-central-1":      {fd("-me"), api("-me"), mc("-me")},
		"sg":                {fd("-sg"), api("-sg"), mc("-sg")},
		"ap-southeast-1":    {fd("-sg"), api("-sg"), mc("-sg")},
		"jp":                {fd("-jp"), api("-jp"), mc("-jp")},
		"ap-northeast-1":    {fd("-jp"), api("-jp"), mc("-jp")},
		"in":                {fd("-in"), api("-in"), mc("-in")},
		"ap-south-1":        {fd("-in"), api("-in"), mc("-in")},
		"ca":                {fd("-ca"), api("-ca"), mc("-ca")},
		"ca-central-1":      {fd("-ca"), api("-ca"), mc("-ca")},
		"gov":               {fd("-usgov"), api(".usgov"), mc("-production-gov")},
		"us-gov-west-1":     {fd("-usgov"), api(".usgov"), mc("-production-gov")},
		// No metrics-collector serves gov2: a config error on that path, never the commercial US.
		"gov2":          {fd("-usgov2"), api(".usgov2"), ""},
		"us-gov-east-1": {fd("-usgov2"), api(".usgov2"), ""},
		// Deliberate: a hybrid region's own Frontdoor, and the US Cloudability.
		"hybrid-eu": {fd("-eu"), us, usMC},
		"hybrid-au": {fd("-au"), us, usMC},
		"hybrid-me": {fd("-me"), us, usMC},
		"hybrid-sg": {fd("-sg"), us, usMC},
		"hybrid-jp": {fd("-jp"), us, usMC},
		"hybrid-in": {fd("-in"), us, usMC},
		"hybrid-ca": {fd("-ca"), us, usMC},
		// Case and surrounding space don't make a region unknown.
		"EU":    {fd("-eu"), api("-eu"), mc("-eu")},
		" eu  ": {fd("-eu"), api("-eu"), mc("-eu")},
	}
	for region, w := range known {
		f, c, m, fallback := cldy.RegionURLsForTest(region)
		if fallback {
			t.Errorf("F-17: known region %q is reported as a fallback", region)
		}
		if got := (want{f, c, m}); got != w {
			t.Errorf("F-17: region %q maps to %+v, want %+v", region, got, w)
		}
		isUS := strings.HasPrefix(region, "us") || strings.HasPrefix(region, "hybrid-")
		if !isUS && (c == us || m == usMC || f == usFD) {
			t.Errorf("F-17: non-US region %q returns a US URL: %s %s %s", region, f, c, m)
		}
	}
	for _, region := range []string{"", "mars", "eu-west-1", "hybrid-us", "us-east-1", "gov3", "stage"} {
		f, c, m, fallback := cldy.RegionURLsForTest(region)
		if !fallback {
			t.Errorf("F-17: unknown region %q is not reported as a fallback (D3)", region)
		}
		if f != usFD || c != us || m != usMC {
			t.Errorf("unknown region %q maps to %s %s %s, want the US endpoints (D3)", region, f, c, m)
		}
	}
	// MetricsCollectorURLForRegion is the documented accessor for the metrics-collector column.
	for region, w := range known {
		if got := cldy.MetricsCollectorURLForRegion(region); got != w.metricsCollector {
			t.Errorf("MetricsCollectorURLForRegion(%q) = %q, want %q", region, got, w.metricsCollector)
		}
	}
}

// D3: an unknown region on a Cloudability upload path raises region_fallback. It doesn't apply
// to a custom bucket, which ignores the region.
func TestUnknownRegionRaisesRegionFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config func(*cldy.UploaderConfig)
		want   bool
	}{
		{"frontdoor, unknown region", func(c *cldy.UploaderConfig) { c.EnvID, c.Region = "env", "mars" }, true},
		{"metrics-collector, unknown region", func(c *cldy.UploaderConfig) {
			c.APIKeySecretManager, c.Region = cldy.NewValueSecretManager("key"), "mars"
		}, true},
		{"frontdoor, known region", func(c *cldy.UploaderConfig) { c.EnvID, c.Region = "env", "eu" }, false},
		{"frontdoor, hybrid region", func(c *cldy.UploaderConfig) { c.EnvID, c.Region = "env", "hybrid-eu" }, false},
		{"custom S3, unknown region", func(c *cldy.UploaderConfig) {
			c.CustomS3UploadBucket, c.CustomS3UploadRegion, c.Region = "b", "eu-west-1", "mars"
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scratch := newProdScratch(t, t.TempDir(), "cid-region")
			config := scratch.UploaderConfig(t)
			tc.config(&config)
			// No service is built, so nothing leaves the process.
			cu := cldy.NewUploaderForTest(config, nil, nil)
			if got := cu.EventsForTest().Conditions[cldy.ConditionRegionFallback]; got != tc.want {
				t.Errorf("condition %s = %v, want %v", cldy.ConditionRegionFallback, got, tc.want)
			}
		})
	}
}

// CLOUDABILITY_UPLOAD_MIN_THROUGHPUT_KBPS sets the slowest upload allowed to finish; it defaults
// to 256 KiB/s and must be at least 1.
func TestMinThroughputFromEnv(t *testing.T) {
	scratch := newProdScratch(t, t.TempDir(), "cid-throughput")
	if got := scratch.UploaderConfig(t).MinThroughput; got != 256<<10 {
		t.Errorf("default MinThroughput = %d, want %d", got, 256<<10)
	}
	t.Setenv("CLOUDABILITY_UPLOAD_MIN_THROUGHPUT_KBPS", "512")
	if got := scratch.UploaderConfig(t).MinThroughput; got != 512<<10 {
		t.Errorf("MinThroughput = %d, want %d", got, 512<<10)
	}
	t.Setenv("CLOUDABILITY_UPLOAD_MIN_THROUGHPUT_KBPS", "0")
	if _, err := cldy.NewEmitterConfigFromEnv(); err == nil {
		t.Error("CLOUDABILITY_UPLOAD_MIN_THROUGHPUT_KBPS=0 must be rejected")
	}
}

// F-17: a region one path doesn't serve is a configuration error on that path, naming both; it
// is never sent to the commercial US endpoint. The error comes before any request.
func TestRegionUnservedByPathIsConfigError(t *testing.T) {
	_, err := cldy.NewMetricsCollectorService(cldy.ApptioConfig{APIKeySecretManager: cldy.NewValueSecretManager("key"), Region: "gov2"})
	if err == nil || !strings.Contains(err.Error(), `"gov2"`) || !strings.Contains(err.Error(), "metrics-collector") {
		t.Errorf("metrics-collector with region gov2 gave %v; want a configuration error naming the region and the path", err)
	}
}
