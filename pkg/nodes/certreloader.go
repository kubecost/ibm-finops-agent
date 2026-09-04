package nodes

import (
	"crypto/tls"
	"fmt"
	"os"
	"sync"
	"time"
)

// clientCertReloader provides the current client certificate for mutual TLS handshakes,
// reloading it from disk when the underlying key file changes.
//
// The static tls.Config.Certificates field is read from memory on every handshake but
// never re-reads the files on disk, so a rotated client certificate (e.g. rotated by
// cert-manager or the kubelet) is not picked up until the process restarts. Wiring this
// type's GetClientCertificate into tls.Config lets Go fetch the current certificate on
// each new handshake, reloading only when the file's modification time advances.
type clientCertReloader struct {
	certFile, keyFile string

	mu      sync.Mutex
	cached  *tls.Certificate
	modTime time.Time
}

// newClientCertReloader creates a reloader for the given cert/key pair.
func newClientCertReloader(certFile, keyFile string) *clientCertReloader {
	return &clientCertReloader{certFile: certFile, keyFile: keyFile}
}

// GetClientCertificate matches the tls.Config.GetClientCertificate signature. Go invokes it
// on each new handshake when the server requests a client certificate. It returns the
// current certificate, reloading from disk only when the key file has changed.
func (r *clientCertReloader) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, err := os.Stat(r.keyFile)
	if err != nil {
		if r.cached != nil {
			// Stat failed but we have a usable cert in memory; keep serving it.
			return r.cached, nil
		}
		return nil, fmt.Errorf("stat client key %s: %w", r.keyFile, err)
	}

	// Reload only if the file changed (or on first load).
	if r.cached == nil || info.ModTime().After(r.modTime) {
		cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
		if err != nil {
			if r.cached != nil {
				// Fall back to the last-good cert during a partial write mid-rotation.
				return r.cached, nil
			}
			return nil, fmt.Errorf("loading client cert: %w", err)
		}
		r.cached = &cert
		r.modTime = info.ModTime()
	}

	return r.cached, nil
}
