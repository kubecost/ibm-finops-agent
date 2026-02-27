package nodes

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

const DefaultHttpClientTimeout = 10 * time.Second

func safeClose(closer func() error, err *error) {
	if closeErr := closer(); closeErr != nil && *err == nil {
		*err = closeErr
	}
}

func newHttpClient(transport *http.Transport) *http.Client {
	return &http.Client{
		Transport: transport,
		Timeout:   DefaultHttpClientTimeout,
	}
}

func transportWithTLSConfig(tlsConfig *tls.Config) *http.Transport {
	transport := newDefaultHttpTransport()
	transport.TLSClientConfig = tlsConfig
	return transport
}

// attempt to return the default http transport from the http package. If the go std library changes
// the implementation, this will return a hard coded transport.
func newDefaultHttpTransport() *http.Transport {
	// hot path
	if transport, ok := http.DefaultTransport.(*http.Transport); ok {
		return transport.Clone()
	}

	// cold path
	// If we're here, it means the go std library changed the underlying default transport implementation
	// and we return hard coded timeouts and other defaults.
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
}
