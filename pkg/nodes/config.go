package nodes

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/ibm/finops-agent/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/spf13/viper"

	v1 "k8s.io/api/core/v1"
)

const (
	defaultHTTPTimeout           = 10 * time.Second
	defaultMaxIdleConns          = 0
	defaultMaxIdleConnsPerHost   = 2
	defaultIdleConnTimeout       = 30 * time.Second
	defaultTLSHandshakeTimeout   = 5 * time.Second
	defaultExpectContinueTimeout = 500 * time.Millisecond
)

func NewNodeClientConfigFromEnv() (NodeClientConfig, error) {
	viper.AutomaticEnv()
	clusterName := env.GetNodeStatsClusterIDName()
	concurrentPollers := env.GetNodeStatsConcurrentPollers()
	insecure := env.IsNodeStatsInsecure()
	certFile := env.GetNodeStatsCertFile()
	keyFile := env.GetNodeStatsKeyFile()
	forceKubeProxy := env.IsNodeStatsForceKubeProxy()
	localProxy := env.GetNodeStatsLocalProxy()

	if strings.TrimSpace(clusterName) == "" {
		return NodeClientConfig{}, fmt.Errorf("cluster name is required and cannot be exclusively whitespace")
	}

	if concurrentPollers <= 0 {
		return NodeClientConfig{}, fmt.Errorf("number of concurrent pollers is either zero or misconfigured")
	}

	var transport *http.Transport
	if insecure {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			MaxIdleConns:          defaultMaxIdleConns,
			MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
			IdleConnTimeout:       defaultIdleConnTimeout,
			TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
			ExpectContinueTimeout: defaultExpectContinueTimeout,
		}
	} else {
		pemData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
		if err != nil {
			log.Fatalf("could not load CA certificate: %v", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(pemData)

		var tlsConfig *tls.Config

		if certFile != "" && keyFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)

			if err != nil {
				log.Fatalf("Unable to load cert: %s key: %s error: %v", certFile, keyFile, err)
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      caCertPool,
			}

			transport = &http.Transport{
				TLSClientConfig:       tlsConfig,
				MaxIdleConns:          defaultMaxIdleConns,
				MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
				IdleConnTimeout:       defaultIdleConnTimeout,
				TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
				ExpectContinueTimeout: defaultExpectContinueTimeout,
			}
		} else {
			tlsConfig = &tls.Config{
				RootCAs: caCertPool,
			}
			transport = &http.Transport{
				TLSClientConfig:       tlsConfig,
				MaxIdleConns:          defaultMaxIdleConns,
				MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
				IdleConnTimeout:       defaultIdleConnTimeout,
				TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
				ExpectContinueTimeout: defaultExpectContinueTimeout,
			}
		}
	}

	return NodeClientConfig{
		ClusterName:       clusterName,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient:  NewClient(&http.Client{Transport: transport, Timeout: defaultHTTPTimeout}, 0),
		InClusterClient:   NewClient(&http.Client{Transport: transport, Timeout: defaultHTTPTimeout}, 0),
		ProxyConfig: NodeClientProxyConfig{
			ForceKubeProxy: forceKubeProxy,
			LocalProxy:     localProxy,
		},
	}, nil
}

type NodeClientProxyConfig struct {
	ForceKubeProxy bool
	LocalProxy     string
}

func (nac NodeClientProxyConfig) IsLocalProxy() bool {
	return nac.LocalProxy != ""
}

type NodeClientConfig struct {
	ClusterName       string
	ConcurrentPollers int
	DirectNodeClient  Client
	InClusterClient   Client
	CertFile          string
	KeyFile           string
	ProxyConfig       NodeClientProxyConfig
}

// connectionOptions returns the connection methods that are allowed for this node based on config
// settings and cluster composition
func (nac NodeClientConfig) connectionOptions(n v1.Node, nd nodeFetchData) []connectionMethod {
	connectionMethods := make([]connectionMethod, 0)

	// Do not allow direct connection to fargate nodes
	if !nac.ProxyConfig.ForceKubeProxy && !isFargateNode(n) {
		directAPI, err := setupDirectNodeAPI(&n)
		if err != nil {
			log.Warnf("error reaching direct node api %s", err)
		} else {
			connectionMethods = append(connectionMethods, connectionMethod{directAPI, nac.DirectNodeClient})
		}
	}
	clusterHostURL := nd.ClusterHostURL
	if nac.ProxyConfig.IsLocalProxy() {
		clusterHostURL = nac.ProxyConfig.LocalProxy
	}

	proxyAPI := setupProxyAPI(clusterHostURL, nd.nodeName)
	connectionMethods = append(connectionMethods, connectionMethod{proxyAPI, nac.InClusterClient})
	return connectionMethods
}
