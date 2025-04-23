package nodes

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"os"

	"github.com/ibm/finops-agent/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"

	v1 "k8s.io/api/core/v1"
)

func NewNodeClientConfigFromEnv() NodeClientConfig {
	return NewNodeClientConfig(
		NodeClientProxyConfig{
			ForceKubeProxy: env.IsNodeStatsForceKubeProxy(),
			LocalProxy:     env.GetNodeStatsLocalProxy(),
		},
		env.GetNodeStatsConcurrentPollers(),
		env.IsNodeStatsInsecureSkipVerify(),
	)
}

func NewNodeClientConfig(proxyConfig NodeClientProxyConfig, concurrentPollers int, insecure bool) NodeClientConfig {
	var transport *http.Transport
	if insecure {
		transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	} else {
		pemData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
		if err != nil {
			log.Fatalf("Could not load CA certificate: %v", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(pemData)

		tlsConfig := &tls.Config{
			RootCAs: caCertPool,
		}
		transport = &http.Transport{TLSClientConfig: tlsConfig}
	}

	return NodeClientConfig{
		ProxyConfig:       proxyConfig,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient:  NewClient(http.Client{Transport: transport}, 0),
		InClusterClient:   NewClient(http.Client{Transport: transport}, 0),
	}
}

type NodeClientProxyConfig struct {
	ForceKubeProxy bool
	LocalProxy     string
}

func (nac NodeClientProxyConfig) IsLocalProxy() bool {
	return nac.LocalProxy != ""
}

type NodeClientConfig struct {
	ProxyConfig       NodeClientProxyConfig
	ConcurrentPollers int
	DirectNodeClient  Client
	InClusterClient   Client
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
