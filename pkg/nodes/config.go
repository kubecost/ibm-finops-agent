package nodes

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"os"

	v1 "k8s.io/api/core/v1"
)

func NewNodeClientConfig(clusterHostURL string, forceKubeProxy bool, concurrentPollers int, insecure bool) NodeClientConfig {	

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
		ClusterHostURL: clusterHostURL,
		ForceKubeProxy: forceKubeProxy,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient: NewClient(http.Client{Transport: transport}, 0),
		InClusterClient: NewClient(http.Client{Transport: transport}, 0),
	}
}

type NodeClientConfig struct {
	ClusterHostURL     string
	ForceKubeProxy     bool
	ConcurrentPollers  int
	DirectNodeClient   Client
	InClusterClient    Client
}

// connectionOptions returns the connection methods that are allowed for this node based on config
// settings and cluster composition
func (nac NodeClientConfig) connectionOptions(n v1.Node, nd nodeFetchData) []connectionMethod {
	connectionMethods := make([]connectionMethod, 0)

	// Do not allow direct connection to fargate nodes
	if !nac.ForceKubeProxy && !isFargateNode(n) {
		directAPI, err := setupDirectNodeAPI(&n)
		if err != nil {
			// log.Printf(err.Error())
		} else {
			connectionMethods = append(connectionMethods, connectionMethod{directAPI, nac.DirectNodeClient})
		}
	}
	proxyAPI := setupProxyAPI(nac.ClusterHostURL, nd.nodeName)
	connectionMethods = append(connectionMethods, connectionMethod{proxyAPI, nac.InClusterClient})
	return connectionMethods
}