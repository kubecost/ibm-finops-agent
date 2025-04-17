package nodes

import (
	"crypto/tls"
	"net/http"

	v1 "k8s.io/api/core/v1"
)

func NewNodeClientConfig(forceKubeProxy bool, concurrentPollers int, insecure bool) NodeClientConfig {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: insecure,
		},
	}

	return NodeClientConfig{
		ForceKubeProxy: forceKubeProxy,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient: NewClient(http.Client{Transport: transport}, 0),
		InClusterClient: NewClient(http.Client{Transport: transport}, 0),
	}
}

type NodeClientConfig struct {
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
	proxyAPI := setupProxyAPI(nd.ClusterHostURL, nd.nodeName)
	connectionMethods = append(connectionMethods, connectionMethod{proxyAPI, nac.InClusterClient})
	return connectionMethods
}