package nodes

import (
	"crypto/tls"
	"net/http"
)

func NewKubeAgentConfig(clusterHostURL string, forceKubeProxy bool, concurrentPollers int, insecure bool) KubeAgentConfig {	
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: insecure,
		},
	}

	return KubeAgentConfig{
		ClusterHostURL: clusterHostURL,
		ForceKubeProxy: forceKubeProxy,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient: NewClient(http.Client{Transport: transport}, 0),
		InClusterClient: NewClient(http.Client{Transport: transport}, 0),
	}
}

type KubeAgentConfig struct {
	ClusterHostURL     string
	ForceKubeProxy     bool
	ConcurrentPollers  int
	DirectNodeClient   Client
	InClusterClient    Client
}