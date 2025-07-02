package nodes

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"k8s.io/client-go/rest"
	"net/http"
	"os"
	"strings"

	"github.com/ibm/finops-agent/pkg/env"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/spf13/viper"

	v1 "k8s.io/api/core/v1"
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

	thisConfig, err := rest.InClusterConfig()
	if err != nil {
		log.Warnf("failed getting in-cluster config: %s", err.Error())
	}
	// use in cluster cert if certs are not supplied through env
	if thisConfig != nil && certFile == "" && keyFile == "" {
		certFile = thisConfig.CertFile
		keyFile = thisConfig.KeyFile
	}

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
		}
	} else {
		pemData, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
		if err != nil {
			log.Fatalf("Could not load CA certificate: %v", err)
		}
		log.Infof("pulled cert from ca.crt on filesystem")

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(pemData)

		var tlsConfig *tls.Config

		if certFile != "" || keyFile != "" {
			log.Infof("Found either a cert file or a key file so loading that")
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)

			if err != nil {
				log.Fatalf("Unable to load cert: %s key: %s error: %v", certFile, keyFile, err)
			}

			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      caCertPool,
			}

			transport = &http.Transport{TLSClientConfig: tlsConfig}
		} else {
			log.Infof("found no certFile so just setting the caCertPool from defaults")
			tlsConfig = &tls.Config{
				RootCAs: caCertPool,
			}
			transport = &http.Transport{TLSClientConfig: tlsConfig}
		}
	}

	return NodeClientConfig{
		ClusterName:       clusterName,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient:  NewClient(&http.Client{Transport: transport}, 0),
		InClusterClient:   NewClient(&http.Client{Transport: transport}, 0),
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
