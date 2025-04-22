package nodes

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/spf13/cast"
	"github.com/spf13/viper"

	v1 "k8s.io/api/core/v1"
)

func NewNodeClientConfig() (NodeClientConfig, error) {
	viper.AutomaticEnv()
	forceKubeProxy := getEnvValueOrDefault("FORCE_KUBE_PROXY", false, cast.ToBool)
	insecure := getEnvValueOrDefault("INSECURE", false, cast.ToBool)
	concurrentPollers := getEnvValueOrDefault("NUMBER_OF_CONCURRENT_NODE_POLLERS", 100, cast.ToInt)
	clusterName := getEnvValueOrDefault("CLUSTER_NAME", "", cast.ToString)
	certFile := getEnvValueOrDefault("CERT_FILE", "", cast.ToString)
	keyFile := getEnvValueOrDefault("KEY_FILE", "", cast.ToString)

	if strings.TrimSpace(clusterName) == "" {
		return NodeClientConfig{}, fmt.Errorf("Cluster name is required and cannot be exclusively whitespace.")
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

			transport = &http.Transport{TLSClientConfig: tlsConfig}
		} else {
			tlsConfig := &tls.Config{
				RootCAs: caCertPool,
			}
			transport = &http.Transport{TLSClientConfig: tlsConfig}
		}
	}

	return NodeClientConfig{
		ClusterName:       clusterName,
		ForceKubeProxy:    forceKubeProxy,
		ConcurrentPollers: concurrentPollers,
		DirectNodeClient:  NewClient(http.Client{Transport: transport}, 0),
		InClusterClient:   NewClient(http.Client{Transport: transport}, 0),
	}, nil
}

type NodeClientConfig struct {
	ClusterName       string
	ForceKubeProxy    bool
	ConcurrentPollers int
	DirectNodeClient  Client
	InClusterClient   Client
	CertFile          string
	KeyFile           string
}

// connectionOptions returns the connection methods that are allowed for this node based on config
// settings and cluster composition
func (nac NodeClientConfig) connectionOptions(n v1.Node, nd nodeFetchData) []connectionMethod {
	connectionMethods := make([]connectionMethod, 0)

	// Do not allow direct connection to fargate nodes
	if !nac.ForceKubeProxy && !isFargateNode(n) {
		directAPI, err := setupDirectNodeAPI(&n)
		if err != nil {
			log.Warnf("error reaching direct node api %s", err)
		} else {
			connectionMethods = append(connectionMethods, connectionMethod{directAPI, nac.DirectNodeClient})
		}
	}
	proxyAPI := setupProxyAPI(nd.ClusterHostURL, nd.nodeName)
	connectionMethods = append(connectionMethods, connectionMethod{proxyAPI, nac.InClusterClient})
	return connectionMethods
}

// getEnvValueOrDefault attempts to read the environment variable raw and then with the CLOUDABILITY_ prefix,
// converting it to the relevant type if found
func getEnvValueOrDefault[T any](envVariable string, defaultValue T, convert func(interface{}) T) T {
	const prefix = "CLOUDABILITY_"
	var envValue interface{}

	// Attempt without prefix first
	envValue = viper.Get(envVariable)
	if envValue == nil {
		// Attempt with prefix
		envValue = viper.Get(prefix + envVariable)
		if envValue == nil {
			// Set to default value
			envValue = defaultValue
		}
	}

	return convert(envValue)
}
