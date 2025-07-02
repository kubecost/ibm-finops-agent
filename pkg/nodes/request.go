package nodes

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
	v1 "k8s.io/api/core/v1"
)

// AttemptEndPoint will hit a specified endpoint with as many retries as it is allotted.
func (c *Client) AttemptEndPoint(method string, URL string, bearerToken string) ([]byte, error) {
	attempts := c.retries + 1

	for i := uint(0); i < attempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
		}

		data, err := c.makeRequest(method, URL, bearerToken)
		if err == nil {
			return data, nil
		}
		log.Warnf("Error making request to %s: %s", URL, err)
	}

	return nil, fmt.Errorf("requests to %v failed", URL)
}

// makeRequest will call out to an endpoint and attempt to decode the body into an existing
// data type.
func (c *Client) makeRequest(method string, URL string, bearerToken string) (data []byte, err error) {
	request, err := http.NewRequest(method, URL, nil)
	if err != nil {
		return nil, err
	}

	if bearerToken != "" {
		request.Header.Add("Authorization", "bearer "+bearerToken)
	}

	resp, err := c.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer safeClose(resp.Body.Close, &err)

	if !(resp.StatusCode >= 200 && resp.StatusCode <= 299){
		return nil, fmt.Errorf("invalid response %s", strconv.Itoa(resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading body of response: %s", err)
	}
	return body, nil
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client defines an HTTP Client with specified retries
type Client struct {
	client  HTTPClient
	retries uint
}

func NewClient(client *http.Client, retries uint) Client {
	return Client{
		client:  client,
		retries: retries,
	}
}

type connectionMethod struct {
	API    nodeAPI
	client Client
}

type nodeAPI interface {
	formatEndpoint(s string) string
}

type directNode struct {
	ip   string
	port int64
}

func (d directNode) formatEndpoint(s string) string {
	return fmt.Sprintf("https://%s:%v/%s", d.ip, d.port, s)
}

// setupDirectNodeAPI retrieves node stats directly from the node api
func setupDirectNodeAPI(n *v1.Node) (directNode, error) {
	ip, port, err := NodeAddress(n)
	if err != nil {
		return directNode{}, fmt.Errorf("problem getting node address: %s", err)
	}
	return directNodeEndpoint(ip, port), nil
}

func directNodeEndpoint(ip string, port int32) directNode {
	return directNode{
		ip:   ip,
		port: int64(port),
	}
}

type proxyAPI struct {
	clusterHostURL string
	nodeName       string
}

func (p proxyAPI) formatEndpoint(s string) string {
	return fmt.Sprintf("%s/api/v1/nodes/%s/proxy/%s", p.clusterHostURL, p.nodeName, s)
}

// setupProxyAPI retrieves node stats through the proxy
func setupProxyAPI(clusterHostURL, nodeName string) proxyAPI {
	return proxyAPI{
		clusterHostURL: clusterHostURL,
		nodeName:       nodeName,
	}
}
