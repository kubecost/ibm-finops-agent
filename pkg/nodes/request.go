package nodes

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// AttemptEndPoint will hit a specified endpoint with as many retries as it is allotted.
func (c *Client) AttemptEndPoint(method string, URL string) (interface{}, error) {
	attempts := c.retries + 1

	for i := uint(0); i < attempts; i++ {
		if i > 0 {
			time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
		}

		data, err := makeRequest(c, method, URL)
		if err == nil {
			return data, nil
		}
	}
	err := fmt.Errorf("requests to %v failed", URL)
	return nil, err
}

// makeRequest will call out to an endpoint and attempt to decode the body into an existing
// data type.
func makeRequest(c *Client, method string, URL string) (interface{}, error) {
	request, err := http.NewRequest(method, URL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, err
	}

	// In the instance where there is no body to the response
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if !(resp.StatusCode >= 200 && resp.StatusCode <= 299) {
		return nil, fmt.Errorf("invalid response %s", strconv.Itoa(resp.StatusCode))
	}

	// Note: Type assertion was failing when decoding to an empty interface
	data := &stats.Summary{}
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err == nil {
		return data, nil
	}

	// TODO: Implement cAdvisor decoding like above

	return nil, fmt.Errorf("data schema did not fit any existing sources")
}

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Client defines an HTTP Client with specified retries
type Client struct {
	HTTPClient HTTPClient
	retries    uint
}

func NewClient(HTTPClient http.Client, retries uint) Client {
	return Client{
		HTTPClient: &HTTPClient,
		retries:    retries,
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