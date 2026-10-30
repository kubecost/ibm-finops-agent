package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// agentServicePort is the name of the Service port the chart maps to the
// agent's HTTP server (container port 9003), which serves /healthz,
// /version and /metrics.
const agentServicePort = "http"

// Agent reaches the agent's HTTP server through the API server's service
// proxy. That avoids a port-forward: no background process, no local port,
// nothing to clean up if the suite is killed.
type Agent struct {
	Client    kubernetes.Interface
	Namespace string
	Release   string
}

// Response is an HTTP response from the agent.
type Response struct {
	StatusCode int
	Body       []byte
}

// Get requests path (e.g. "/healthz") from the agent. A non-2xx status is
// returned as a Response, not an error; err is only for failing to reach it.
func (a Agent) Get(ctx context.Context, path string) (Response, error) {
	svc, err := a.service(ctx)
	if err != nil {
		return Response{}, err
	}
	body, err := a.Client.CoreV1().RESTClient().Get().
		Namespace(a.Namespace).
		Resource("services").
		Name(svc + ":" + agentServicePort).
		SubResource("proxy").
		Suffix(path).
		DoRaw(ctx)
	if err == nil {
		return Response{StatusCode: http.StatusOK, Body: body}, nil
	}
	// client-go turns a non-2xx response into an error carrying its status
	// code; give that back as a Response so callers can assert on it.
	if code, ok := httpStatus(err); ok {
		return Response{StatusCode: code, Body: body}, nil
	}
	// No HTTP response at all (connection refused, context expired, ...).
	return Response{}, fmt.Errorf("GET %s from agent: %w", path, err)
}

// httpStatus returns the HTTP status code carried by a client-go error.
func httpStatus(err error) (int, bool) {
	var s apierrors.APIStatus
	if errors.As(err, &s) && s.Status().Code > 0 {
		return int(s.Status().Code), true
	}
	return 0, false
}

// service returns the name of the agent's Service. It is found by label, like
// the Deployment, because the chart's naming depends on the release name.
func (a Agent) service(ctx context.Context) (string, error) {
	sel := AgentSelector(a.Release)
	list, err := a.Client.CoreV1().Services(a.Namespace).List(ctx, metav1.ListOptions{LabelSelector: sel})
	if err != nil {
		return "", fmt.Errorf("find agent service: %w", err)
	}
	if len(list.Items) != 1 {
		return "", fmt.Errorf("find agent service: want 1 service matching %q in %s, found %d", sel, a.Namespace, len(list.Items))
	}
	return list.Items[0].Name, nil
}

// ParseMetrics parses a /metrics body in the Prometheus text format, keyed by
// metric name.
func ParseMetrics(body []byte) (map[string]*dto.MetricFamily, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, fmt.Errorf("parse metrics: empty body")
	}
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("parse metrics: %w", err)
	}
	return families, nil
}

// TrimmedBody returns the response body as a string without surrounding
// whitespace, for small text endpoints such as /version.
func (r Response) TrimmedBody() string {
	return strings.TrimSpace(string(r.Body))
}
