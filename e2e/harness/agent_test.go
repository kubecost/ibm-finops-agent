package harness

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func agentService(name, release string) *corev1.Service {
	return &corev1.Service{
		Name:      name,
		Namespace: testNS,
		Labels:    map[string]string{"app.kubernetes.io/name": "finops-agent", "app.kubernetes.io/instance": release},
	}
}

// The Service is found by label for this release, like the Deployment.
func TestAgentService(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		objs    []runtime.Object
		want    string
		wantErr string
	}{
		{name: "one service", objs: []runtime.Object{agentService("unified-agent-finops-agent", testRelease)}, want: "unified-agent-finops-agent"},
		{name: "other release ignored", objs: []runtime.Object{agentService("x-finops-agent", "x")}, wantErr: "found 0"},
		{name: "ambiguous", objs: []runtime.Object{agentService("a", testRelease), agentService("b", testRelease)}, wantErr: "found 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := Agent{Client: k8sfake.NewClientset(tt.objs...), Namespace: testNS, Release: testRelease}
			got, err := a.service(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("service = %q, err = %v; want %q", got, err, tt.want)
			}
		})
	}
}

// Non-2xx responses arrive as client-go errors; their code must come back out.
func TestHTTPStatus(t *testing.T) {
	t.Parallel()
	gr := schema.GroupResource{Resource: "services"}
	tests := []struct {
		name   string
		err    error
		want   int
		wantOK bool
	}{
		{name: "503 from the agent", err: apierrors.NewGenericServerResponse(503, "get", gr, "svc", "", 0, true), want: 503, wantOK: true},
		{name: "404", err: apierrors.NewNotFound(gr, "svc"), want: 404, wantOK: true},
		{name: "wrapped", err: fmt.Errorf("proxy: %w", apierrors.NewGenericServerResponse(500, "get", gr, "svc", "", 0, true)), want: 500, wantOK: true},
		{name: "no response", err: errors.New("dial tcp: connection refused")},
		{name: "deadline", err: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := httpStatus(tt.err)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("httpStatus = %d, %v; want %d, %v", got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestParseMetrics(t *testing.T) {
	t.Parallel()
	const body = `# HELP go_goroutines Number of goroutines that currently exist.
# TYPE go_goroutines gauge
go_goroutines 42
# HELP http_requests_total Requests.
# TYPE http_requests_total counter
http_requests_total{code="200"} 7
`
	families, err := ParseMetrics([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	g := families["go_goroutines"]
	if g == nil || g.GetMetric()[0].GetGauge().GetValue() != 42 {
		t.Fatalf("go_goroutines = %v", g)
	}
	if c := families["http_requests_total"]; c == nil || c.GetMetric()[0].GetCounter().GetValue() != 7 {
		t.Fatalf("http_requests_total = %v", c)
	}

	for name, bad := range map[string]string{
		"empty":          "  \n",
		"not prometheus": "<html>502 Bad Gateway</html>",
		"bad value":      "go_goroutines lots\n",
	} {
		if _, err := ParseMetrics([]byte(bad)); err == nil {
			t.Errorf("%s: expected a parse error", name)
		}
	}
}
