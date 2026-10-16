package health

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ibm/finops-agent/pkg/condition"
)

// LivenessHandler serves /healthz: 200 when every component is live, 503 otherwise, with an
// empty body. Before any component registers it is 200. It returns within the check timeout
// whatever the components do.
func (r *Registry) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		writeEmpty(w, r.Check(req.Context()).Live)
	}
}

// StartupHandler serves /startupz: 200 once startup has finished (PhaseRunning), whether or not
// the agent is ready, 503 before, with an empty body. It checks no component, so a startupProbe
// on it covers informer sync (bounded by its timeout) and the collector WAL restore, and never
// turns an RBAC or remote fault into a restart loop.
func (r *Registry) StartupHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeEmpty(w, r.Phase() == PhaseRunning)
	}
}

// ReadinessHandler serves /readyz: 200 when startup has finished and every component is live
// with no active condition, 503 otherwise with one line per reason: the startup phase, a
// component that isn't live, or an active condition.
func (r *Registry) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		result := r.Check(req.Context())
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		if result.Ready {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprint(w, strings.Join(result.NotReadyLines(), "\n")+"\n")
	}
}

// NotReadyLines returns one line per reason the agent isn't ready.
func (res Result) NotReadyLines() []string {
	var lines []string
	if res.Phase != PhaseRunning {
		lines = append(lines, "startup: phase "+res.Phase)
	}
	for _, c := range res.Components {
		if !c.Live {
			lines = append(lines, fmt.Sprintf("%s: not live: %s", c.Name, oneLine(condition.Redact(c.NotLiveReason))))
		}
		for _, cond := range c.Conditions {
			line := c.Name + ": " + cond.Type
			if cond.Reason != "" {
				line += " (" + cond.Reason + ")"
			}
			if cond.Message != "" {
				line += ": " + oneLine(condition.Redact(cond.Message))
			}
			lines = append(lines, line)
		}
	}
	return lines
}

// StatusHandler serves /status: JSON of the startup phase and every component's liveness,
// conditions and progress. Components' Status values must hold no secrets; condition messages
// are redacted again here.
func (r *Registry) StatusHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		result := r.Check(req.Context())
		body, err := json.MarshalIndent(result.status(), "", "  ")
		if err != nil {
			http.Error(w, "failed to encode status", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(body)
	}
}

// StatusDoc is the /status document.
type StatusDoc struct {
	Phase      string            `json:"phase"`
	Live       bool              `json:"live"`
	Ready      bool              `json:"ready"`
	Components []ComponentStatus `json:"components"`
}

// ComponentStatus is one component in the /status document.
type ComponentStatus struct {
	Name          string            `json:"name"`
	Live          bool              `json:"live"`
	NotLiveReason string            `json:"notLiveReason,omitempty"`
	Ready         bool              `json:"ready"`
	Conditions    []ConditionStatus `json:"conditions,omitempty"`
	Status        any               `json:"status,omitempty"`
}

// ConditionStatus is one active condition in the /status document.
type ConditionStatus struct {
	Type    string    `json:"type"`
	Reason  string    `json:"reason,omitempty"`
	Message string    `json:"message,omitempty"`
	Since   time.Time `json:"since"`
}

func (res Result) status() StatusDoc {
	doc := StatusDoc{Phase: res.Phase, Live: res.Live, Ready: res.Ready, Components: make([]ComponentStatus, 0, len(res.Components))}
	for _, c := range res.Components {
		cs := ComponentStatus{Name: c.Name, Live: c.Live, NotLiveReason: condition.Redact(c.NotLiveReason), Ready: c.Ready(), Status: c.Status}
		for _, cond := range c.Conditions {
			cs.Conditions = append(cs.Conditions, ConditionStatus{
				Type: cond.Type, Reason: cond.Reason, Message: condition.Redact(cond.Message), Since: cond.Since,
			})
		}
		doc.Components = append(doc.Components, cs)
	}
	return doc
}

func writeEmpty(w http.ResponseWriter, ok bool) {
	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Cache-Control", "no-store")
	if ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}
