// Package condition holds the degraded-state conditions components raise for readiness, status
// and alerting (docs/reliability/FINDINGS.md chunk 08). A component keeps its active conditions
// in a Set and exposes them through a Conditions() method.
package condition

import (
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/log"
)

// Condition is one active degraded state. Only active conditions are reported, so there is no
// status field: a condition that is listed is true.
type Condition struct {
	// Type is a stable snake_case identifier, e.g. "bucket_unavailable".
	Type string
	// Reason is a short snake_case cause within the type, e.g. "write_failed".
	Reason string
	// Message is a human-readable detail. It never contains secrets: URL query strings are
	// redacted.
	Message string
	// Since is when the condition became active. A change of Reason restarts it.
	Since time.Time
}

// Set is a component's active conditions. Transitions are logged once, at Warn when a
// condition is raised or its reason changes and at Info when it clears. It is safe for
// concurrent use.
type Set struct {
	component string
	now       func() time.Time

	mu     sync.Mutex
	active map[string]Condition
}

// NewSet returns an empty Set whose log lines name component.
func NewSet(component string) *Set {
	return &Set{component: component, now: time.Now, active: map[string]Condition{}}
}

// Raise makes typ active with the given reason and message. Raising an active condition again
// with the same reason only updates its message.
func (s *Set) Raise(typ, reason, message string) {
	message = Redact(message)

	s.mu.Lock()
	prev, ok := s.active[typ]
	c := Condition{Type: typ, Reason: reason, Message: message, Since: s.now()}
	if ok && prev.Reason == reason {
		c.Since = prev.Since
	}
	s.active[typ] = c
	s.mu.Unlock()

	if !ok || prev.Reason != reason {
		log.Warnf("[%s] condition %s raised (%s): %s", s.component, typ, reason, message)
	}
}

// Clear makes typ inactive.
func (s *Set) Clear(typ string) {
	s.mu.Lock()
	_, ok := s.active[typ]
	delete(s.active, typ)
	s.mu.Unlock()

	if ok {
		log.Infof("[%s] condition %s cleared", s.component, typ)
	}
}

// Active reports whether typ is active.
func (s *Set) Active(typ string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.active[typ]
	return ok
}

// List returns the active conditions sorted by type.
func (s *Set) List() []Condition {
	s.mu.Lock()
	out := make([]Condition, 0, len(s.active))
	for _, c := range s.active {
		out = append(out, c)
	}
	s.mu.Unlock()

	slices.SortFunc(out, func(a, b Condition) int { return strings.Compare(a.Type, b.Type) })
	return out
}

// Has reports whether conditions contains typ.
func Has(conditions []Condition, typ string) bool {
	return slices.ContainsFunc(conditions, func(c Condition) bool { return c.Type == typ })
}

var urlQuery = regexp.MustCompile(`(https?://[^\s?"']*)\?[^\s"']*`)

// Redact removes URL query strings, which can carry credentials such as SAS tokens and
// presigned signatures, from an error message.
func Redact(message string) string {
	return urlQuery.ReplaceAllString(message, "$1?REDACTED")
}
