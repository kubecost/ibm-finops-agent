package cluster

import (
	"maps"
	"reflect"
	"strings"
	"sync"
)

// podsResource is the ConversionFailures key for pods, including deleted pods captured as
// short-lived pods.
const podsResource = "pods"

// conversionFailures counts objects skipped because they failed typed conversion, by resource
// (snapshot_object_conversion_failures_total{resource}). Keys are the resources in
// cacheResourceMap, so the set is bounded.
var conversionFailures = &failureCounter{counts: map[string]uint64{}}

type failureCounter struct {
	mu     sync.Mutex
	counts map[string]uint64
}

func (c *failureCounter) add(resource string, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[resource] += n
}

// ConversionFailures returns, per resource (e.g. "pods"), the number of objects skipped over the
// life of the process because they failed typed conversion.
func ConversionFailures() map[string]uint64 {
	conversionFailures.mu.Lock()
	defer conversionFailures.mu.Unlock()
	return maps.Clone(conversionFailures.counts)
}

// resourceName returns the resource name for t from cacheResourceMap, or its lower-cased type
// name for a type the cache doesn't hold.
func resourceName(t reflect.Type) string {
	if gvr, ok := cacheResourceMap[t]; ok {
		return gvr.Resource
	}
	return strings.ToLower(t.Name())
}
