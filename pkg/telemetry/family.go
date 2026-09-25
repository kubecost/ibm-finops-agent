package telemetry

import (
	"slices"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// counterFamily is a counter with labels whose value per series is what was added to it plus
// what its sources report. Sources are counters kept elsewhere (the exporter's, the cluster
// cache's), read at scrape time, so they are never counted twice. Series given to preset are
// exported at zero before anything is counted, so increase() sees their first increment.
type counterFamily struct {
	desc *prometheus.Desc

	mu      sync.Mutex
	added   map[string]float64 // joined label values -> value
	order   []string           // keys of added, in first-seen order
	sources []func(add func(v uint64, labelValues ...string))
}

const labelSep = "\xff"

func newCounterFamily(name, help string, labels ...string) *counterFamily {
	return &counterFamily{
		desc:  prometheus.NewDesc(name, help, labels, nil),
		added: map[string]float64{},
	}
}

func (f *counterFamily) preset(labelValues ...string) {
	f.add(0, labelValues...)
}

func (f *counterFamily) add(v float64, labelValues ...string) {
	key := strings.Join(labelValues, labelSep)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.added[key]; !ok {
		f.order = append(f.order, key)
	}
	f.added[key] += v
}

// addSource adds a counter read at scrape time. It calls add once per series with that series'
// running total.
func (f *counterFamily) addSource(src func(add func(v uint64, labelValues ...string))) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sources = append(f.sources, src)
}

// totals returns every series' current value, keyed by joined label values, and their order.
func (f *counterFamily) totals() (map[string]float64, []string) {
	f.mu.Lock()
	values := make(map[string]float64, len(f.added))
	for k, v := range f.added {
		values[k] = v
	}
	order := slices.Clone(f.order)
	sources := slices.Clone(f.sources)
	f.mu.Unlock()

	for _, src := range sources {
		src(func(v uint64, labelValues ...string) {
			key := strings.Join(labelValues, labelSep)
			if _, ok := values[key]; !ok {
				order = append(order, key)
			}
			values[key] += float64(v)
		})
	}
	return values, order
}

func (f *counterFamily) Describe(ch chan<- *prometheus.Desc) { ch <- f.desc }

func (f *counterFamily) Collect(ch chan<- prometheus.Metric) {
	values, order := f.totals()
	for _, key := range order {
		ch <- prometheus.MustNewConstMetric(f.desc, prometheus.CounterValue, values[key], strings.Split(key, labelSep)...)
	}
}
