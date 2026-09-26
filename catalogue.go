package prometheus

import (
	"maps"
	"mindseye/internal/model"
	"mindseye/internal/module"
	"slices"
	"strings"
)

// metaEntry is one family's metadata from /api/v1/metadata.
type metaEntry struct {
	Type string `json:"type"`
	Help string `json:"help"`
	Unit string `json:"unit"`
}

// metric is how one catalogue entry is queried: a family by name (a counter as a rate), or a
// canonical metric by an expression for each kind of target.
type metric struct {
	module.Metric
	counter bool
	exprs   map[model.Kind]string // PromQL with $sel and $range, grouped by job and instance
}

// canonical are the metric names shared with other modules: hosts from node_exporter's
// families, services from the process families most client libraries export.
var canonical = []metric{
	{
		Metric: module.Metric{Name: "cpu.utilisation", Unit: model.UnitPercent, Description: "share of CPU time spent busy; for a service, of one core"},
		exprs: map[model.Kind]string{
			model.KindHost:    `100 * (1 - avg by (job, instance) (rate(node_cpu_seconds_total{mode="idle",$sel}[$range])))`,
			model.KindService: `100 * sum by (job, instance) (rate(process_cpu_seconds_total{$sel}[$range]))`,
		},
	},
	{
		Metric: module.Metric{Name: "memory.utilisation", Unit: model.UnitPercent, Description: "share of memory not available to new work"},
		exprs: map[model.Kind]string{
			model.KindHost: `100 * (1 - sum by (job, instance) (node_memory_MemAvailable_bytes{$sel}) / sum by (job, instance) (node_memory_MemTotal_bytes{$sel}))`,
		},
	},
	{
		Metric: module.Metric{Name: "memory.rss", Unit: model.UnitBytes, Description: "resident memory"},
		exprs:  map[model.Kind]string{model.KindService: `sum by (job, instance) (process_resident_memory_bytes{$sel})`},
	},
	{
		Metric: module.Metric{Name: "disk.read", Unit: model.UnitBytesPS, Description: "bytes read from every disk"},
		exprs:  map[model.Kind]string{model.KindHost: `sum by (job, instance) (rate(node_disk_read_bytes_total{$sel}[$range]))`},
	},
	{
		Metric: module.Metric{Name: "disk.write", Unit: model.UnitBytesPS, Description: "bytes written to every disk"},
		exprs:  map[model.Kind]string{model.KindHost: `sum by (job, instance) (rate(node_disk_written_bytes_total{$sel}[$range]))`},
	},
	{
		Metric: module.Metric{Name: "net.receive", Unit: model.UnitBytesPS, Description: "bytes received on every interface but loopback"},
		exprs:  map[model.Kind]string{model.KindHost: `sum by (job, instance) (rate(node_network_receive_bytes_total{device!="lo",$sel}[$range]))`},
	},
	{
		Metric: module.Metric{Name: "net.transmit", Unit: model.UnitBytesPS, Description: "bytes sent on every interface but loopback"},
		exprs:  map[model.Kind]string{model.KindHost: `sum by (job, instance) (rate(node_network_transmit_bytes_total{device!="lo",$sel}[$range]))`},
	},
}

// synthetic are the series Prometheus adds for every target, which metadata does not describe.
var synthetic = map[string][]metaEntry{
	"up":                      {{Type: "gauge", Help: "1 when the target's last scrape succeeded, else 0"}},
	"scrape_duration_seconds": {{Type: "gauge", Help: "how long the target's last scrape took"}},
}

// catalogueOf lists the canonical metrics whose families the server has, then every gauge and
// counter family by its own name as an extra, with up and scrape_duration_seconds shown for
// every target. Histograms, summaries and info families are left out.
func catalogueOf(meta map[string][]metaEntry) []metric {
	var out []metric
	for _, c := range canonical {
		if c, ok := available(c, meta); ok {
			out = append(out, c)
		}
	}
	meta = maps.Clone(meta)
	for name, e := range synthetic {
		if len(meta[name]) == 0 {
			meta[name] = e
		}
	}
	for _, name := range slices.Sorted(maps.Keys(meta)) {
		if len(meta[name]) == 0 {
			continue
		}
		if m, ok := familyMetric(name, meta[name][0]); ok {
			out = append(out, m)
		}
	}
	return out
}

// available keeps c's expressions whose families the server has, with those kinds.
func available(c metric, meta map[string][]metaEntry) (metric, bool) {
	exprs := map[model.Kind]string{}
	var natives []string
	for _, k := range slices.Sorted(maps.Keys(c.exprs)) {
		if hasFamilies(meta, c.exprs[k]) {
			exprs[k] = c.exprs[k]
			c.Kinds = append(c.Kinds, k)
			natives = append(natives, string(k)+": "+c.exprs[k])
		}
	}
	c.exprs, c.Native = exprs, strings.Join(natives, "; ")
	return c, len(exprs) > 0
}

// hasFamilies reports whether every family expr reads, the names with underscores, is in meta.
func hasFamilies(meta map[string][]metaEntry, expr string) bool {
	for f := range strings.FieldsFuncSeq(expr, func(r rune) bool { return !isNameRune(r) }) {
		if strings.Contains(f, "_") && len(meta[f]) == 0 {
			return false
		}
	}
	return true
}

func isNameRune(r rune) bool {
	return r == '_' || r == ':' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func familyMetric(name string, e metaEntry) (metric, bool) {
	_, shown := synthetic[name]
	m := metric{Metric: module.Metric{Name: name, Description: e.Help, Native: name, Extra: !shown}}
	switch e.Type {
	case "counter":
		m.counter, m.Unit = true, rateUnit(name)
		m.Native = "rate(" + name + ")"
		m.Description = strings.TrimSpace(e.Help + " (per second)")
	case "gauge", "unknown", "":
		m.Unit = gaugeUnit(name, e.Unit)
	default:
		return metric{}, false
	}
	return m, true
}

// rateUnit is the unit of a counter's rate, from its name's suffix.
func rateUnit(name string) model.Unit {
	switch {
	case strings.HasSuffix(name, "_bytes_total"):
		return model.UnitBytesPS
	case strings.HasSuffix(name, "_bits_total"):
		return model.UnitBitsPS
	case strings.HasSuffix(name, "_seconds_total"):
		return model.UnitRatio // seconds per second
	}
	return model.UnitPerSec
}

// gaugeUnit is a gauge's unit, from its metadata or its name's suffix.
func gaugeUnit(name, unit string) model.Unit {
	suffix := unit
	if suffix == "" {
		suffix = name[strings.LastIndexByte(name, '_')+1:]
	}
	switch suffix {
	case "bytes":
		return model.UnitBytes
	case "seconds":
		return model.UnitSeconds
	case "ratio":
		return model.UnitRatio
	case "percent":
		return model.UnitPercent
	case "bits":
		return model.UnitBits
	}
	return model.UnitNone
}
