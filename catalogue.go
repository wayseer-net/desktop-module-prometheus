package prometheus

import (
	"maps"
	"slices"
	"strings"

	"wayseer.dev/sdk"
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
	sdk.Metric
	counter bool
	exprs   map[sdk.Kind]string // PromQL with $sel and $range, grouped by job and instance
}

// canonical are the metric names shared with other modules: hosts from node_exporter's
// families, services from the process families most client libraries export.
var canonical = []metric{
	{
		Metric: sdk.Metric{Name: "cpu.utilisation", Unit: sdk.UnitPercent, Description: "share of CPU time spent busy; for a service, of one core"},
		exprs: map[sdk.Kind]string{
			sdk.KindHost:    `100 * (1 - avg by (job, instance) (rate(node_cpu_seconds_total{mode="idle",$sel}[$range])))`,
			sdk.KindService: `100 * sum by (job, instance) (rate(process_cpu_seconds_total{$sel}[$range]))`,
		},
	},
	{
		Metric: sdk.Metric{Name: "memory.utilisation", Unit: sdk.UnitPercent, Description: "share of memory not available to new work"},
		exprs: map[sdk.Kind]string{
			sdk.KindHost: `100 * (1 - sum by (job, instance) (node_memory_MemAvailable_bytes{$sel}) / sum by (job, instance) (node_memory_MemTotal_bytes{$sel}))`,
		},
	},
	{
		Metric: sdk.Metric{Name: "memory.rss", Unit: sdk.UnitBytes, Description: "resident memory"},
		exprs:  map[sdk.Kind]string{sdk.KindService: `sum by (job, instance) (process_resident_memory_bytes{$sel})`},
	},
	{
		Metric: sdk.Metric{Name: "disk.read", Unit: sdk.UnitBytesPS, Description: "bytes read from every disk"},
		exprs:  map[sdk.Kind]string{sdk.KindHost: `sum by (job, instance) (rate(node_disk_read_bytes_total{$sel}[$range]))`},
	},
	{
		Metric: sdk.Metric{Name: "disk.write", Unit: sdk.UnitBytesPS, Description: "bytes written to every disk"},
		exprs:  map[sdk.Kind]string{sdk.KindHost: `sum by (job, instance) (rate(node_disk_written_bytes_total{$sel}[$range]))`},
	},
	{
		Metric: sdk.Metric{Name: "net.receive", Unit: sdk.UnitBytesPS, Description: "bytes received on every interface but loopback"},
		exprs:  map[sdk.Kind]string{sdk.KindHost: `sum by (job, instance) (rate(node_network_receive_bytes_total{device!="lo",$sel}[$range]))`},
	},
	{
		Metric: sdk.Metric{Name: "net.transmit", Unit: sdk.UnitBytesPS, Description: "bytes sent on every interface but loopback"},
		exprs:  map[sdk.Kind]string{sdk.KindHost: `sum by (job, instance) (rate(node_network_transmit_bytes_total{device!="lo",$sel}[$range]))`},
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
	exprs := map[sdk.Kind]string{}
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
	m := metric{Metric: sdk.Metric{Name: name, Description: e.Help, Native: name, Extra: !shown}}
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
func rateUnit(name string) sdk.Unit {
	switch {
	case strings.HasSuffix(name, "_bytes_total"):
		return sdk.UnitBytesPS
	case strings.HasSuffix(name, "_bits_total"):
		return sdk.UnitBitsPS
	case strings.HasSuffix(name, "_seconds_total"):
		return sdk.UnitRatio // seconds per second
	}
	return sdk.UnitPerSec
}

// gaugeUnit is a gauge's unit, from its metadata or its name's suffix.
func gaugeUnit(name, unit string) sdk.Unit {
	suffix := unit
	if suffix == "" {
		suffix = name[strings.LastIndexByte(name, '_')+1:]
	}
	switch suffix {
	case "bytes":
		return sdk.UnitBytes
	case "seconds":
		return sdk.UnitSeconds
	case "ratio":
		return sdk.UnitRatio
	case "percent":
		return sdk.UnitPercent
	case "bits":
		return sdk.UnitBits
	}
	return sdk.UnitNone
}
