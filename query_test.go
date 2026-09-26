package prometheus

import (
	"encoding/json"
	"mindseye/internal/data"
	"mindseye/internal/model"
	"mindseye/internal/module"
	"strings"
	"testing"
	"time"
)

func TestQueriesTranslateToPromQL(t *testing.T) {
	gauge := metric{Metric: module.Metric{Name: "go_goroutines"}}
	counter := metric{Metric: module.Metric{Name: "http_requests_total"}, counter: true}
	one := selector([]scrapeKey{{"node", "web-1:9100"}})
	for _, tc := range []struct {
		name string
		m    metric
		kind model.Kind
		agg  data.Aggregation
		sel  string
		want string
	}{
		{"a gauge averages", gauge, "", data.AggNone, one, `avg by (job, instance) (go_goroutines{job="node",instance="web-1:9100"})`},
		{"a counter sums its rate", counter, "", data.AggNone, one, `sum by (job, instance) (rate(http_requests_total{job="node",instance="web-1:9100"}[60s]))`},
		{"max", gauge, "", data.AggMax, one, `max by (job, instance) (go_goroutines{job="node",instance="web-1:9100"})`},
		{"p95", counter, "", data.AggP95, one, `quantile by (job, instance) (0.95, rate(http_requests_total{job="node",instance="web-1:9100"}[60s]))`},
		{"canonical for a host", canonical[0], model.KindHost, data.AggMax, one, `100 * (1 - avg by (job, instance) (rate(node_cpu_seconds_total{mode="idle",job="node",instance="web-1:9100"}[60s])))`},
		{"canonical for a service", canonical[0], model.KindService, data.AggNone, one, `100 * sum by (job, instance) (rate(process_cpu_seconds_total{job="node",instance="web-1:9100"}[60s]))`},
		{
			"several targets match by escaped regex", gauge, "", data.AggNone,
			selector([]scrapeKey{{"api", "10.0.0.1:80"}, {"api", `we"b:80`}}),
			`avg by (job, instance) (go_goroutines{job="api",instance=~"10\\.0\\.0\\.1:80|we\"b:80"})`,
		},
	} {
		if got := expression(tc.m, tc.kind, tc.agg, tc.sel, time.Minute); got != tc.want {
			t.Errorf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
	}
}

func TestStepsAndRangesSuitPrometheus(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	day := data.TimeWindow{From: now.Add(-24 * time.Hour), To: now}
	if got := stepFor(data.SeriesQuery{Window: day, Step: time.Second}); day.Span()/got > maxPoints {
		t.Errorf("a one-second step over a day gives %d points", day.Span()/got)
	}
	if got := stepFor(data.SeriesQuery{Window: day}); got != data.StepFor(day, 1000) {
		t.Errorf("no step asks for %v", got)
	}
	for _, tc := range []struct{ step, scrape, want time.Duration }{
		{15 * time.Second, 15 * time.Second, time.Minute},
		{5 * time.Minute, 15 * time.Second, 5 * time.Minute},
		{time.Second, 0, time.Minute},
	} {
		if got := rangeFor(tc.step, tc.scrape); got != tc.want {
			t.Errorf("rangeFor(%v, %v) = %v, want %v", tc.step, tc.scrape, got, tc.want)
		}
	}
	f := rangeForm("up", data.TimeWindow{From: now, To: now.Add(time.Hour)}, 1500*time.Millisecond)
	if f.Get("start") != "1800000000.000" || f.Get("end") != "1800003600.000" || f.Get("step") != "1.5" {
		t.Errorf("range form %v", f)
	}
}

func TestPointsKeepNumbersInsideTheWindow(t *testing.T) {
	var values [][2]json.RawMessage
	if err := json.Unmarshal([]byte(`[[99.5,"1"],[100,"2"],[100.25,"NaN"],[101,"+Inf"],[102,"3"],[110,"4"]]`), &values); err != nil {
		t.Fatal(err)
	}
	w := data.TimeWindow{From: time.Unix(100, 0), To: time.Unix(110, 0)}
	got, err := points(values, w)
	if err != nil {
		t.Fatal(err)
	}
	want := []data.Point{{T: 100e9, V: 2}, {T: 102e9, V: 3}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("points %v, want %v", got, want)
	}
}

func TestCatalogueComesFromMetadata(t *testing.T) {
	meta := map[string][]metaEntry{
		"node_cpu_seconds_total":           {{Type: "counter"}},
		"node_network_receive_bytes_total": {{Type: "counter"}},
		"process_resident_memory_bytes":    {{Type: "gauge", Help: "Resident memory size in bytes."}},
		"http_request_duration_seconds":    {{Type: "histogram"}},
		"temperature":                      {{Type: "gauge", Unit: "celsius"}},
	}
	got := map[string]model.Unit{}
	for _, m := range catalogueOf(meta) {
		got[m.Name] = m.Unit
	}
	want := map[string]model.Unit{
		"cpu.utilisation": model.UnitPercent, "node_cpu_seconds_total": model.UnitRatio,
		"node_network_receive_bytes_total": model.UnitBytesPS, "process_resident_memory_bytes": model.UnitBytes,
		"temperature": model.UnitNone, "net.receive": model.UnitBytesPS, "up": model.UnitNone, "scrape_duration_seconds": model.UnitSeconds,
	}
	for name, unit := range want {
		if u, ok := got[name]; !ok || u != unit {
			t.Errorf("%s: in catalogue %v, unit %q; want %q", name, ok, u, unit)
		}
	}
	for _, m := range catalogueOf(meta) {
		if shown := m.Name == "up" || m.Name == "scrape_duration_seconds" || strings.Contains(m.Name, "."); m.Extra == shown {
			t.Errorf("%s: extra %v", m.Name, m.Extra)
		}
	}
	for _, absent := range []string{"http_request_duration_seconds", "memory.utilisation"} {
		if _, ok := got[absent]; ok {
			t.Errorf("%s is in the catalogue", absent)
		}
	}
}
