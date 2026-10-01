package prometheus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"wayseer/modules/prometheus/promtest"
	"wayseer/pkg/sdk"
	"wayseer/pkg/sdk/sdktest"

	"go.yaml.in/yaml/v3"
)

const labURL = "http://promhost:9090"

// lab replays the recorded server; loose answers queries it never recorded with a recorded one.
func lab(t testing.TB, loose bool) *promtest.Replayer {
	t.Helper()
	xs, err := promtest.Load("../../testdata/prometheus/lab.json")
	if err != nil {
		t.Fatal(err)
	}
	return &promtest.Replayer{Exchanges: xs, Loose: loose}
}

// onlyHost sends requests for host to next, and refuses the rest.
type onlyHost struct {
	host string
	next http.RoundTripper
}

func (o onlyHost) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host != o.host {
		return nil, errors.New("connection refused")
	}
	return o.next.RoundTrip(r)
}

func configured(t testing.TB, rt http.RoundTripper, opts string) *Module {
	t.Helper()
	m := NewWithTransport(rt)
	if err := m.Configure(context.Background(), cfg(t, opts)); err != nil {
		t.Fatal(err)
	}
	return m
}

func cfg(t testing.TB, opts string) sdk.Config {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(opts), &doc); err != nil {
		t.Fatal(err)
	}
	c := sdk.Config{Name: "lab", Line: 1}
	if len(doc.Content) > 0 {
		c.Options = *doc.Content[0]
	}
	return c
}

// running is m after its first snapshot.
func running(t *testing.T, m *Module) *sdktest.Sink {
	t.Helper()
	sink := sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	sink.WaitFor(t, 1)
	return sink
}

func TestConformance(t *testing.T) {
	rt := onlyHost{"promhost:9090", lab(t, true)}
	sdktest.Conform(t, sdktest.Case{
		New:     func() sdk.Module { return NewWithTransport(rt) },
		Options: "url: " + labURL,
		Failing: "url: http://nowhere:9090\ntimeout: 200ms",
	})
}

func TestDiscoveryListsTargetsJobsAndHosts(t *testing.T) {
	m := configured(t, lab(t, false), "url: "+labURL)
	cs, err := m.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sdktest.Golden(t, "testdata/prometheus-lab.txt", describe(cs))
}

// describe lists entities with their status and the edges, one per line.
func describe(cs *sdk.ChangeSet) string {
	var b strings.Builder
	for _, e := range cs.Upserts {
		fmt.Fprintf(&b, "%s %q %s", e.Ref, e.Name, e.Status.Level)
		if e.Status.Reason != "" {
			fmt.Fprintf(&b, " (%s)", e.Status.Reason)
		}
		b.WriteByte('\n')
	}
	for _, e := range cs.Edges {
		fmt.Fprintf(&b, "%s -%s-> %s\n", e.From, e.Rel, e.To)
	}
	return b.String()
}

func TestADownTargetCarriesItsScrapeError(t *testing.T) {
	m := configured(t, lab(t, false), "url: "+labURL)
	cs, err := m.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	api := entity(t, cs, labRef(sdk.KindService, "api/127.0.0.1:19999"))
	if api.Status.Level != sdk.StatusDown || !strings.Contains(api.Status.Reason, "connection refused") {
		t.Errorf("the refused target reads %v", api.Status)
	}
	if job := entity(t, cs, labRef(KindJob, "api")); job.Status.Level != sdk.StatusCrit {
		t.Errorf("a job with every target down reads %v", job.Status)
	}
	if host := entity(t, cs, labRef(sdk.KindHost, "promhost")); host.Status.Level != sdk.StatusOK || host.Attrs["job"].Str() != "hosts" {
		t.Errorf("node's target is not the host itself: %v %v", host.Status, host.Attrs)
	}
}

func labRef(kind sdk.Kind, native string) sdk.EntityRef {
	r, _ := sdk.NewEntityRef("lab", kind, native)
	return r
}

func entity(t *testing.T, cs *sdk.ChangeSet, ref sdk.EntityRef) sdk.Entity {
	t.Helper()
	i := slices.IndexFunc(cs.Upserts, func(e sdk.Entity) bool { return e.Ref == ref })
	if i < 0 {
		t.Fatalf("no %s", ref)
	}
	return cs.Upserts[i]
}

// recordedWindow is the window the fixture's range queries asked about.
func recordedWindow(t testing.TB, rp *promtest.Replayer) sdk.TimeWindow {
	t.Helper()
	for _, x := range rp.Exchanges {
		if x.Path == "/api/v1/query_range" {
			return sdk.TimeWindow{From: unix(t, x.Form.Get("start")), To: unix(t, x.Form.Get("end"))}
		}
	}
	t.Fatal("no range query recorded")
	return sdk.TimeWindow{}
}

func unix(t testing.TB, s string) time.Time {
	t.Helper()
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatal(err)
	}
	return time.UnixMilli(int64(f * 1e3))
}

func TestSeriesComeFromTheRecordedRangeQueries(t *testing.T) {
	rp := lab(t, false)
	m := configured(t, rp, "url: "+labURL)
	running(t, m)
	w := recordedWindow(t, rp)
	host := labRef(sdk.KindHost, "promhost")
	for _, tc := range []struct {
		metric   string
		agg      sdk.Aggregation
		entities []sdk.EntityRef
		want     int
		unit     sdk.Unit
	}{
		{"cpu.utilisation", sdk.AggNone, []sdk.EntityRef{host}, 1, sdk.UnitPercent},
		{"net.receive", sdk.AggNone, nil, 1, sdk.UnitBytesPS},
		{"process_resident_memory_bytes", sdk.AggNone, nil, 2, sdk.UnitBytes},
		{"prometheus_http_requests_total", sdk.AggNone, nil, 1, sdk.UnitPerSec},
		{"go_goroutines", sdk.AggMax, nil, 2, sdk.UnitNone},
		{"cpu.utilisation", sdk.AggNone, nil, 2, sdk.UnitPercent},
		{"memory.rss", sdk.AggNone, []sdk.EntityRef{host}, 0, ""},
		{"no_such_metric", sdk.AggNone, nil, 0, ""},
	} {
		q := sdk.SeriesQuery{Entities: tc.entities, Metrics: []string{tc.metric}, Window: w, Step: sdk.StepFor(w, 100), Agg: tc.agg}
		got, err := m.QuerySeries(context.Background(), q)
		if err != nil {
			t.Fatalf("%s: %v", tc.metric, err)
		}
		if len(got) != tc.want {
			t.Errorf("%s over %v: %d series, want %d", tc.metric, tc.entities, len(got), tc.want)
			continue
		}
		for _, s := range got {
			if s.Unit != tc.unit || len(s.Points) == 0 || s.Ref.Metric != tc.metric {
				t.Errorf("%s: series %v in %q with %d points", tc.metric, s.Ref, s.Unit, len(s.Points))
			}
		}
	}
}

func TestSlowServerShowsAsLoadingThenAnError(t *testing.T) {
	m := configured(t, stall{}, "url: "+labURL+"\ntimeout: 300ms")
	sink := sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	time.Sleep(100 * time.Millisecond)
	if h := m.Health(); h.Err != nil || len(sink.Sets()) != 0 {
		t.Fatalf("while waiting: %v, %d change sets", h.Err, len(sink.Sets()))
	}
	sdktest.Eventually(t, func() bool { return m.Health().Err != nil })
	if err := m.Health().Err; !strings.Contains(err.Error(), "no answer within 300ms") {
		t.Errorf("a slow server reads %v", err)
	}
	if len(sink.Sets()) != 0 {
		t.Error("sent a snapshot without reading the targets")
	}
}

// stall never answers, until the request is cancelled.
type stall struct{}

func (stall) RoundTrip(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}

func TestMetadataAsksForOneEntryPerMetric(t *testing.T) {
	rp := lab(t, false)
	m := configured(t, rp, "url: "+labURL)
	running(t, m)
	for _, r := range rp.Requests() {
		if r.URL.Path == "/api/v1/metadata" {
			if got := r.URL.Query().Get("limit_per_metric"); got != "1" {
				t.Errorf("metadata asked with limit_per_metric=%q, want 1", got)
			}
			return
		}
	}
	t.Error("the metric catalogue was never read")
}
