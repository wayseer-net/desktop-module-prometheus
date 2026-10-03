package prometheus

import (
	"context"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"wayseer.dev/sdk"
)

// What the app asks of a module: at most seriesBatch entities a query (the app's series batch),
// or, of one that ranks, Grid's top (grid.MaxTiles).
const appBatch, appTop = 100, 500

// lanSize is the generated server: the owner's LAN server has 549 targets exporting their CPU.
func lanSize() *bigLab { return &bigLab{hosts: 20, services: 549} }

// BenchmarkSeriesRoundTrip asks for CPU at 400 points over an hour, as the app asks: a batch of
// entities, and a ranked top; "every" asks for every target at once, a worst case with no
// budget. It uses the generated server, or the server at WAYSEER_PROM_URL over the last hour.
func BenchmarkSeriesRoundTrip(b *testing.B) {
	var rt http.RoundTripper = lanSize()
	url, end := bigLabURL, time.Unix(1_790_000_000, 0)
	if u := os.Getenv("WAYSEER_PROM_URL"); u != "" {
		rt, url, end = New().transport, u, time.Now()
	}
	m := configured(b, rt, "url: "+url)
	if _, err := m.refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	w := sdk.TimeWindow{From: end.Add(-time.Hour), To: end}
	q := sdk.SeriesQuery{Metrics: []string{"cpu.utilisation"}, Window: w, Step: sdk.StepFor(w, 400)}
	batch, top := q, q
	batch.Entities = targetRefs(m)[:min(appBatch, len(m.world.scraped))]
	top.Top = appTop
	b.Run("batch", func(b *testing.B) { roundTrips(b, m, batch) })
	b.Run("top", func(b *testing.B) { roundTrips(b, m, top) })
	b.Run("every", func(b *testing.B) { roundTrips(b, m, q) })
}

// roundTrips asks q until b is done, reporting the p95 and the samples each answer had.
func roundTrips(b *testing.B, m *Module, q sdk.SeriesQuery) {
	took := make([]time.Duration, 0, b.N)
	n := 0
	b.ReportAllocs()
	for b.Loop() {
		began := time.Now()
		got, err := m.QuerySeries(context.Background(), q)
		took = append(took, time.Since(began))
		if err != nil || len(got) == 0 {
			b.Fatalf("%d series, %v", len(got), err)
		}
		n = pointsIn(got)
	}
	slices.Sort(took)
	b.ReportMetric(float64(took[len(took)*95/100])/float64(time.Millisecond), "p95-ms")
	b.ReportMetric(float64(n), "samples")
}

func targetRefs(m *Module) []sdk.EntityRef {
	refs := make([]sdk.EntityRef, 0, len(m.world.scraped))
	for ref := range m.world.scraped {
		refs = append(refs, ref)
	}
	slices.Sort(refs)
	return refs
}

func pointsIn(ss []sdk.Series) int {
	n := 0
	for _, s := range ss {
		n += len(s.Points)
	}
	return n
}

func TestALANSizedAnswerTakesUnderHalfAnAllocationASample(t *testing.T) {
	m := configured(t, lanSize(), "url: "+bigLabURL)
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	end := time.Unix(1_790_000_000, 0)
	w := sdk.TimeWindow{From: end.Add(-time.Hour), To: end}
	q := sdk.SeriesQuery{Metrics: []string{"cpu.utilisation"}, Window: w, Step: sdk.StepFor(w, 400)}
	var n int
	allocs := testing.AllocsPerRun(3, func() {
		got, err := m.QuerySeries(context.Background(), q)
		if err != nil {
			t.Fatal(err)
		}
		n = pointsIn(got)
	})
	if n < 500*400 {
		t.Fatalf("the answer had %d samples", n)
	}
	if per := allocs / float64(n); per >= 0.5 {
		t.Errorf("%.0f allocations for %d samples: %.2f a sample", allocs, n, per)
	}
}
