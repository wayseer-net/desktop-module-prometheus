package prometheus

import (
	"context"
	"mindseye/pkg/sdk"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"
)

// BenchmarkSeriesRoundTrip asks for every host's CPU at 400 points: over the recorded window
// from the recorded lab, or over the last hour from the server at MINDSEYE_PROM_URL when set.
// It reports the p95 of the round trips.
func BenchmarkSeriesRoundTrip(b *testing.B) {
	rp := lab(b, true)
	var rt http.RoundTripper = rp
	url, w := labURL, recordedWindow(b, rp)
	if u := os.Getenv("MINDSEYE_PROM_URL"); u != "" {
		now := time.Now()
		rt, url, w = http.DefaultTransport, u, sdk.TimeWindow{From: now.Add(-time.Hour), To: now}
	}
	m := configured(b, rt, "url: "+url)
	if _, err := m.refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	q := sdk.SeriesQuery{Metrics: []string{"cpu.utilisation"}, Window: w, Step: sdk.StepFor(w, 400)}
	took := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	for b.Loop() {
		began := time.Now()
		got, err := m.QuerySeries(context.Background(), q)
		took = append(took, time.Since(began))
		if err != nil || len(got) == 0 {
			b.Fatalf("%d series, %v", len(got), err)
		}
	}
	slices.Sort(took)
	b.ReportMetric(float64(took[len(took)*95/100])/float64(time.Millisecond), "p95-ms")
}
