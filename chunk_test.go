package prometheus

import (
	"cmp"
	"context"
	"io"
	"mindseye/pkg/sdk"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// inFlight passes requests to next, counting range queries, the targets each names, and the
// most in flight at once; each range query waits a moment, as a server evaluating it would.
type inFlight struct {
	next http.RoundTripper

	mu              sync.Mutex
	now, most, sent int
	widest          int
	topSent         int // the ranking queries, and the most targets one named
	topWidest       int
}

func (f *inFlight) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	m := instanceMatcher.FindStringSubmatch(r.Form.Get("query"))
	if strings.HasPrefix(r.Form.Get("query"), "topk(") && m != nil {
		f.mu.Lock()
		f.topSent++
		f.topWidest = max(f.topWidest, strings.Count(m[1], "|")+1)
		f.mu.Unlock()
	}
	if r.URL.Path != "/api/v1/query_range" {
		return f.next.RoundTrip(r)
	}
	f.mu.Lock()
	f.now++
	f.sent++
	f.most = max(f.most, f.now)
	if m != nil {
		f.widest = max(f.widest, strings.Count(m[1], "|")+1)
	}
	f.mu.Unlock()
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	f.now--
	f.mu.Unlock()
	return f.next.RoundTrip(r)
}

func TestALargeAskIsSplitAndSentAtOnce(t *testing.T) {
	f := &inFlight{next: lanSize()}
	m := configured(t, f, "url: "+bigLabURL)
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	end := time.Unix(1_790_000_000, 0)
	w := sdk.TimeWindow{From: end.Add(-time.Hour), To: end}
	got, err := m.QuerySeries(context.Background(), sdk.SeriesQuery{Metrics: []string{"cpu.utilisation"}, Window: w, Step: sdk.StepFor(w, 400)})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 20+549 {
		t.Errorf("%d series, want one for each of the 569 targets", len(got))
	}
	seen := map[sdk.EntityRef]bool{}
	for _, s := range got {
		if seen[s.Ref.Entity] || len(s.Points) == 0 {
			t.Errorf("%s: repeated, or no points", s.Ref.Entity)
		}
		seen[s.Ref.Entity] = true
	}
	if f.widest > rangeChunk || f.sent != 1+6 {
		t.Errorf("%d range queries, the widest naming %d targets; want 7, none over %d", f.sent, f.widest, rangeChunk)
	}
	if f.most < 2 || f.most > rangeParallel {
		t.Errorf("%d range queries in flight at most; want 2 to %d", f.most, rangeParallel)
	}
}

func TestATopIsRankedInChunksAndMerged(t *testing.T) {
	lab := lanSize()
	f := &inFlight{next: lab}
	m := configured(t, f, "url: "+bigLabURL)
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	end := time.Unix(1_790_000_000, 0)
	w := sdk.TimeWindow{From: end.Add(-time.Hour), To: end}
	got, err := m.QuerySeries(context.Background(), sdk.SeriesQuery{Metrics: []string{"cpu.utilisation"}, Window: w, Step: sdk.StepFor(w, 400), Top: 50})
	if err != nil {
		t.Fatal(err)
	}
	var services []labTarget
	for _, lt := range lab.targets() {
		if lt.node == "" {
			services = append(services, lt)
		}
	}
	slices.SortFunc(services, func(a, b labTarget) int { return cmp.Compare(b.now, a.now) })
	want := map[string]bool{}
	for _, lt := range services[:50] {
		want[lt.job+"/"+lt.instance] = true
	}
	n := 0
	for _, s := range got {
		if e := m.world.ents[s.Ref.Entity]; e.Kind == sdk.KindService {
			n++
			if !want[m.world.scraped[s.Ref.Entity].job+"/"+m.world.scraped[s.Ref.Entity].instance] {
				t.Errorf("%s is not among the 50 highest", s.Ref.Entity)
			}
		}
	}
	if n != 50 {
		t.Errorf("%d services, want the top 50", n)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.topWidest > rangeChunk || f.topSent < 2 {
		t.Errorf("%d ranking queries, the widest naming %d targets; want several, none over %d", f.topSent, f.topWidest, rangeChunk)
	}
}

func TestParallelAsksReuseTheirConnections(t *testing.T) {
	lab := lanSize()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp, err := lab.RoundTrip(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = io.Copy(w, resp.Body)
	}))
	var opened atomic.Int32
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.StartTLS()
	defer srv.Close()
	tr := New().transport.(*http.Transport).Clone()
	tr.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	m := configured(t, tr, "url: "+srv.URL)
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	end := time.Unix(1_790_000_000, 0)
	w := sdk.TimeWindow{From: end.Add(-time.Hour), To: end}
	q := sdk.SeriesQuery{Metrics: []string{"cpu.utilisation"}, Window: w, Step: sdk.StepFor(w, 400), Top: appTop}
	if _, err := m.QuerySeries(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	before := opened.Load()
	if _, err := m.QuerySeries(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if n := opened.Load() - before; n != 0 {
		t.Errorf("asking again opened %d connections; want the first ask's reused", n)
	}
}
