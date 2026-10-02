package prometheus

import (
	"context"
	"testing"
	"time"
	"wayseer/modules/prometheus/promtest"
	"wayseer/pkg/sdk"
)

// trackerAllocs is TrackerUnchanged10k's allocation ceiling in scripts/budgets.txt.
const trackerAllocs = 10

// bigGraph refreshes a module reading a 500-service graph of 2000 calls, held at one time, then
// gives it a world holding what it made, as the app's world does after the first refresh.
func bigGraph(t testing.TB) *Module {
	t.Helper()
	at := time.Unix(1_790_000_000, 0)
	g := &promtest.Graph{Services: 500, Edges: 2000, Now: func() time.Time { return at }}
	m, cs := graphing(t, g, graphOptions(true, ""), fakeWorld{})
	w := fakeWorld{refs: map[sdk.Kind]map[string]sdk.EntityRef{sdk.KindService: {}}}
	for _, e := range cs.Upserts {
		if e.Kind == sdk.KindService {
			w.refs[e.Kind][e.Name] = e.Ref
		}
	}
	if n := len(w.refs[sdk.KindService]); n != 501 || len(cs.Edges) != 2000 {
		t.Fatalf("%d services and %d edges; want 501 and 2000", n, len(cs.Edges))
	}
	m.UseWorld(func() sdk.Resolver { return w })
	return m
}

// TestAnUnchangedServiceGraphSendsNothing wants a refresh of a 500-service graph whose rates have
// not moved to send nothing, and finding that out to allocate within the tracker's budget.
func TestAnUnchangedServiceGraphSendsNothing(t *testing.T) {
	m := bigGraph(t)
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !cs.Empty() {
		t.Fatalf("an unchanged graph sent %d upserts, %d removes and %d edges", len(cs.Upserts), len(cs.Removes), len(cs.Edges))
	}
	if n := len(m.world.ents); n != 501 {
		t.Fatalf("the second refresh holds %d entities; want 501", n)
	}
	now := time.Now()
	allocs := testing.AllocsPerRun(10, func() {
		if !m.tracker.Changes(m.world.ents, m.world.edges, now).Empty() {
			t.Fatal("the tracker found a change")
		}
	})
	if allocs > trackerAllocs {
		t.Errorf("finding nothing changed took %.0f allocations; the budget is %d", allocs, trackerAllocs)
	}
}

// BenchmarkGraphRefresh500 times a refresh of a 500-service graph of 2000 calls that finds
// nothing changed, from asking the generated server to the change set.
func BenchmarkGraphRefresh500(b *testing.B) {
	m := bigGraph(b)
	b.ReportAllocs()
	for b.Loop() {
		cs, err := m.refresh(context.Background())
		if err != nil || !cs.Empty() {
			b.Fatalf("%v, or a change", err)
		}
	}
}

// countingWorld counts the matches asked of it.
type countingWorld struct {
	fakeWorld
	asked *int
}

func (w countingWorld) MatchExcept(kind sdk.Kind, value string, skip func(sdk.EntityRef) bool) (sdk.EntityRef, error) {
	*w.asked++
	return w.fakeWorld.MatchExcept(kind, value, skip)
}

func TestEachEndIsMatchedOnceARefresh(t *testing.T) {
	var asked int
	g := &promtest.Graph{Services: 500, Edges: 2000}
	graphing(t, g, graphOptions(true, ""), countingWorld{asked: &asked})
	if asked > 2*500 {
		t.Errorf("%d matches for 2000 calls between 500 services; want at most one a service at each end", asked)
	}
}
