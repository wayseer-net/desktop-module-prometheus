package promtest

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestGraphAnswersTheRecipe wants a generated graph to answer the service graph recipe's queries
// with exactly its calls, every service in one of them, and no service calling itself.
func TestGraphAnswersTheRecipe(t *testing.T) {
	g := &Graph{Services: 500, Edges: 2000}
	edges := graphEdges(t, g, "/api/v1/query", GraphRequests)
	if len(edges) != 2000 {
		t.Fatalf("%d calls; want 2000", len(edges))
	}
	named, servers := map[string]bool{}, map[string]bool{}
	for e := range edges {
		c, s, _ := strings.Cut(e, "→")
		if c == s {
			t.Errorf("%s calls itself", c)
		}
		named[c], named[s], servers[s] = true, true, true
	}
	if len(named) != 500 {
		t.Errorf("%d services named; want 500", len(named))
	}
	for e := range graphEdges(t, g, "/api/v1/query", GraphFailed) {
		if !edges[e] {
			t.Errorf("failures on %s, which has no requests", e)
		}
	}
	for _, q := range []string{GraphErrors, GraphP95} {
		if got := graphEdges(t, g, "/api/v1/query_range", q); len(got) != len(servers) {
			t.Errorf("%s: %d series; want one for each of %d servers", q, len(got), len(servers))
		}
	}
}

// TestGraphIsTheSameEachTime wants two graphs of one size to be the same graph, with the same
// rates at the same time and other rates later.
func TestGraphIsTheSameEachTime(t *testing.T) {
	at := time.Unix(1_790_000_000, 0)
	a := &Graph{Services: 50, Edges: 120, Now: func() time.Time { return at }}
	b := &Graph{Services: 50, Edges: 120, Now: func() time.Time { return at }}
	if x, y := answer(t, a, GraphRequests), answer(t, b, GraphRequests); x != y {
		t.Error("two graphs of one size answered differently")
	}
	before := answer(t, a, GraphRequests)
	at = at.Add(time.Minute)
	if answer(t, a, GraphRequests) == before {
		t.Error("the rates did not change in a minute")
	}
}

func TestGraphRefusesWhatItCannotMake(t *testing.T) {
	for _, g := range []*Graph{{Services: 1, Edges: 0}, {Services: 10, Edges: 8}, {Services: 10, Edges: 46}} {
		req, _ := http.NewRequest("GET", "http://graph.test/api/v1/query?"+url.Values{"query": {GraphRequests}}.Encode(), nil)
		if _, err := g.RoundTrip(req); err == nil {
			t.Errorf("%d services and %d calls: no error", g.Services, g.Edges)
		}
	}
}

func TestGraphRefusesOtherQueries(t *testing.T) {
	g := &Graph{Services: 10, Edges: 20}
	req, _ := http.NewRequest("GET", "http://graph.test/api/v1/query?"+url.Values{"query": {"up"}}.Encode(), nil)
	resp, err := g.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusNotFound {
		t.Errorf("another query: %v, %v; want 404", resp, err)
	}
}

func answer(t *testing.T, g *Graph, query string) string {
	t.Helper()
	req, _ := http.NewRequest("GET", "http://graph.test/api/v1/query?"+url.Values{"query": {query}}.Encode(), nil)
	resp, err := g.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	return string(mustRead(t, resp))
}
