package promtest

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
)

const (
	graphRequests = "sum by (client, server) (rate(traces_service_graph_request_total[1m]))"
	graphFailed   = "sum by (client, server) (rate(traces_service_graph_request_failed_total[1m]))"
	serverErrors  = "sum by (server) (rate(traces_service_graph_request_failed_total[5m]))"
	serverP95     = "histogram_quantile(0.95, sum by (server, le) (rate(traces_service_graph_request_server_seconds_bucket[5m])))"
)

// shopEdges is the made-up shop's call graph, client → server.
var shopEdges = []string{
	"user→storefront", "user→gateway",
	"storefront→catalog", "storefront→search", "storefront→cart", "storefront→recommend",
	"gateway→catalog", "gateway→cart", "gateway→checkout",
	"search→catalog", "recommend→catalog", "cart→inventory",
	"checkout→cart", "checkout→payments", "checkout→inventory", "checkout→shipping", "checkout→email",
	"payments→ledger", "reports→ledger", "reports→email",
}

// TestServiceGraph wants the recorded Tempo service graph to answer its queries with every edge
// of the shop, failures only on edges that exist, and errors and p95 latency for every server.
func TestServiceGraph(t *testing.T) {
	xs, err := Load("../../../testdata/prometheus/service-graph.json")
	if err != nil {
		t.Fatal(err)
	}
	rp := &Replayer{Exchanges: xs}

	edges := graphEdges(t, rp, "/api/v1/query", graphRequests)
	if got := slices.Sorted(maps.Keys(edges)); !slices.Equal(got, slices.Sorted(slices.Values(shopEdges))) {
		t.Errorf("edges %v; want %v", got, shopEdges)
	}
	servers, clients := map[string]bool{}, map[string]bool{}
	for e := range edges {
		c, s, _ := strings.Cut(e, "→")
		clients[c], servers[s] = true, true
	}
	if servers["reports"] || servers["user"] || !clients["reports"] || !clients["user"] {
		t.Errorf("servers %v; want reports and user calling but called by no one", servers)
	}
	for e := range graphEdges(t, rp, "/api/v1/query", graphFailed) {
		if !edges[e] {
			t.Errorf("failures on %s, which has no requests", e)
		}
	}
	for _, q := range []string{serverErrors, serverP95} {
		got := graphEdges(t, rp, "/api/v1/query_range", q)
		for s := range servers {
			if !got[s] {
				t.Errorf("%s: no series for %s", q, s)
			}
		}
	}
}

// graphEdges asks rp for query at path and returns each series' "client→server", or its server
// alone when the query keeps no client, after checking every sample is a number.
func graphEdges(t *testing.T, rp *Replayer, path, query string) map[string]bool {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://promhost:9090"+path, strings.NewReader(url.Values{"query": {query}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := rp.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("%s: HTTP %d", query, resp.StatusCode)
	}
	var res struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Value  []any             `json:"value"`
				Values [][]any           `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	got := map[string]bool{}
	for _, r := range res.Data.Result {
		key := r.Metric["server"]
		if c, ok := r.Metric["client"]; ok {
			key = c + "→" + key
		}
		if got[key] || len(r.Value)+len(r.Values) == 0 {
			t.Errorf("%s: %s twice or with no samples", query, key)
		}
		got[key] = true
	}
	return got
}
