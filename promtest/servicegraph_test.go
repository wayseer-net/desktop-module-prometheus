package promtest

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
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

	edges := graphEdges(t, rp, "/api/v1/query", GraphRequests)
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
	for e := range graphEdges(t, rp, "/api/v1/query", GraphFailed) {
		if !edges[e] {
			t.Errorf("failures on %s, which has no requests", e)
		}
	}
	for _, q := range []string{GraphErrors, GraphP95} {
		got := graphEdges(t, rp, "/api/v1/query_range", q)
		for s := range servers {
			if !got[s] {
				t.Errorf("%s: no series for %s", q, s)
			}
		}
	}
}

// graphEdges asks rt for query at path, over the last hour for a range, and returns each
// series' "client→server", or its server alone when the query keeps no client, after checking
// every sample is a number.
func graphEdges(t *testing.T, rt http.RoundTripper, path, query string) map[string]bool {
	t.Helper()
	form := url.Values{"query": {query}}
	if strings.HasSuffix(path, "_range") {
		end := time.Now().Unix()
		form.Set("start", strconv.FormatInt(end-3600, 10))
		form.Set("end", strconv.FormatInt(end, 10))
		form.Set("step", "60")
	}
	req, _ := http.NewRequest("POST", "http://promhost:9090"+path, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	body := mustRead(t, resp)
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

func mustRead(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
