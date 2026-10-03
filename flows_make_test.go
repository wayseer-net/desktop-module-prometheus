package prometheus

import (
	"context"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"wayseer/modules/prometheus/promtest"
	"wayseer/pkg/sdk"
)

const graphQuery = "sum by (client, server) (rate(traces_service_graph_request_total[1m]))"

// shopServices are the services of the recorded service graph, sorted.
var shopServices = []string{
	"cart", "catalog", "checkout", "email", "gateway", "inventory", "ledger",
	"payments", "recommend", "reports", "search", "shipping", "storefront", "user",
}

// graphOptions reads the service graph, making ends no module found when make is set.
func graphOptions(making bool, extra string) string {
	m := ""
	if making {
		m = ", make: true"
	}
	return `
url: ` + labURL + extra + `
flows:
  - query: '` + graphQuery + `'
    from: {label: client, kind: service` + m + `}
    to: {label: server, kind: service` + m + `}
    unit: requests
`
}

// graphLab replays the lab with the recorded service graph.
func graphLab(t *testing.T) *promtest.Replayer {
	t.Helper()
	rp := lab(t, false)
	xs, err := promtest.Load("testdata/service-graph.json")
	if err != nil {
		t.Fatal(err)
	}
	rp.Exchanges = append(rp.Exchanges, xs...)
	return rp
}

// graphing refreshes a module reading the service graph against w.
func graphing(t testing.TB, rt http.RoundTripper, opts string, w sdk.Resolver) (*Module, *sdk.ChangeSet) {
	t.Helper()
	m := configured(t, rt, opts)
	m.UseWorld(func() sdk.Resolver { return w })
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return m, cs
}

// madeNames are the names of the services cs makes from flow ends.
func madeNames(cs *sdk.ChangeSet) []string {
	var out []string
	for _, e := range cs.Upserts {
		if _, made := e.Attrs["flow_label"]; made && e.Kind == sdk.KindService {
			out = append(out, e.Name)
		}
	}
	slices.Sort(out)
	return out
}

// foundElsewhere is a world where another module found catalog and cart, and nothing else.
func foundElsewhere(t *testing.T) fakeWorld {
	d := func(name string) sdk.EntityRef { return k8sRef(t, sdk.KindService, "shop/"+name) }
	return fakeWorld{refs: map[sdk.Kind]map[string]sdk.EntityRef{sdk.KindService: {"catalog": d("catalog"), "cart": d("cart")}}}
}

func TestFlowsMake(t *testing.T) {
	for _, tc := range []struct {
		name string
		test func(*testing.T)
	}{
		{"every service", makesEveryService},
		{"without make only what was found", makesNothingWithoutMake},
		{"a found service is used, not doubled", usesAFoundService},
		{"the module's own host is used, not doubled", usesItsOwnHost},
		{"a service once made then found elsewhere is the found one", givesWayToAFoundService},
		{"past the bound a note", notesPastTheBound},
		{"gone when no series names them", dropsWhatNoSeriesNames},
	} {
		t.Run(tc.name, tc.test)
	}
}

func makesEveryService(t *testing.T) {
	_, cs := graphing(t, graphLab(t), graphOptions(true, ""), fakeWorld{})
	if got := madeNames(cs); !slices.Equal(got, shopServices) {
		t.Errorf("made %v; want %v", got, shopServices)
	}
	if n := len(traffic(cs)); n != 20 {
		t.Errorf("%d flows, want the graph's 20", n)
	}
}

func makesNothingWithoutMake(t *testing.T) {
	_, cs := graphing(t, graphLab(t), graphOptions(false, ""), foundElsewhere(t))
	if got := madeNames(cs); len(got) != 0 {
		t.Errorf("made %v without make", got)
	}
	if n := len(traffic(cs)); n != 0 {
		t.Errorf("%d flows; catalog and cart never call each other", n)
	}
}

func usesAFoundService(t *testing.T) {
	w := foundElsewhere(t)
	_, cs := graphing(t, graphLab(t), graphOptions(true, ""), w)
	if got := madeNames(cs); slices.Contains(got, "catalog") || slices.Contains(got, "cart") || len(got) != len(shopServices)-2 {
		t.Errorf("made %v; want all but catalog and cart", got)
	}
	tr := traffic(cs)
	made := func(n string) sdk.EntityRef { r, _ := sdk.NewEntityRef("lab", sdk.KindService, n); return r }
	for _, k := range [][2]sdk.EntityRef{{made("storefront"), w.refs[sdk.KindService]["catalog"]}, {w.refs[sdk.KindService]["cart"], made("inventory")}} {
		if _, ok := tr[k]; !ok {
			t.Errorf("no flow %s → %s in %v", k[0], k[1], slices.Collect(maps.Keys(tr)))
		}
	}
	if n := len(tr); n != 20 {
		t.Errorf("%d flows, want the graph's 20", n)
	}
}

func usesItsOwnHost(t *testing.T) {
	const q = "sum by (src, dst) (rate(net_sent_bytes_total[1m]))"
	rp := lab(t, false)
	rp.Exchanges = append(rp.Exchanges, promtest.Exchange{
		Method: "POST", Path: "/api/v1/query", Form: url.Values{"query": {q}}, Status: 200,
		Body: []byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"src":"promhost","dst":"db-07"},"value":[1800000000,"1536"]}]}}`),
	})
	_, cs := graphing(t, rp, "url: "+labURL+"\nflows: [{query: '"+q+"', from: {label: src, kind: host, make: true}, to: {label: dst, kind: host, make: true}, unit: bytes}]", fakeWorld{})
	own, _ := sdk.NewEntityRef("lab", sdk.KindHost, "promhost")
	made, _ := sdk.NewEntityRef("lab", sdk.KindHost, "db-07")
	if _, ok := traffic(cs)[[2]sdk.EntityRef{own, made}]; !ok {
		t.Errorf("no flow from the scraped promhost to a made db-07: %v", traffic(cs))
	}
	for _, e := range cs.Upserts {
		if _, mk := e.Attrs["flow_label"]; e.Ref == own && (mk || e.Status.Level != sdk.StatusOK) {
			t.Errorf("promhost was made again: %+v", e)
		}
	}
}

func givesWayToAFoundService(t *testing.T) {
	m, _ := graphing(t, graphLab(t), graphOptions(true, ""), fakeWorld{})
	mine, _ := sdk.NewEntityRef("lab", sdk.KindService, "catalog")
	theirs := k8sRef(t, sdk.KindService, "shop/catalog")
	m.UseWorld(func() sdk.Resolver { return bothCatalogs{mine, theirs} })
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	storefront, _ := sdk.NewEntityRef("lab", sdk.KindService, "storefront")
	if _, ok := traffic(cs)[[2]sdk.EntityRef{storefront, theirs}]; !ok || !slices.Contains(cs.Removes, mine) {
		t.Errorf("the made catalog stays beside the found one: removes %v", cs.Removes)
	}
}

func notesPastTheBound(t *testing.T) {
	m, cs := graphing(t, graphLab(t), graphOptions(true, "\nmax_made: 5"), fakeWorld{})
	if got := madeNames(cs); len(got) != 5 {
		t.Errorf("made %d services, want the bound's 5", len(got))
	}
	if note := m.Health().Note; !strings.Contains(note, "flows: 9 ends left out past max_made 5") {
		t.Errorf("health note %q", note)
	}
}

func dropsWhatNoSeriesNames(t *testing.T) {
	rp := graphLab(t)
	m, _ := graphing(t, rp, graphOptions(true, ""), fakeWorld{})
	for i, x := range rp.Exchanges {
		if x.Form.Get("query") == graphQuery {
			rp.Exchanges[i].Body = []byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`)
		}
	}
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n := len(cs.Removes); n != len(shopServices) {
		t.Errorf("%d removed once no series names them, want %d", n, len(shopServices))
	}
}

func TestFlowsMakeOptionsAreChecked(t *testing.T) {
	for _, tc := range []struct{ opts, want string }{
		{"flows: [{query: up, from: {label: a, make: true}, to: {label: b, kind: service}, unit: requests}]", "from.make needs a kind"},
		{"max_made: 0", "max_made 0 must be between 1 and 10000"},
		{"max_made: 10001", "max_made 10001 must be between 1 and 10000"},
	} {
		err := NewWithTransport(graphLab(t)).Configure(context.Background(), cfg(t, tc.opts))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %q", tc.opts, err, tc.want)
		}
	}
}

// bothCatalogs is a world holding the catalog a module made and the one another found.
type bothCatalogs [2]sdk.EntityRef

func (w bothCatalogs) Match(kind sdk.Kind, value string) (sdk.EntityRef, error) {
	return w.MatchExcept(kind, value, func(sdk.EntityRef) bool { return false })
}

func (w bothCatalogs) MatchExcept(kind sdk.Kind, value string, skip func(sdk.EntityRef) bool) (sdk.EntityRef, error) {
	found := slices.DeleteFunc(slices.Clone(w[:]), skip)
	switch {
	case kind != sdk.KindService || value != "catalog" || len(found) == 0:
		return "", sdk.ErrNoMatch
	case len(found) > 1:
		return "", sdk.ErrAmbiguous
	}
	return found[0], nil
}
