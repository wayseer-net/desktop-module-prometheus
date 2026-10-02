package prometheus

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"wayseer/modules/prometheus/promtest"
	"wayseer/pkg/sdk"
)

const (
	istioQuery = `sum by (source_workload, destination_workload, response_code) (rate(istio_requests_total{reporter="destination"}[1m]))`
	plainQuery = `sum by (src, dst) (rate(net_sent_bytes_total[1m]))`
)

// flowOptions reads the recorded Istio-style requests and a plain byte count between hosts.
const flowOptions = `
url: ` + labURL + `
flows:
  - query: '` + istioQuery + `'
    from: {label: source_workload, kind: k8s/deployment}
    to: {label: destination_workload, kind: k8s/deployment}
    unit: requests
  - query: '` + plainQuery + `'
    from: {label: src, kind: host}
    to: {label: dst, kind: host}
    unit: bytes
`

// flowLab replays the lab with the recorded flow answers.
func flowLab(t *testing.T) *promtest.Replayer {
	t.Helper()
	rp := lab(t, false)
	xs, err := promtest.Load("../../testdata/prometheus/flows.json")
	if err != nil {
		t.Fatal(err)
	}
	rp.Exchanges = append(rp.Exchanges, xs...)
	return rp
}

// fakeWorld is what other modules found: names to refs, by kind, and names several entities share.
type fakeWorld struct {
	refs      map[sdk.Kind]map[string]sdk.EntityRef
	ambiguous []string
}

func (w fakeWorld) Match(kind sdk.Kind, value string) (sdk.EntityRef, error) {
	return w.MatchExcept(kind, value, func(sdk.EntityRef) bool { return false })
}

func (w fakeWorld) MatchExcept(kind sdk.Kind, value string, skip func(sdk.EntityRef) bool) (sdk.EntityRef, error) {
	if slices.Contains(w.ambiguous, value) {
		return "", sdk.ErrAmbiguous
	}
	if r, ok := w.refs[kind][value]; ok && !skip(r) {
		return r, nil
	}
	return "", sdk.ErrNoMatch
}

func k8sRef(t *testing.T, kind sdk.Kind, native string) sdk.EntityRef {
	t.Helper()
	r, err := sdk.NewEntityRef("k8s", kind, native)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// shopWorld has the shop's deployments and two hosts, with inventory in two namespaces.
func shopWorld(t *testing.T) fakeWorld {
	d := func(name string) sdk.EntityRef { return k8sRef(t, "k8s/deployment", "shop/"+name) }
	return fakeWorld{
		refs: map[sdk.Kind]map[string]sdk.EntityRef{
			"k8s/deployment": {"frontend": d("frontend"), "checkout": d("checkout"), "payments": d("payments"), "cart": d("cart")},
			sdk.KindHost:     {"web-01": k8sRef(t, sdk.KindHost, "web-01"), "db-07:5432": k8sRef(t, sdk.KindHost, "db-07")},
		},
		ambiguous: []string{"inventory"},
	}
}

func flowing(t *testing.T, w sdk.Resolver) (*Module, *sdk.ChangeSet) {
	t.Helper()
	m := configured(t, flowLab(t), flowOptions)
	if w != nil {
		m.UseWorld(func() sdk.Resolver { return w })
	}
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return m, cs
}

// traffic is each talks_to edge's ends and rate.
func traffic(cs *sdk.ChangeSet) map[[2]sdk.EntityRef]sdk.Traffic {
	out := map[[2]sdk.EntityRef]sdk.Traffic{}
	for _, e := range cs.Edges {
		if e.Rel == sdk.RelTalksTo {
			out[[2]sdk.EntityRef{e.From, e.To}] = e.Traffic
		}
	}
	return out
}

func TestFlowQueriesBecomeTrafficBetweenEntitiesInTheWorld(t *testing.T) {
	w := shopWorld(t)
	_, cs := flowing(t, w)
	d := w.refs["k8s/deployment"]
	h := w.refs[sdk.KindHost]
	want := map[[2]sdk.EntityRef]sdk.Traffic{
		{d["frontend"], d["checkout"]}: {Rate: 120.5, Unit: sdk.TrafficRequests},
		{d["checkout"], d["payments"]}: {Rate: 42, Unit: sdk.TrafficRequests},
		{d["checkout"], d["cart"]}:     {Rate: 30, Unit: sdk.TrafficRequests},
		{h["web-01"], h["db-07:5432"]}: {Rate: 1536, Unit: sdk.TrafficBytes},
	}
	got := traffic(cs)
	if len(got) != len(want) {
		t.Errorf("%d flows, want %d: %v", len(got), len(want), got)
	}
	for k, tr := range want {
		if got[k] != tr {
			t.Errorf("%s → %s carries %+v, want %+v", k[0], k[1], got[k], tr)
		}
	}
	for _, e := range cs.Edges {
		if e.Rel == sdk.RelTalksTo && e.Source != "lab" {
			t.Errorf("flow %v is sent as %s's", e.Key(), e.Source)
		}
	}
}

func TestFlowEndsNotInTheWorldAreCountedNotInvented(t *testing.T) {
	m, cs := flowing(t, shopWorld(t))
	for _, e := range cs.Upserts {
		if strings.Contains(e.Name, "loadgen") || e.Name == "unknown" || e.Name == "inventory" {
			t.Errorf("an end not in the world became %s", e.Ref)
		}
	}
	note := m.Health().Note
	for _, want := range []string{"3 with an end not in the world", "1 with an end naming several entities"} {
		if !strings.Contains(note, want) {
			t.Errorf("health note %q lacks %q", note, want)
		}
	}
}

func TestFlowsWithoutTheWorldAddNothing(t *testing.T) {
	m, cs := flowing(t, nil)
	if n := len(traffic(cs)); n != 0 {
		t.Errorf("%d flows with no world to match against", n)
	}
	if note := m.Health().Note; !strings.Contains(note, "flows: no world to match against") {
		t.Errorf("health note %q", note)
	}
}

func TestAFlowQueryTheServerRefusesKeepsItsTextOutOfTheNote(t *testing.T) {
	opts := flowOptions + `
  - query: 'sum by (a, b) (rate(nothing_recorded_total[1m]))'
    from: {label: a}
    to: {label: b}
    unit: messages
`
	m := configured(t, flowLab(t), opts)
	m.UseWorld(func() sdk.Resolver { return shopWorld(t) })
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n := len(traffic(cs)); n != 4 {
		t.Errorf("%d flows, want the other queries' 4", n)
	}
	note := m.Health().Note
	if !strings.Contains(note, "flow 3: the server refused the query (not_found)") || strings.Contains(note, "nothing_recorded") {
		t.Errorf("health note %q", note)
	}
}

func TestFlowOptionsAreChecked(t *testing.T) {
	for _, tc := range []struct{ flow, want string }{
		{`{from: {label: a}, to: {label: b}, unit: requests}`, "query"},
		{`{query: up, to: {label: b}, unit: requests}`, "from.label"},
		{`{query: up, from: {label: a}, to: {label: "b-c"}, unit: requests}`, "to.label"},
		{`{query: up, from: {label: a, kind: "Not A Kind"}, to: {label: b}, unit: requests}`, "from.kind"},
		{`{query: up, from: {label: a}, to: {label: b}}`, "unit"},
	} {
		m := NewWithTransport(flowLab(t))
		err := m.Configure(context.Background(), cfg(t, "flows: ["+tc.flow+"]"))
		if err == nil || !strings.Contains(err.Error(), "flows[0]") || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want flows[0] and %q", tc.flow, err, tc.want)
		}
	}
}

// failing refuses every request while down is set, and passes the rest to next.
type failing struct {
	down *bool
	next http.RoundTripper
}

func (f failing) RoundTrip(r *http.Request) (*http.Response, error) {
	if *f.down && r.Method == http.MethodPost {
		return nil, errors.New("connection reset")
	}
	return f.next.RoundTrip(r)
}

func TestAFailedFlowQueryKeepsItsLastTraffic(t *testing.T) {
	down := false
	m := configured(t, failing{&down, flowLab(t)}, flowOptions)
	m.UseWorld(func() sdk.Resolver { return shopWorld(t) })
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	down = true
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	cs, err := m.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n := len(traffic(cs)); n != 4 {
		t.Errorf("%d flows after the queries fail, want the last 4", n)
	}
	if note := m.Health().Note; !strings.Contains(note, "flow 1: /api/v1/query: connection reset") {
		t.Errorf("health note %q", note)
	}
}
