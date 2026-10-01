package prometheus

import (
	"context"
	"mindseye/modules/prometheus/promtest"
	"mindseye/pkg/sdk"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	queueQuery  = `max by (namespace, pod, queue) (shop_queue_depth)`
	hpaQuery    = `max by (namespace, horizontalpodautoscaler) (kube_horizontalpodautoscaler_status_target_metric{metric_name="queue_depth",metric_target_type="average"})`
	brokenQuery = `sum by (namespace, pod) (rate(shop_queue_depth[5m])`
)

// seriesOptions reads the recorded queue depths of pods, an HPA's target, and a query the
// server refuses.
const seriesOptions = `
url: ` + labURL + `
series:
  - query: '` + queueQuery + `'
    metric: queue.depth
    unit: count
    description: messages waiting in the pod's queues
    entity: {labels: [namespace, pod], kind: pod}
  - query: '` + hpaQuery + `'
    metric: hpa.queue_depth
    description: the queue depth the deployment's autoscaler sees
    entity: {labels: [namespace, horizontalpodautoscaler], kind: k8s/deployment}
  - query: '` + brokenQuery + `'
    metric: queue.rate
    entity: {labels: [namespace, pod], kind: pod}
`

// seriesWindow is the hour the recording covers.
var seriesWindow = sdk.TimeWindow{From: time.Unix(1_799_996_400, 0), To: time.Unix(1_800_000_000, 0)}

// podWorld has the shop's pods and deployments; the batch namespace's pod is not in it.
func podWorld(t *testing.T) fakeWorld {
	pod := func(name string) sdk.EntityRef { return k8sRef(t, sdk.KindPod, "shop/"+name) }
	d := func(name string) sdk.EntityRef { return k8sRef(t, "k8s/deployment", "shop/"+name) }
	return fakeWorld{refs: map[sdk.Kind]map[string]sdk.EntityRef{
		sdk.KindPod: {
			"shop/checkout-6d8f9c7b5-x2k4p": pod("checkout-6d8f9c7b5-x2k4p"),
			"shop/checkout-6d8f9c7b5-q7m2n": pod("checkout-6d8f9c7b5-q7m2n"),
			"shop/payments-7c5d8b9f4-h8t6r": pod("payments-7c5d8b9f4-h8t6r"),
		},
		"k8s/deployment": {"checkout": d("checkout"), "payments": d("payments")},
	}}
}

// seriesLab is the module replaying the lab and the recorded series, matching w.
func seriesLab(t *testing.T, w fakeWorld) (*Module, *promtest.Replayer) {
	t.Helper()
	rp := lab(t, false)
	xs, err := promtest.Load("../../testdata/prometheus/series.json")
	if err != nil {
		t.Fatal(err)
	}
	rp.Exchanges = append(rp.Exchanges, xs...)
	m := configured(t, rp, seriesOptions)
	m.UseWorld(func() sdk.Resolver { return w })
	return m, rp
}

func ask(t *testing.T, m *Module, metric string, ents ...sdk.EntityRef) ([]sdk.Series, error) {
	t.Helper()
	return m.QuerySeries(context.Background(), sdk.SeriesQuery{Entities: ents, Metrics: []string{metric}, Window: seriesWindow, Step: time.Minute})
}

func TestSeriesJoin(t *testing.T) {
	w := podWorld(t)
	m, _ := seriesLab(t, w)
	pods := w.refs[sdk.KindPod]
	x2k4p, q7m2n := pods["shop/checkout-6d8f9c7b5-x2k4p"], pods["shop/checkout-6d8f9c7b5-q7m2n"]
	got, err := ask(t, m, "queue.depth", x2k4p, q7m2n)
	if err != nil {
		t.Fatal(err)
	}
	if refs := seriesRefs(got); !slices.Equal(refs, []sdk.EntityRef{q7m2n, x2k4p}) {
		t.Fatalf("series of %v; want only the two pods asked for", refs)
	}
	sum := got[1]
	if sum.Unit != sdk.UnitCount || sum.Ref.Metric != "queue.depth" || len(sum.Points) != 60 {
		t.Errorf("x2k4p's series: unit %q, metric %q, %d points; want count, queue.depth, 60", sum.Unit, sum.Ref.Metric, len(sum.Points))
	}
	if sum.Points[0].V != 24+3 {
		t.Errorf("x2k4p starts at %v; want its orders and retries queues added, 27", sum.Points[0].V)
	}

	d := w.refs["k8s/deployment"]
	got, err = ask(t, m, "hpa.queue_depth", d["checkout"], d["payments"])
	if err != nil {
		t.Fatal(err)
	}
	if refs := seriesRefs(got); len(refs) != 2 || got[0].Unit != sdk.UnitNone {
		t.Errorf("the HPA's metric is on %v in %q; want both deployments, a plain number", refs, got[0].Unit)
	}
}

func seriesRefs(ss []sdk.Series) []sdk.EntityRef {
	out := make([]sdk.EntityRef, len(ss))
	for i, s := range ss {
		out[i] = s.Ref.Entity
	}
	return out
}

func TestSeriesLabelsNamingNoEntityAreCounted(t *testing.T) {
	m, _ := seriesLab(t, podWorld(t))
	got, err := ask(t, m, "queue.depth")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range got {
		if strings.Contains(string(s.Ref.Entity), "reindex") {
			t.Errorf("a pod not in the world got a series: %s", s.Ref.Entity)
		}
	}
	if len(got) != 3 {
		t.Errorf("%d series; want the shop's 3 pods", len(got))
	}
	if _, err := m.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if note := m.Health().Note; !strings.Contains(note, "series queue.depth: 1 naming no entity in the world") {
		t.Errorf("health note %q; want the batch pod counted", note)
	}
}

func TestASeriesQueryThatFailsHidesItsText(t *testing.T) {
	w := podWorld(t)
	m, _ := seriesLab(t, w)
	_, err := ask(t, m, "queue.rate", w.refs[sdk.KindPod]["shop/checkout-6d8f9c7b5-x2k4p"])
	if err == nil {
		t.Fatal("the refused query answered")
	}
	if msg := err.Error(); strings.Contains(msg, "shop_queue_depth") || !strings.Contains(msg, "queue.rate") || !strings.Contains(msg, "bad_data") {
		t.Errorf("error %q; want the metric and why, without the query", msg)
	}
}

func TestSeriesAreInTheCatalogueWithoutTheirQueries(t *testing.T) {
	m, _ := seriesLab(t, podWorld(t))
	ms := m.Metrics()
	i := slices.IndexFunc(ms, func(x sdk.Metric) bool { return x.Name == "queue.depth" })
	if i < 0 {
		t.Fatalf("queue.depth is not in the catalogue %v", ms)
	}
	q := ms[i]
	if !q.Joined || !slices.Equal(q.Kinds, []sdk.Kind{sdk.KindPod}) || q.Unit != sdk.UnitCount || q.Description != "messages waiting in the pod's queues" {
		t.Errorf("queue.depth is %+v; want a joined count of pods, described", q)
	}
	for _, x := range ms {
		if strings.Contains(x.Native+x.Description, "shop_queue_depth") || strings.Contains(x.Native, "kube_horizontalpodautoscaler") {
			t.Errorf("%s shows its query: %+v", x.Name, x)
		}
	}
}

func TestSeriesWithoutTheWorldAnswerNothing(t *testing.T) {
	m, _ := seriesLab(t, podWorld(t))
	m.UseWorld(nil)
	if _, err := ask(t, m, "queue.depth"); err == nil || !strings.Contains(err.Error(), "no world") {
		t.Errorf("err %v; want no world to match against", err)
	}
}

func TestSeriesOptionsAreChecked(t *testing.T) {
	for _, tc := range []struct{ opts, want string }{
		{"series:\n  - {metric: q, entity: {labels: [pod]}}", "needs a query"},
		{"series:\n  - {query: up, metric: 'a b', entity: {labels: [pod]}}", "not a metric name"},
		{"series:\n  - {query: up, metric: q, unit: furlongs, entity: {labels: [pod]}}", "unknown unit"},
		{"series:\n  - {query: up, metric: q}", "needs a label"},
		{"series:\n  - {query: up, metric: q, entity: {labels: [a-b]}}", "not a label name"},
		{"series:\n  - {query: up, metric: q, entity: {labels: [pod]}}\n  - {query: up, metric: q, entity: {labels: [pod]}}", "named twice"},
	} {
		err := NewWithTransport(nil).Configure(context.Background(), cfg(t, "url: "+labURL+"\n"+tc.opts))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err %v; want %q", tc.opts, err, tc.want)
		}
	}
}
