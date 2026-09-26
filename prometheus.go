package prometheus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"mindseye/pkg/sdk"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Kind is the module kind in config.
const Kind = "prometheus"

const version = "1"

// How often the metric catalogue is read again, and the longest wait before retrying a failure.
const (
	catalogueEvery = 10 * time.Minute
	retryMax       = 10 * time.Second
)

func init() { sdk.Register(Kind, func() sdk.Module { return New() }) }

// Module shows a Prometheus server's targets as entities and answers series queries from it.
type Module struct {
	transport http.RoundTripper
	health    atomic.Pointer[sdk.Health]

	mu        sync.Mutex // guards what follows, shared by Run and queries
	name      sdk.ModuleID
	opts      options
	client    *client
	world     world
	read      bool // world has been read
	tracker   sdk.Tracker
	metrics   []metric
	catalogAt time.Time
	catNote   string
	am        *client            // nil without an Alertmanager
	alerts    map[string]alertOn // the last alerts read, by fingerprint
	amNote    string
}

// New makes an unconfigured module that talks HTTP through the default transport.
func New() *Module { return NewWithTransport(http.DefaultTransport) }

// NewWithTransport makes an unconfigured module that sends its requests through rt.
func NewWithTransport(rt http.RoundTripper) *Module { return &Module{transport: rt} }

// Info describes the module.
func (m *Module) Info() sdk.Info {
	return sdk.Info{Kind: Kind, Version: version, Description: "A Prometheus server's scrape targets, their jobs and hosts, with their series"}
}

// Configure decodes options and reads the secret; no request is made until Run or Discover.
func (m *Module) Configure(_ context.Context, cfg sdk.Config) error {
	o := defaults()
	if err := cfg.Decode(&o); err != nil {
		return err
	}
	if err := o.validate(); err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	s, err := o.Read()
	if err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	am, err := alertmanagerClient(&o, m.transport)
	if err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name, m.opts, m.client, m.am = cfg.Name, o, newClient(&o.endpoint, o.Timeout, m.transport, s), am
	m.alerts, m.amNote = nil, ""
	m.world, m.read, m.metrics, m.catalogAt, m.catNote = world{}, false, nil, time.Time{}, ""
	m.tracker.Reset()
	m.health.Store(&sdk.Health{})
	return nil
}

// alertmanagerClient is a client for the configured Alertmanager, or nil for none.
func alertmanagerClient(o *options, rt http.RoundTripper) (*client, error) {
	if o.Alertmanager == nil {
		return nil, nil
	}
	s, err := o.Alertmanager.Read()
	if err != nil {
		return nil, fmt.Errorf("alertmanager: %w", err)
	}
	c := newClient(o.Alertmanager, o.Timeout, rt, s)
	c.bare = true
	return c, nil
}

// Run reads the targets every interval, sending a snapshot once they are first read and then
// what changed; a failed read shows in Health and is retried sooner.
func (m *Module) Run(ctx context.Context, sink sdk.Sink) error {
	m.mu.Lock()
	m.tracker.Reset()
	every := m.opts.Interval
	m.mu.Unlock()
	sent := false
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		cs, err := m.refresh(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			t.Reset(min(every, retryMax))
			continue
		}
		send := sink.Delta
		if !sent {
			send, sent = sink.Snapshot, true
		}
		if err := send(ctx, cs); err != nil {
			return err
		}
		t.Reset(every)
	}
}

// refresh reads the targets, the alerts, and the catalogue when due, returning what changed.
func (m *Module) refresh(ctx context.Context) (*sdk.ChangeSet, error) {
	now := time.Now()
	w, err := m.readWorld(ctx)
	var evs []sdk.Event
	if err == nil {
		m.readCatalogue(ctx)
		evs = m.readAlerts(ctx, &w, now)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health.Store(&sdk.Health{Err: err, Note: notes(m.catNote, m.amNote)})
	if err != nil {
		return nil, err
	}
	showAlerts(&w, m.alerts)
	m.world, m.read = w, true
	cs := m.tracker.Changes(w.ents, w.edges, now)
	cs.Events = evs
	return cs, nil
}

func notes(ns ...string) string {
	return strings.Join(slices.DeleteFunc(ns, func(n string) bool { return n == "" }), "; ")
}

func (m *Module) readWorld(ctx context.Context) (world, error) {
	m.mu.Lock()
	c, name, o := m.client, m.name, m.opts
	m.mu.Unlock()
	var ts targets
	if err := c.get(ctx, "/api/v1/targets", map[string][]string{"state": {"active"}}, &ts); err != nil {
		return world{}, err
	}
	return buildWorld(name, o.base.Host, o.kinds, nodeExporters(ctx, c), ts.Active), nil
}

// nodeQuery finds the node exporters, and the name each machine gives itself.
const nodeQuery = `group by (job, instance, nodename) (node_uname_info)`

// nodeExporters are the targets exporting node_uname_info, by nodename; none if it fails.
func nodeExporters(ctx context.Context, c *client) map[scrapeKey]string {
	var res struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
		} `json:"result"`
	}
	if c.get(ctx, "/api/v1/query", map[string][]string{"query": {nodeQuery}}, &res) != nil {
		return nil
	}
	out := map[scrapeKey]string{}
	for _, r := range res.Result {
		if name := r.Metric["nodename"]; name != "" {
			out[scrapeKey{r.Metric["job"], r.Metric["instance"]}] = name
		}
	}
	return out
}

// readCatalogue reads the metric metadata when it is due; failing keeps the last catalogue.
func (m *Module) readCatalogue(ctx context.Context) {
	m.mu.Lock()
	c, due := m.client, time.Since(m.catalogAt) >= catalogueEvery || m.metrics == nil
	m.mu.Unlock()
	if !due {
		return
	}
	var meta map[string][]metaEntry
	err := c.get(ctx, "/api/v1/metadata", nil, &meta)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.catNote = "metric catalogue: " + err.Error()
		return
	}
	m.metrics, m.catalogAt, m.catNote = catalogueOf(meta), time.Now(), ""
}

// Health reports whether the server answered the last read.
func (m *Module) Health() sdk.Health {
	if h := m.health.Load(); h != nil {
		return *h
	}
	return sdk.Health{}
}

// Discover returns the targets as last read, reading them first if Run has not.
func (m *Module) Discover(ctx context.Context) (*sdk.ChangeSet, error) {
	m.mu.Lock()
	w, read := m.world, m.read
	m.mu.Unlock()
	if !read {
		var err error
		if w, err = m.readWorld(ctx); err != nil {
			return nil, err
		}
	}
	var probe sdk.Tracker
	return probe.Changes(w.ents, w.edges, time.Now()), nil
}

// Metrics lists the canonical metrics the server can answer, then its own gauges and counters.
func (m *Module) Metrics() []sdk.Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]sdk.Metric, len(m.metrics))
	for i, mt := range m.metrics {
		out[i] = mt.Metric
	}
	return out
}

// QuerySeries asks query_range for each metric over every target asked for, in one request per
// metric; entities that are not targets have no series.
func (m *Module) QuerySeries(ctx context.Context, q sdk.SeriesQuery) ([]sdk.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	c, w := m.client, m.world
	byName := map[string]metric{}
	for _, mt := range m.metrics {
		byName[mt.Name] = mt
	}
	m.mu.Unlock()
	wanted := wantedTargets(&w, q)
	step := stepFor(q)
	var out []sdk.Series
	for _, name := range q.Metrics {
		mt, ok := byName[name]
		if !ok {
			continue
		}
		gs := groups(&w, wanted, mt)
		for _, kind := range slices.Sorted(maps.Keys(gs)) {
			expr := expression(mt, kind, q.Agg, selector(gs[kind]), rangeFor(step, w.interval))
			got, err := rangeSeries(ctx, c, expr, q.Window, step, func(res *matrix) ([]sdk.Series, error) {
				return seriesOf(res, wanted, mt, q.Window)
			})
			if err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
			out = append(out, got...)
		}
	}
	return out, nil
}

// wantedTargets are the targets q names, or that its filter selects, by their scrape labels.
func wantedTargets(w *world, q sdk.SeriesQuery) map[scrapeKey]sdk.EntityRef {
	out := map[scrapeKey]sdk.EntityRef{}
	for ref, key := range w.scraped {
		e := w.ents[ref]
		if len(q.Entities) > 0 && slices.Contains(q.Entities, ref) || len(q.Entities) == 0 && q.Filter.Match(&e) {
			out[key] = ref
		}
	}
	return out
}

// groups are the wanted targets that can have mt, by the kind whose expression queries them;
// a family is queried the same way for every kind.
func groups(w *world, wanted map[scrapeKey]sdk.EntityRef, mt metric) map[sdk.Kind][]scrapeKey {
	out := map[sdk.Kind][]scrapeKey{}
	for key, ref := range wanted {
		kind := w.ents[ref].Kind
		switch {
		case mt.exprs == nil:
			out[""] = append(out[""], key)
		case mt.exprs[kind] != "":
			out[kind] = append(out[kind], key)
		}
	}
	return out
}

// rangeSeries asks query_range for expr over win at step, reading the answer with read.
func rangeSeries(ctx context.Context, c *client, expr string, win sdk.TimeWindow, step time.Duration, read func(*matrix) ([]sdk.Series, error)) ([]sdk.Series, error) {
	var res matrix
	if err := c.post(ctx, "/api/v1/query_range", rangeForm(expr, win, step), &res); err != nil {
		return nil, err
	}
	return read(&res)
}

// seriesOf maps a range query's results back to the targets asked for.
func seriesOf(res *matrix, wanted map[scrapeKey]sdk.EntityRef, mt metric, win sdk.TimeWindow) ([]sdk.Series, error) {
	if res.ResultType != "matrix" {
		return nil, errors.New("answer is a " + res.ResultType + ", not a matrix")
	}
	var out []sdk.Series
	for _, r := range res.Result {
		ref, ok := wanted[scrapeKey{r.Metric["job"], r.Metric["instance"]}]
		if !ok {
			continue
		}
		ps, err := points(r.Values, win)
		if err != nil {
			return nil, err
		}
		out = append(out, sdk.Series{Ref: sdk.SeriesRef{Entity: ref, Metric: mt.Name}, Unit: mt.Unit, Points: ps})
	}
	return out, nil
}
