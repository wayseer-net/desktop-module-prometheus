package prometheus

import (
	"context"
	"errors"
	"fmt"
	"mindseye/internal/data"
	"mindseye/internal/model"
	"mindseye/internal/module"
	"net/http"
	"slices"
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

func init() { module.Register(Kind, func() module.Module { return New() }) }

// Module shows a Prometheus server's targets as entities and answers series queries from it.
type Module struct {
	transport http.RoundTripper
	health    atomic.Pointer[data.Health]

	mu        sync.Mutex // guards what follows, shared by Run and queries
	name      model.ModuleID
	opts      options
	client    *client
	world     world
	read      bool // world has been read
	tracker   module.Tracker
	metrics   []metric
	catalogAt time.Time
	catNote   string
}

// New makes an unconfigured module that talks HTTP through the default transport.
func New() *Module { return NewWithTransport(http.DefaultTransport) }

// NewWithTransport makes an unconfigured module that sends its requests through rt.
func NewWithTransport(rt http.RoundTripper) *Module { return &Module{transport: rt} }

// Info describes the module.
func (m *Module) Info() module.Info {
	return module.Info{Kind: Kind, Version: version, Description: "A Prometheus server's scrape targets, their jobs and hosts, with their series"}
}

// Configure decodes options and reads the secret; no request is made until Run or Discover.
func (m *Module) Configure(_ context.Context, cfg module.Config) error {
	o := defaults()
	if err := cfg.Decode(&o); err != nil {
		return err
	}
	if err := o.validate(); err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	s, err := o.readSecret()
	if err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name, m.opts, m.client = cfg.Name, o, newClient(&o, m.transport, s)
	m.world, m.read, m.metrics, m.catalogAt, m.catNote = world{}, false, nil, time.Time{}, ""
	m.tracker.Reset()
	m.health.Store(&data.Health{})
	return nil
}

// Run reads the targets every interval, sending a snapshot once they are first read and then
// what changed; a failed read shows in Health and is retried sooner.
func (m *Module) Run(ctx context.Context, sink module.Sink) error {
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

// refresh reads the targets, and the catalogue when due, returning what changed.
func (m *Module) refresh(ctx context.Context) (*model.ChangeSet, error) {
	w, err := m.readWorld(ctx)
	if err == nil {
		m.readCatalogue(ctx)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.health.Store(&data.Health{Err: err, Note: m.catNote})
	if err != nil {
		return nil, err
	}
	m.world, m.read = w, true
	return m.tracker.Changes(w.ents, w.edges, time.Now()), nil
}

func (m *Module) readWorld(ctx context.Context) (world, error) {
	m.mu.Lock()
	c, name, o := m.client, m.name, m.opts
	m.mu.Unlock()
	var ts targets
	if err := c.get(ctx, "/api/v1/targets", map[string][]string{"state": {"active"}}, &ts); err != nil {
		return world{}, err
	}
	return buildWorld(name, o.base.Host, o.kinds, ts.Active), nil
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
func (m *Module) Health() data.Health {
	if h := m.health.Load(); h != nil {
		return *h
	}
	return data.Health{}
}

// Discover returns the targets as last read, reading them first if Run has not.
func (m *Module) Discover(ctx context.Context) (*model.ChangeSet, error) {
	m.mu.Lock()
	w, read := m.world, m.read
	m.mu.Unlock()
	if !read {
		var err error
		if w, err = m.readWorld(ctx); err != nil {
			return nil, err
		}
	}
	var probe module.Tracker
	return probe.Changes(w.ents, w.edges, time.Now()), nil
}

// Metrics lists the canonical metrics the server can answer, then its own gauges and counters.
func (m *Module) Metrics() []module.Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]module.Metric, len(m.metrics))
	for i, mt := range m.metrics {
		out[i] = mt.Metric
	}
	return out
}

// QuerySeries asks query_range for each metric over every target asked for, in one request per
// metric; entities that are not targets have no series.
func (m *Module) QuerySeries(ctx context.Context, q data.SeriesQuery) ([]data.Series, error) {
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
	var out []data.Series
	for _, name := range q.Metrics {
		mt, ok := byName[name]
		keys := keysFor(&w, wanted, mt)
		if !ok || len(keys) == 0 {
			continue
		}
		expr := expression(mt, q.Agg, selector(keys), rangeFor(step, w.interval))
		var res matrix
		if err := c.post(ctx, "/api/v1/query_range", rangeForm(expr, q.Window, step), &res); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		got, err := seriesOf(&res, wanted, mt, q.Window)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out = append(out, got...)
	}
	return out, nil
}

// wantedTargets are the targets q names, or that its filter selects, by their scrape labels.
func wantedTargets(w *world, q data.SeriesQuery) map[scrapeKey]model.EntityRef {
	out := map[scrapeKey]model.EntityRef{}
	for ref, key := range w.scraped {
		e := w.ents[ref]
		if len(q.Entities) > 0 && slices.Contains(q.Entities, ref) || len(q.Entities) == 0 && q.Filter.Match(&e) {
			out[key] = ref
		}
	}
	return out
}

// keysFor are the wanted targets whose kind can have mt.
func keysFor(w *world, wanted map[scrapeKey]model.EntityRef, mt metric) []scrapeKey {
	var keys []scrapeKey
	for key, ref := range wanted {
		if len(mt.Kinds) == 0 || slices.Contains(mt.Kinds, w.ents[ref].Kind) {
			keys = append(keys, key)
		}
	}
	return keys
}

// seriesOf maps a range query's results back to the targets asked for.
func seriesOf(res *matrix, wanted map[scrapeKey]model.EntityRef, mt metric, win data.TimeWindow) ([]data.Series, error) {
	if res.ResultType != "matrix" {
		return nil, errors.New("answer is a " + res.ResultType + ", not a matrix")
	}
	var out []data.Series
	for _, r := range res.Result {
		ref, ok := wanted[scrapeKey{r.Metric["job"], r.Metric["instance"]}]
		if !ok {
			continue
		}
		ps, err := points(r.Values, win)
		if err != nil {
			return nil, err
		}
		out = append(out, data.Series{Ref: data.SeriesRef{Entity: ref, Metric: mt.Name}, Unit: mt.Unit, Points: ps})
	}
	return out, nil
}
