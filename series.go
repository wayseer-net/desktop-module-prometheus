package prometheus

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"mindseye/pkg/sdk"
	"slices"
	"strings"
)

// seriesCount is how many series, as a metric was last read, named no entity or several.
type seriesCount struct{ unknown, ambiguous int }

// joinedMetrics are the owner's series as catalogue entries for other modules' entities; the
// query stays with the module.
func joinedMetrics(qs []seriesQuery) []sdk.Metric {
	out := make([]sdk.Metric, len(qs))
	for i, s := range qs {
		out[i] = sdk.Metric{Name: s.Metric, Unit: s.Unit, Description: s.Description, Kinds: []sdk.Kind{s.Entity.Kind}, Joined: true}
	}
	return out
}

// seriesFor is the owner's series named metric, if there is one.
func (m *Module) seriesFor(metric string) (seriesQuery, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := slices.IndexFunc(m.opts.Series, func(s seriesQuery) bool { return s.Metric == metric })
	if i < 0 {
		return seriesQuery{}, false
	}
	return m.opts.Series[i], true
}

// querySeriesOf asks for s's query over q's window and gives each entity in the world its
// series, adding up the series that name one entity; q.Entities, when set, are the only ones
// answered. What named no entity, or several, is counted for Health.
func (m *Module) querySeriesOf(ctx context.Context, s seriesQuery, q sdk.SeriesQuery) ([]sdk.Series, error) {
	m.mu.Lock()
	c, resolve := m.client, m.resolve
	m.mu.Unlock()
	if resolve == nil {
		return nil, fmt.Errorf("%s: no world to match against", s.Metric)
	}
	var res matrix
	if err := c.post(ctx, "/api/v1/query_range", rangeForm(s.Query, q.Window, stepFor(q)), &res); err != nil {
		return nil, fmt.Errorf("%s: %s", s.Metric, queryError(err))
	}
	if res.ResultType != "matrix" {
		return nil, fmt.Errorf("%s: the answer is a %s, not a matrix", s.Metric, res.ResultType)
	}
	r := resolve()
	byRef := map[sdk.EntityRef][]sdk.Point{}
	var count seriesCount
	for _, rs := range res.Result {
		ref, err := s.Entity.match(r, rs.Metric)
		switch {
		case errors.Is(err, sdk.ErrAmbiguous):
			count.ambiguous++
			continue
		case err != nil:
			count.unknown++
			continue
		case len(q.Entities) > 0 && !slices.Contains(q.Entities, ref):
			continue
		}
		ps, err := points(rs.Values, q.Window)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.Metric, err)
		}
		byRef[ref] = addPoints(byRef[ref], ps)
	}
	m.mu.Lock()
	m.seriesCounts[s.Metric] = count
	m.mu.Unlock()
	refs := slices.Sorted(maps.Keys(byRef))
	if q.Top > 0 && len(refs) > q.Top {
		slices.SortStableFunc(refs, func(a, b sdk.EntityRef) int { return cmp.Compare(lastValue(byRef[b]), lastValue(byRef[a])) })
		refs = refs[:q.Top]
	}
	out := make([]sdk.Series, len(refs))
	for i, ref := range refs {
		out[i] = sdk.Series{Ref: sdk.SeriesRef{Entity: ref, Metric: s.Metric}, Unit: s.Unit, Points: byRef[ref]}
	}
	return out, nil
}

// match is the entity of e's kind that labels name: their values joined by "/", as a native
// ID, or failing that the last value alone, as a name.
func (e seriesEntity) match(r sdk.Resolver, labels map[string]string) (sdk.EntityRef, error) {
	vs := make([]string, len(e.Labels))
	for i, l := range e.Labels {
		if vs[i] = labels[l]; vs[i] == "" {
			return "", sdk.ErrNoMatch
		}
	}
	ref, err := r.Match(e.Kind, strings.Join(vs, "/"))
	if errors.Is(err, sdk.ErrNoMatch) && len(vs) > 1 {
		return r.Match(e.Kind, vs[len(vs)-1])
	}
	return ref, err
}

// addPoints adds b's values to a's at the same instants, keeping instants only one has.
func addPoints(a, b []sdk.Point) []sdk.Point {
	if len(a) == 0 {
		return b
	}
	out := make([]sdk.Point, 0, max(len(a), len(b)))
	for len(a) > 0 && len(b) > 0 {
		switch {
		case a[0].T < b[0].T:
			out, a = append(out, a[0]), a[1:]
		case b[0].T < a[0].T:
			out, b = append(out, b[0]), b[1:]
		default:
			out = append(out, sdk.Point{T: a[0].T, V: a[0].V + b[0].V})
			a, b = a[1:], b[1:]
		}
	}
	return append(append(out, a...), b...)
}

func lastValue(ps []sdk.Point) float64 {
	if len(ps) == 0 {
		return 0
	}
	return ps[len(ps)-1].V
}

// seriesNote counts, by metric, the series last read that named no entity or several.
func (m *Module) seriesNote() string {
	var ns []string
	for _, name := range slices.Sorted(maps.Keys(m.seriesCounts)) {
		c := m.seriesCounts[name]
		if c.unknown > 0 {
			ns = append(ns, fmt.Sprintf("series %s: %d naming no entity in the world", name, c.unknown))
		}
		if c.ambiguous > 0 {
			ns = append(ns, fmt.Sprintf("series %s: %d naming several entities", name, c.ambiguous))
		}
	}
	return notes(ns...)
}
