package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"wayseer/pkg/sdk"
)

// UseWorld gives the module the world, to match flow ends against what other modules found.
func (m *Module) UseWorld(world func() sdk.Resolver) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resolve = world
}

// vector is an instant query's answer.
type vector struct {
	ResultType string `json:"resultType"`
	Result     []struct {
		Metric map[string]string  `json:"metric"`
		Value  [2]json.RawMessage `json:"value"`
	} `json:"result"`
}

// flowCount is how many series named ends that match no entity, or several.
type flowCount struct{ unknown, ambiguous int }

// readFlows adds each flow query's traffic to w as talks_to edges between entities in the world,
// returning a note of what could not be read or matched. A query that fails keeps its last edges.
func (m *Module) readFlows(ctx context.Context, w *world) string {
	m.mu.Lock()
	c, flows, resolve, last := m.client, m.opts.Flows, m.resolve, m.flowEdges
	m.mu.Unlock()
	if len(flows) == 0 {
		return ""
	}
	if resolve == nil {
		return "flows: no world to match against"
	}
	r := resolve()
	next := make([][]sdk.Edge, len(flows))
	var count flowCount
	var msgs []string
	for i, f := range flows {
		var res vector
		err := c.post(ctx, "/api/v1/query", map[string][]string{"query": {f.Query}}, &res)
		if err == nil && res.ResultType != "vector" {
			err = errors.New("the answer is a " + res.ResultType + ", not a vector")
		}
		if err != nil {
			msgs = append(msgs, fmt.Sprintf("flow %d: %s", i+1, queryError(err)))
			if i < len(last) {
				next[i] = last[i]
			}
			continue
		}
		next[i] = flowEdges(m.name, f, &res, r, &count)
	}
	for _, es := range next {
		for _, e := range es {
			if _, ok := w.edges[e.Key()]; !ok {
				w.edges[e.Key()] = e
			}
		}
	}
	m.mu.Lock()
	m.flowEdges = next
	m.mu.Unlock()
	return notes(append(msgs, count.note())...)
}

// flowEdges sums the answer's rates by the entities its ends name, leaving out a flow within
// one entity and a series whose end names no one entity.
func flowEdges(src sdk.ModuleID, f flowQuery, res *vector, r sdk.Resolver, count *flowCount) []sdk.Edge {
	sums := map[sdk.EdgeKey]float64{}
	var order []sdk.EdgeKey
	for _, s := range res.Result {
		rate, ok := sampleValue(s.Value[1])
		if !ok {
			continue
		}
		from, err1 := r.Match(f.From.Kind, s.Metric[f.From.Label])
		to, err2 := r.Match(f.To.Kind, s.Metric[f.To.Label])
		switch {
		case errors.Is(err1, sdk.ErrAmbiguous) || errors.Is(err2, sdk.ErrAmbiguous):
			count.ambiguous++
			continue
		case err1 != nil || err2 != nil:
			count.unknown++
			continue
		case from == to:
			continue
		}
		k := sdk.EdgeKey{From: from, To: to, Rel: sdk.RelTalksTo}
		if _, ok := sums[k]; !ok {
			order = append(order, k)
		}
		sums[k] += rate
	}
	out := make([]sdk.Edge, len(order))
	for i, k := range order {
		out[i] = sdk.Edge{From: k.From, To: k.To, Rel: k.Rel, Weight: 1, Source: src, Traffic: sdk.Traffic{Rate: sums[k], Unit: f.Unit}}
	}
	return out
}

// sampleValue reads a sample's value, refusing one that is not a rate.
func sampleValue(raw json.RawMessage) (float64, bool) {
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) && f >= 0
}

// queryError says why an owner's query failed without what the server said, which may quote it.
func queryError(err error) string {
	var ae *apiError
	if errors.As(err, &ae) {
		return "the server refused the query (" + ae.kind + ")"
	}
	return err.Error()
}

func (c flowCount) note() string {
	var ns []string
	if c.unknown > 0 {
		ns = append(ns, fmt.Sprintf("flows: %d with an end not in the world", c.unknown))
	}
	if c.ambiguous > 0 {
		ns = append(ns, fmt.Sprintf("flows: %d with an end naming several entities", c.ambiguous))
	}
	return notes(ns...)
}
