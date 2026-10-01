package prometheus

import (
	"maps"
	"math"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"wayseer/pkg/sdk"
)

// maxPoints is Prometheus's limit on points per series in one range query.
const maxPoints = 11000

// selector matches the series scraped from keys.
func selector(keys []scrapeKey) string {
	jobs, insts := map[string]bool{}, map[string]bool{}
	for _, k := range keys {
		jobs[k.job], insts[k.instance] = true, true
	}
	return matcher("job", jobs) + "," + matcher("instance", insts)
}

func matcher(label string, values map[string]bool) string {
	vs := slices.Sorted(maps.Keys(values))
	if len(vs) == 1 {
		return label + "=" + strconv.Quote(vs[0])
	}
	for i, v := range vs {
		vs[i] = regexp.QuoteMeta(v)
	}
	return label + "=~" + strconv.Quote(strings.Join(vs, "|"))
}

// expression is the PromQL for m over the targets of kind sel matches, one series per target.
func expression(m metric, kind sdk.Kind, agg sdk.Aggregation, sel string, rateRange time.Duration) string {
	rng := strconv.FormatInt(int64(rateRange/time.Second), 10) + "s"
	if m.exprs != nil {
		return strings.NewReplacer("$sel", sel, "$range", rng).Replace(m.exprs[kind])
	}
	inner := m.Name + "{" + sel + "}"
	if m.counter {
		inner = "rate(" + inner + "[" + rng + "])"
	}
	switch agg {
	case sdk.AggP95:
		return "quantile by (job, instance) (0.95, " + inner + ")"
	case sdk.AggAvg, sdk.AggMax, sdk.AggMin, sdk.AggSum:
		return agg.String() + " by (job, instance) (" + inner + ")"
	}
	if m.counter {
		return "sum by (job, instance) (" + inner + ")"
	}
	return "avg by (job, instance) (" + inner + ")"
}

// rangeFor is the rate window: at least a step, and four scrapes so a missed one does not
// leave a gap.
func rangeFor(step, scrape time.Duration) time.Duration {
	if scrape <= 0 {
		scrape = 15 * time.Second
	}
	return max(step, 4*scrape).Round(time.Second)
}

// stepFor is q's step, coarsened so no series exceeds Prometheus's point limit.
func stepFor(q sdk.SeriesQuery) time.Duration {
	step := q.Step
	if step <= 0 {
		step = sdk.StepFor(q.Window, 1000)
	}
	return max(step, q.Window.Span()/maxPoints+1, time.Millisecond)
}

// rangeForm is the query_range request for expr over w at step.
func rangeForm(expr string, w sdk.TimeWindow, step time.Duration) url.Values {
	return url.Values{
		"query": {expr},
		"start": {seconds(w.From.UnixNano())},
		"end":   {seconds(w.To.UnixNano())},
		"step":  {strconv.FormatFloat(step.Seconds(), 'f', -1, 64)},
	}
}

func seconds(ns int64) string { return strconv.FormatFloat(float64(ns)/1e9, 'f', 3, 64) }

// matrix is a range query's answer.
type matrix struct {
	ResultType string `json:"resultType"`
	Result     []struct {
		Metric map[string]string `json:"metric"`
		Values samples           `json:"values"`
	} `json:"result"`
}

// points reads a result's samples inside w, leaving out ones that are not numbers.
func points(values samples, w sdk.TimeWindow) ([]sdk.Point, error) {
	if values.err != nil {
		return nil, values.err
	}
	out := make([]sdk.Point, 0, len(values.at))
	for _, v := range values.at {
		ns := int64(math.Round(v.t*1e3)) * int64(time.Millisecond)
		if v.ok && w.Contains(ns) {
			out = append(out, sdk.Point{T: ns, V: v.v})
		}
	}
	return out, nil
}
