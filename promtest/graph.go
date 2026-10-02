package promtest

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// The service graph recipe's queries, as the guide gives them.
const (
	GraphRequests = "sum by (client, server) (rate(traces_service_graph_request_total[1m]))"
	GraphFailed   = "sum by (client, server) (rate(traces_service_graph_request_failed_total[1m]))"
	GraphErrors   = "sum by (server) (rate(traces_service_graph_request_failed_total[5m]))"
	GraphP95      = "histogram_quantile(0.95, sum by (server, le) (rate(traces_service_graph_request_server_seconds_bucket[5m])))"
)

// Graph is a generated service graph of Services services and Edges calls, answering the
// recipe's queries as Tempo would. Names are made up; a size gives the same graph every time.
type Graph struct {
	Services, Edges int
	Now             func() time.Time // the clock rates follow; nil is time.Now

	once  sync.Once
	calls []call
	err   error

	mu   sync.Mutex
	last map[vectorAt][]byte // the vectors answered in the latest second
}

type vectorAt struct {
	failed bool
	at     int64
}

// call is one edge: client calls server at base requests a second, failing every fail-th.
type call struct {
	client, server int
	base           float64
	fail           bool
}

// RoundTrip answers r as a Prometheus holding the graph, with no targets; other queries get 404.
func (g *Graph) RoundTrip(r *http.Request) (*http.Response, error) {
	if g.once.Do(g.make); g.err != nil {
		return nil, g.err
	}
	form, err := formOf(r)
	if err != nil {
		return nil, err
	}
	switch q := form.Get("query"); {
	case r.URL.Path == "/api/v1/targets":
		return respond(r, http.StatusOK, []byte(`{"status":"success","data":{"activeTargets":[]}}`)), nil
	case r.URL.Path == "/api/v1/query" && (q == GraphRequests || q == GraphFailed):
		return respond(r, http.StatusOK, g.vector(q == GraphFailed, g.now())), nil
	case r.URL.Path == "/api/v1/query_range" && (q == GraphErrors || q == GraphP95):
		start, err1 := strconv.ParseFloat(form.Get("start"), 64)
		end, err2 := strconv.ParseFloat(form.Get("end"), 64)
		step, err3 := strconv.ParseFloat(form.Get("step"), 64)
		if err1 != nil || err2 != nil || err3 != nil || step <= 0 || end < start {
			return respond(r, http.StatusBadRequest, []byte(`{"status":"error","errorType":"bad_data","error":"bad range"}`)), nil
		}
		return respond(r, http.StatusOK, g.matrix(q == GraphP95, start, end, step)), nil
	}
	return respond(r, http.StatusNotFound, []byte(`{"status":"error","errorType":"not_found","error":"not in the graph"}`)), nil
}

// ServeHTTP answers as RoundTrip does, over HTTP.
func (g *Graph) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp, err := g.RoundTrip(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// GraphConfig is a config of one Prometheus instance, named graph, reading the graph at url by the
// guide's recipe every interval.
func GraphConfig(url string, interval time.Duration) string {
	return fmt.Sprintf(`modules:
  - kind: prometheus
    name: graph
    options:
      url: %s
      interval: %s
      flows:
        - query: %q
          from: {label: client, kind: service, make: true}
          to: {label: server, kind: service, make: true}
          unit: requests
      series:
        - query: %q
          metric: requests.failed
          unit: per_second
          entity: {labels: [server], kind: service}
        - query: %q
          metric: latency.p95
          unit: seconds
          entity: {labels: [server], kind: service}
`, url, interval, GraphRequests, GraphErrors, GraphP95)
}

func (g *Graph) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

// make joins every service to one before it, so all are called but the first, then adds calls
// between random pairs, always from the lower to the higher, so no call goes round in a loop.
func (g *Graph) make() {
	n := g.Services
	if n < 2 || g.Edges < n-1 || g.Edges > n*(n-1)/2 {
		g.err = fmt.Errorf("promtest: no graph of %d services has %d calls", n, g.Edges)
		return
	}
	rng := rand.New(rand.NewPCG(uint64(n), uint64(g.Edges)))
	seen := map[[2]int]bool{}
	add := func(c, s int) {
		if !seen[[2]int{c, s}] {
			seen[[2]int{c, s}] = true
			g.calls = append(g.calls, call{c, s, 1 + rng.Float64()*500, len(g.calls)%10 == 3})
		}
	}
	for s := 1; s < n; s++ {
		add(rng.IntN(s), s)
	}
	for len(g.calls) < g.Edges {
		a, b := rng.IntN(n), rng.IntN(n)
		if a != b {
			add(min(a, b), max(a, b))
		}
	}
}

func service(i int) string { return fmt.Sprintf("svc-%03d", i) }

// rate is a call's requests a second at t, drifting a tenth either way over ten minutes.
func (c call) rate(i int, t time.Time) float64 {
	return c.base * (1 + 0.1*math.Sin(float64(t.Unix())/600*2*math.Pi+float64(i)))
}

// vector answers the requests or failures of every call at t, made once a second.
func (g *Graph) vector(failed bool, t time.Time) []byte {
	k := vectorAt{failed, t.Unix()}
	g.mu.Lock()
	defer g.mu.Unlock()
	if b, ok := g.last[k]; ok {
		return b
	}
	if len(g.last) > 1 || g.last == nil {
		g.last = map[vectorAt][]byte{}
	}
	g.last[k] = g.vectorOf(failed, t)
	return g.last[k]
}

func (g *Graph) vectorOf(failed bool, t time.Time) []byte {
	b := []byte(`{"status":"success","data":{"resultType":"vector","result":[`)
	first := true
	for i, c := range g.calls {
		v := c.rate(i, t)
		if failed {
			if !c.fail {
				continue
			}
			v /= 100
		}
		if !first {
			b = append(b, ',')
		}
		first = false
		b = fmt.Appendf(b, `{"metric":{"client":%q,"server":%q},"value":[%d,"%s"]}`, service(c.client), service(c.server), t.Unix(), strconv.FormatFloat(v, 'g', 6, 64))
	}
	return append(b, "]}}"...)
}

// matrix answers each called service's failures a second, or its p95 latency, at every step.
func (g *Graph) matrix(p95 bool, start, end, step float64) []byte {
	called := map[int][]int{}
	var order []int
	for i, c := range g.calls {
		if _, ok := called[c.server]; !ok {
			order = append(order, c.server)
		}
		called[c.server] = append(called[c.server], i)
	}
	var buf bytes.Buffer
	buf.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for k, s := range order {
		if k > 0 {
			buf.WriteByte(',')
		}
		fmt.Fprintf(&buf, `{"metric":{"server":%q},"values":[`, service(s))
		for j, at := 0, start; at <= end; j, at = j+1, at+step {
			if j > 0 {
				buf.WriteByte(',')
			}
			t := time.Unix(int64(at), 0)
			v := 0.02 + 0.001*float64(s%200) + 0.005*math.Sin(at/300+float64(s))
			if !p95 {
				v = 0
				for _, i := range called[s] {
					if g.calls[i].fail {
						v += g.calls[i].rate(i, t) / 100
					}
				}
			}
			fmt.Fprintf(&buf, `[%s,"%s"]`, strconv.FormatFloat(at, 'f', -1, 64), strconv.FormatFloat(v, 'g', 6, 64))
		}
		buf.WriteString("]}")
	}
	buf.WriteString("]}}")
	return buf.Bytes()
}
