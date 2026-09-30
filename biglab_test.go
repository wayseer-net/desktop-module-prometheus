package prometheus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// bigLab is a generated server the size of a LAN's: hosts with node exporters, and many
// services exporting their CPU. Names are made up; answers are built once and kept.
type bigLab struct {
	hosts, services int

	mu      sync.Mutex
	answers map[string][]byte
}

const bigLabURL = "http://biglab.test:9090"

func (l *bigLab) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	key := r.URL.Path + "?" + r.Form.Encode()
	l.mu.Lock()
	body, ok := l.answers[key]
	if !ok {
		body = l.answer(r.URL.Path, r.Form.Get("query"), r.Form)
		if l.answers == nil {
			l.answers = map[string][]byte{}
		}
		l.answers[key] = body
	}
	l.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK, Request: r,
		Header: http.Header{"Content-Type": {"application/json"}},
		Body:   io.NopCloser(bytes.NewReader(body)),
	}, nil
}

// labTarget is one generated target.
type labTarget struct{ job, instance, node string }

func (l *bigLab) targets() []labTarget {
	var ts []labTarget
	for i := range l.hosts {
		ts = append(ts, labTarget{"node", fmt.Sprintf("host-%02d:9100", i), fmt.Sprintf("host-%02d", i)})
	}
	for i := range l.services {
		ts = append(ts, labTarget{fmt.Sprintf("app-%02d", i%40), fmt.Sprintf("svc-%03d:8080", i), ""})
	}
	return ts
}

func (l *bigLab) answer(path, query string, form map[string][]string) []byte {
	switch {
	case path == "/api/v1/targets":
		var active []map[string]any
		for _, t := range l.targets() {
			active = append(active, map[string]any{
				"labels": map[string]string{"job": t.job, "instance": t.instance}, "health": "up",
				"scrapeUrl": "http://" + t.instance + "/metrics", "scrapeInterval": "15s",
			})
		}
		return envelopeOf(map[string]any{"activeTargets": active})
	case path == "/api/v1/metadata":
		counter := []metaEntry{{Type: "counter"}}
		return envelopeOf(map[string]any{"node_cpu_seconds_total": counter, "process_cpu_seconds_total": counter})
	case path == "/api/v1/query" && query == nodeQuery:
		return envelopeOf(labVector(slices.DeleteFunc(l.targets(), func(t labTarget) bool { return t.node == "" }), true))
	case path == "/api/v1/query" && strings.HasPrefix(query, "topk("):
		n, _ := strconv.Atoi(query[len("topk("):strings.IndexByte(query, ',')])
		ts := l.matching(query)
		return envelopeOf(labVector(ts[:min(n, len(ts))], false))
	case path == "/api/v1/query_range":
		return l.matrix(query, form)
	}
	return envelopeOf(map[string]any{})
}

var instanceMatcher = regexp.MustCompile(`instance=~?"((?:[^"\\]|\\.)*)"`)

// matching are the targets query's instance matcher names, of the kind its metric is for.
func (l *bigLab) matching(query string) []labTarget {
	m := instanceMatcher.FindStringSubmatch(query)
	if m == nil {
		return nil
	}
	pattern, _ := strconv.Unquote(`"` + m[1] + `"`)
	named := regexp.MustCompile("^(?:" + pattern + ")$")
	hosts := strings.Contains(query, "node_")
	return slices.DeleteFunc(l.targets(), func(t labTarget) bool {
		return (t.node != "") != hosts || !named.MatchString(t.instance)
	})
}

func labVector(ts []labTarget, nodes bool) map[string]any {
	var res []map[string]any
	for _, t := range ts {
		m := map[string]string{"job": t.job, "instance": t.instance}
		if nodes {
			m["nodename"] = t.node
		}
		res = append(res, map[string]any{"metric": m, "value": []any{0, "1"}})
	}
	return map[string]any{"resultType": "vector", "result": res}
}

// matrix answers a range query with a series per target, at every step, as Prometheus writes it.
func (l *bigLab) matrix(query string, form map[string][]string) []byte {
	start, _ := strconv.ParseFloat(form["start"][0], 64)
	end, _ := strconv.ParseFloat(form["end"][0], 64)
	step, _ := strconv.ParseFloat(form["step"][0], 64)
	b := []byte(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for i, t := range l.matching(query) {
		if i > 0 {
			b = append(b, ',')
		}
		b = fmt.Appendf(b, `{"metric":{"instance":%q,"job":%q},"values":[`, t.instance, t.job)
		for j, at := 0, start; at <= end; j, at = j+1, at+step {
			if j > 0 {
				b = append(b, ',')
			}
			b = append(b, '[')
			b = strconv.AppendFloat(b, math.Round(at*1e3)/1e3, 'f', -1, 64)
			b = append(b, `,"`...)
			b = strconv.AppendFloat(b, 50+50*math.Sin(float64(i+j)/9), 'f', -1, 64)
			b = append(b, `"]`...)
		}
		b = append(b, "]}"...)
	}
	return append(b, "]}}"...)
}

func envelopeOf(data any) []byte {
	b, err := json.Marshal(map[string]any{"status": "success", "data": data})
	if err != nil {
		panic(err)
	}
	return b
}
