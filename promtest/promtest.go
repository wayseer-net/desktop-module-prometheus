// Package promtest records a module's HTTP traffic with Prometheus and replays it in tests.
// Request headers are never recorded, so credentials stay out of fixtures.
package promtest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
)

// Exchange is one request and its answer; Form holds the query string and any form body.
type Exchange struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Form   url.Values      `json:"form,omitempty"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

// Load reads exchanges saved by a Recorder.
func Load(path string) ([]Exchange, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var xs []Exchange
	return xs, json.Unmarshal(b, &xs)
}

// Recorder passes requests to Next and keeps each exchange, with Scrub's keys replaced by its
// values in paths, forms and bodies.
type Recorder struct {
	Next  http.RoundTripper
	Scrub map[string]string

	mu sync.Mutex
	xs []Exchange
}

// RoundTrip forwards r and records the exchange.
func (rec *Recorder) RoundTrip(r *http.Request) (*http.Response, error) {
	form, err := formOf(r)
	if err != nil {
		return nil, err
	}
	resp, err := rec.Next.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, err
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	x := Exchange{Method: r.Method, Path: r.URL.Path, Form: form, Status: resp.StatusCode, Body: body}
	rec.mu.Lock()
	rec.xs = append(rec.xs, rec.scrubbed(x))
	rec.mu.Unlock()
	return resp, nil
}

func (rec *Recorder) scrubbed(x Exchange) Exchange {
	pairs := make([]string, 0, 2*len(rec.Scrub))
	for from, to := range rec.Scrub {
		pairs = append(pairs, from, to)
	}
	s := strings.NewReplacer(pairs...)
	x.Path = s.Replace(x.Path)
	for k, vs := range x.Form {
		for i := range vs {
			vs[i] = s.Replace(vs[i])
		}
		x.Form[k] = vs
	}
	x.Body = json.RawMessage(s.Replace(string(x.Body)))
	return x
}

// Save writes the exchanges recorded so far, indented, to path.
func (rec *Recorder) Save(path string) error {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", " ")
	if err := enc.Encode(rec.xs); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}

// Replayer answers requests from recorded exchanges, matched by method, path and the "query"
// form value; with Loose, an unmatched request takes the first exchange on its path.
type Replayer struct {
	Exchanges []Exchange
	Loose     bool

	mu   sync.Mutex
	seen []*http.Request
}

// RoundTrip answers r from the recordings, or with 404 when none matches.
func (rp *Replayer) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.Context().Err(); err != nil {
		return nil, err
	}
	form, err := formOf(r)
	if err != nil {
		return nil, err
	}
	rp.mu.Lock()
	rp.seen = append(rp.seen, r.Clone(r.Context()))
	rp.mu.Unlock()
	x, ok := rp.match(r.Method, r.URL.Path, form.Get("query"))
	if !ok {
		return respond(r, http.StatusNotFound, fmt.Appendf(nil, `{"status":"error","errorType":"not_found","error":"no recording of %s %s"}`, r.Method, r.URL.Path)), nil
	}
	return respond(r, x.Status, x.Body), nil
}

func (rp *Replayer) match(method, path, query string) (Exchange, bool) {
	same := func(x Exchange) bool { return x.Method == method && x.Path == path }
	if i := slices.IndexFunc(rp.Exchanges, func(x Exchange) bool { return same(x) && x.Form.Get("query") == query }); i >= 0 {
		return rp.Exchanges[i], true
	}
	if i := slices.IndexFunc(rp.Exchanges, same); rp.Loose && i >= 0 {
		return rp.Exchanges[i], true
	}
	return Exchange{}, false
}

// Requests returns the requests answered so far, headers included.
func (rp *Replayer) Requests() []*http.Request {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return slices.Clone(rp.seen)
}

func respond(r *http.Request, status int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Request: r,
		Header: http.Header{"Content-Type": {"application/json"}},
		Body:   io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)),
	}
}

// formOf reads the query string and a form body, leaving the body readable again.
func formOf(r *http.Request) (url.Values, error) {
	form := r.URL.Query()
	if r.Body == nil {
		return form, nil
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	body, err := url.ParseQuery(string(b))
	if err != nil {
		return nil, err
	}
	for k, vs := range body {
		form[k] = append(form[k], vs...)
	}
	return form, nil
}
