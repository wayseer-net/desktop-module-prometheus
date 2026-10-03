package prometheus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"wayseer.dev/sdk"
)

// topServer answers the topk instant query with its one target, and range queries with a
// sample for each target the query names; everything else goes to the lab recording.
type topServer struct {
	lab     http.RoundTripper
	job     string
	inst    string
	mu      sync.Mutex
	topks   []string // the instant queries' expressions and times
	ranges  []string // the range queries' expressions
	tAnswer string   // a sample time inside the window, in seconds
}

func (s *topServer) RoundTrip(r *http.Request) (*http.Response, error) {
	isTop := r.URL.Path == "/api/v1/query" && r.Method == http.MethodPost
	if !isTop && r.URL.Path != "/api/v1/query_range" {
		return s.lab.RoundTrip(r)
	}
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	q := r.PostForm.Get("query")
	s.mu.Lock()
	defer s.mu.Unlock()
	sample := fmt.Sprintf(`{"metric":{"job":%q,"instance":%q}`, s.job, s.inst)
	if isTop {
		s.topks = append(s.topks, q+" @"+r.PostForm.Get("time"))
		return answer(r, `{"resultType":"vector","result":[`+sample+`,"value":[`+s.tAnswer+`,"7"]}]}`), nil
	}
	s.ranges = append(s.ranges, q)
	if !strings.Contains(q, `instance="`+s.inst+`"`) {
		return answer(r, `{"resultType":"matrix","result":[]}`), nil
	}
	return answer(r, `{"resultType":"matrix","result":[`+sample+`,"values":[[`+s.tAnswer+`,"7"]]}]}`), nil
}

func answer(r *http.Request, data string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"status":"success","data":` + data + `}`)), Request: r,
	}
}

func TestATopQueryAsksTheServerForTheTopTargetsFirst(t *testing.T) {
	rp := lab(t, false)
	w := recordedWindow(t, rp)
	srv := &topServer{lab: rp, job: "prometheus", inst: "localhost:19090", tAnswer: seconds(w.To.UnixNano())}
	m := configured(t, srv, "url: "+labURL)
	running(t, m)
	if !m.RanksTop() {
		t.Error("Prometheus does not rank")
	}
	services := []sdk.EntityRef{
		labRef(sdk.KindService, "api/127.0.0.1:19999"), labRef(sdk.KindService, "api/localhost:19998"),
		labRef(sdk.KindService, "prometheus/localhost:19090"),
	}
	q := sdk.SeriesQuery{Entities: services, Metrics: []string{"go_goroutines"}, Window: w, Step: sdk.StepFor(w, 100), Top: 1}
	got, err := m.QuerySeries(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if want := labRef(sdk.KindService, "prometheus/localhost:19090"); len(got) != 1 || got[0].Ref.Entity != want {
		t.Fatalf("the top 1 is %+v, want %s", got, want)
	}
	if len(srv.topks) != 1 || !strings.HasPrefix(srv.topks[0], "topk(1, ") || !strings.HasSuffix(srv.topks[0], " @"+seconds(w.To.UnixNano())) {
		t.Errorf("instant queries %q, want one topk(1, …) at the window's end", srv.topks)
	}
	if len(srv.ranges) != 1 || strings.Contains(srv.ranges[0], "19998") || strings.Contains(srv.ranges[0], "19999") {
		t.Errorf("range queries %q, want one naming only the top target", srv.ranges)
	}

	srv.topks, srv.ranges = nil, nil
	q.Top = 5
	if _, err := m.QuerySeries(context.Background(), q); err != nil || len(srv.topks) != 0 || len(srv.ranges) != 1 {
		t.Errorf("a top above the targets' count sent %q and %d range queries, %v; want no topk", srv.topks, len(srv.ranges), err)
	}
}
