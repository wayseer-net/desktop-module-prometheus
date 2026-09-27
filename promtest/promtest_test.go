package promtest

import (
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRecordingScrubsHostsAndReplaysByQuery(t *testing.T) {
	server := &Replayer{Exchanges: []Exchange{
		{Method: "POST", Path: "/api/v1/query_range", Form: url.Values{"query": {"up"}}, Status: 200, Body: []byte(`{"host":"lanbox"}`)},
	}}
	rec := &Recorder{Next: server, Scrub: map[string]string{"lanbox": "promhost"}}
	req, _ := http.NewRequest("POST", "http://lanbox:9090/api/v1/query_range", strings.NewReader("query=up&start=1"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer secret")
	if _, err := rec.RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "x.json")
	if err := rec.Save(path); err != nil {
		t.Fatal(err)
	}
	saved, _ := os.ReadFile(path)
	if s := string(saved); strings.Contains(s, "lanbox") || strings.Contains(s, "secret") || !strings.Contains(s, "promhost") {
		t.Errorf("recording kept a host or header:\n%s", s)
	}
	xs, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		loose  bool
		query  string
		status int
	}{{false, "up", 200}, {false, "down", 404}, {true, "down", 200}} {
		rp := &Replayer{Exchanges: xs, Loose: tc.loose}
		req, _ := http.NewRequest("GET", "http://promhost/api/v1/query_range?query="+tc.query, nil)
		req.Method = "POST"
		resp, err := rp.RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != tc.status {
			t.Errorf("loose %v, query %s: HTTP %d %s", tc.loose, tc.query, resp.StatusCode, body)
		}
	}
}
