// Command recordalerts records an alert's lifecycle in Alertmanager as fixtures for the
// prometheus module's tests: alerts fire, one is silenced, then one resolves.
//
//	go run ./scripts/recordalerts [-url http://localhost:9093] [-out testdata]
//
// It posts the alerts itself, as Prometheus would for the lab's rules, and silences one; use a
// throwaway Alertmanager. Tests never run this.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"wayseer.dev/modules/prometheus/promtest"
)

// alert is what Prometheus posts to /api/v2/alerts.
type alert struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations,omitempty"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt,omitzero"`
	GeneratorURL string            `json:"generatorURL"`
}

// lab's alerts: on a target, on a host by instance, on a job, and on nothing the server scrapes.
func lab(start time.Time) []alert {
	a := func(name, severity, summary string, labels ...string) alert {
		ls := map[string]string{"alertname": name, "severity": severity}
		for i := 0; i+1 < len(labels); i += 2 {
			ls[labels[i]] = labels[i+1]
		}
		return alert{
			Labels: ls, Annotations: map[string]string{"summary": summary}, StartsAt: start,
			GeneratorURL: "http://promhost:9090/graph?g0.expr=" + name,
		}
	}
	return []alert{
		a("TargetDown", "critical", "api target 127.0.0.1:19999 is down", "job", "api", "instance", "127.0.0.1:19999"),
		a("HighLoad", "warning", "load is high on localhost:19100", "job", "hosts", "instance", "localhost:19100"),
		a("JobDegraded", "warning", "some api targets are down", "job", "api"),
		a("DiskFilling", "info", "disk fills within a week", "instance", "localhost:19100"),
		a("Watchdog", "none", "always firing"),
	}
}

func main() {
	url := flag.String("url", "http://localhost:9093", "a throwaway Alertmanager")
	out := flag.String("out", "testdata", "directory for alerts-<stage>.json")
	flag.Parse()
	if err := record(*url, *out); err != nil {
		fmt.Fprintln(os.Stderr, "recordalerts:", err)
		os.Exit(1)
	}
}

func record(url, out string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	alerts := lab(time.Now().Add(-5 * time.Minute).Truncate(time.Second))
	if err := send(ctx, url+"/api/v2/alerts", alerts); err != nil {
		return err
	}
	if err := stage(ctx, url, filepath.Join(out, "alerts-firing.json")); err != nil {
		return err
	}
	silence := map[string]any{
		"matchers": []map[string]any{{"name": "alertname", "value": "HighLoad", "isRegex": false, "isEqual": true}},
		"startsAt": time.Now(), "endsAt": time.Now().Add(time.Hour),
		"createdBy": "recordalerts", "comment": "maintenance",
	}
	if err := send(ctx, url+"/api/v2/silences", silence); err != nil {
		return err
	}
	if err := stage(ctx, url, filepath.Join(out, "alerts-silenced.json")); err != nil {
		return err
	}
	alerts[0].EndsAt = time.Now()
	if err := send(ctx, url+"/api/v2/alerts", alerts[:1]); err != nil {
		return err
	}
	time.Sleep(time.Second)
	return stage(ctx, url, filepath.Join(out, "alerts-resolved.json"))
}

// stage records one read of the alerts, as the module makes it.
func stage(ctx context.Context, url, path string) error {
	rec := &promtest.Recorder{Next: http.DefaultTransport}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/api/v2/alerts", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Transport: rec}).Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	fmt.Println("wrote", path)
	return rec.Save(path)
}

func send(ctx context.Context, url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return nil
}
