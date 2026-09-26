package prometheus

import (
	"mindseye/internal/model"
	"testing"
)

func scrapeOf(job, instance, health, lastError string) target {
	return target{Labels: map[string]string{"job": job, "instance": instance}, Health: health, LastError: lastError}
}

func TestAHostReadsFromTheTargetsOnIt(t *testing.T) {
	o := defaults()
	if err := o.parseKinds(); err != nil {
		t.Fatal(err)
	}
	w := buildWorld("lab", "promhost:9090", o.kinds, []target{
		scrapeOf("api", "web-1:80", "up", ""),
		scrapeOf("kubelet", "web-1:10250", "down", "connection refused"),
		scrapeOf("api", "web-2:80", "down", "connection refused"),
		scrapeOf("kubelet", "web-2:10250", "down", "i/o timeout"),
		scrapeOf("api", "web-3:80", "unknown", ""),
		scrapeOf("node-exporter", "db-1:9100", "up", ""),
	})
	for _, tc := range []struct {
		host   string
		level  model.StatusLevel
		reason string
	}{
		{"web-1", model.StatusOK, ""},
		{"web-2", model.StatusDown, "no target on it answers: connection refused"},
		{"web-3", model.StatusUnknown, "not scraped yet"},
		{"db-1", model.StatusOK, ""},
	} {
		ref, _ := model.NewEntityRef("lab", model.KindHost, tc.host)
		e, ok := w.ents[ref]
		if !ok || e.Status.Level != tc.level || e.Status.Reason != tc.reason {
			t.Errorf("%s: %v %v; want %v %q", tc.host, ok, e.Status, tc.level, tc.reason)
		}
	}
}
