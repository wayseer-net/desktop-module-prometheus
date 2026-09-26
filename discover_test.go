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
	w := buildWorld("lab", "promhost:9090", o.kinds, nil, []target{
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

func TestNodeExportersAreHostsNamedByTheirNodename(t *testing.T) {
	nodes := map[scrapeKey]string{{"kube-nodes", "10.0.0.5:9100"}: "worker-5"}
	w := buildWorld("lab", "promhost:9090", map[string]model.Kind{"custom": model.KindDatabase}, nodes, []target{
		scrapeOf("kube-nodes", "10.0.0.5:9100", "up", ""),
		scrapeOf("kubelet", "10.0.0.5:10250", "up", ""),
		scrapeOf("custom", "10.0.0.5:5432", "up", ""),
		scrapeOf("app", "10.1.2.3:2112", "up", ""),
	})
	worker, _ := model.NewEntityRef("lab", model.KindHost, "worker-5")
	e, ok := w.ents[worker]
	if !ok || e.Attrs["job"].Str() != "kube-nodes" || e.Status.Level != model.StatusOK {
		t.Fatalf("the node exporter is not host worker-5: %v %v", ok, e)
	}
	if k := w.scraped[worker]; k != (scrapeKey{"kube-nodes", "10.0.0.5:9100"}) {
		t.Errorf("worker-5's series come from %v", k)
	}
	for _, native := range []string{"kubelet/10.0.0.5:10250", "custom/10.0.0.5:5432"} {
		kind := model.KindService
		if native == "custom/10.0.0.5:5432" {
			kind = model.KindDatabase
		}
		ref, _ := model.NewEntityRef("lab", kind, native)
		if _, ok := w.edges[model.EdgeKey{From: ref, To: worker, Rel: model.RelRunsOn}]; !ok {
			t.Errorf("%s does not run on worker-5", ref)
		}
	}
	ip, _ := model.NewEntityRef("lab", model.KindHost, "10.0.0.5")
	if _, ok := w.ents[ip]; ok {
		t.Error("the node's address is a host of its own")
	}
}
