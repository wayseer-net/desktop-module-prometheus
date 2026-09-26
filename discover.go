package prometheus

import (
	"cmp"
	"fmt"
	"maps"
	"mindseye/internal/model"
	"net"
	"slices"
	"strings"
	"time"
)

// Kinds and relations the module adds to the core vocabulary.
const (
	KindJob model.Kind = "prometheus/job"
)

// reasonCap bounds a scrape error kept as a status reason.
const reasonCap = 200

// target is one active target from /api/v1/targets.
type target struct {
	Labels             map[string]string `json:"labels"`
	ScrapeURL          string            `json:"scrapeUrl"`
	LastError          string            `json:"lastError"`
	LastScrapeDuration float64           `json:"lastScrapeDuration"`
	Health             string            `json:"health"`
	ScrapeInterval     string            `json:"scrapeInterval"`
}

type targets struct {
	Active []target `json:"activeTargets"`
}

// scrapeKey names a target by the labels every series scraped from it carries.
type scrapeKey struct{ job, instance string }

// world is the server's targets as entities.
type world struct {
	ents      map[model.EntityRef]model.Entity
	edges     map[model.EdgeKey]model.Edge
	src       model.ModuleID
	server    model.EntityRef
	hostNames map[string]string             // a node exporter's address to its nodename
	scraped   map[model.EntityRef]scrapeKey // the targets, to query their series
	interval  time.Duration                 // the longest scrape interval, for rate ranges
}

// builder turns targets into a world.
type builder struct {
	src   model.ModuleID
	kinds map[string]model.Kind
	nodes map[scrapeKey]string // node exporters, by their nodename
	names map[string]string    // a node exporter's address to its nodename
	w     world
	on    map[model.EntityRef][]target // the hosts only named by instances, and the targets on them
}

// buildWorld lists the server, its jobs, their targets and the hosts they run on; nodes are the
// node exporters among the targets, by nodename.
func buildWorld(src model.ModuleID, base string, kinds map[string]model.Kind, nodes map[scrapeKey]string, ts []target) world {
	b := builder{src: src, kinds: kinds, nodes: nodes, names: map[string]string{}, w: world{
		ents: map[model.EntityRef]model.Entity{}, edges: map[model.EdgeKey]model.Edge{},
		scraped: map[model.EntityRef]scrapeKey{}, src: src,
	}, on: map[model.EntityRef][]target{}}
	b.w.server = b.add(model.KindService, "server", base, model.Status{Level: model.StatusOK}, map[string]model.Value{"url": model.String(base)})
	for key, name := range nodes {
		b.names[hostOf(key.instance)] = name
	}
	b.w.hostNames = b.names
	jobs := map[string][]string{} // job to its targets' health
	for _, t := range ts {
		job, inst := t.Labels["job"], t.Labels["instance"]
		if job == "" || inst == "" {
			continue
		}
		b.target(t, job, inst)
		jobs[job] = append(jobs[job], t.Health)
	}
	for _, job := range slices.Sorted(maps.Keys(jobs)) {
		ref := b.add(KindJob, job, job, jobStatus(jobs[job]), map[string]model.Value{"targets": model.Number(float64(len(jobs[job])))})
		b.edge(ref, b.w.server, model.RelMemberOf)
	}
	for ref, key := range b.w.scraped {
		b.edge(ref, b.ref(KindJob, key.job), model.RelMemberOf)
	}
	for ref, on := range b.on {
		if _, scraped := b.w.scraped[ref]; !scraped {
			e := b.w.ents[ref]
			e.Status = hostStatus(on)
			b.w.ents[ref] = e
		}
	}
	return b.w
}

// target adds t: a host itself when it is a node exporter or its job maps to hosts, else a
// service on its host.
func (b *builder) target(t target, job, inst string) {
	host := cmp.Or(b.names[hostOf(inst)], hostOf(inst))
	kind := b.kinds[job]
	switch {
	case kind != "":
	case b.nodes[scrapeKey{job, inst}] != "":
		kind = model.KindHost
	default:
		kind = model.KindService
	}
	status, attrs := targetStatus(t), targetAttrs(t)
	var ref model.EntityRef
	if kind == model.KindHost {
		ref = b.merge(host, status, attrs)
	} else {
		ref = b.add(kind, job+"/"+inst, job+"/"+inst, status, attrs)
		if _, ok := b.w.ents[b.ref(model.KindHost, host)]; !ok {
			b.add(model.KindHost, host, host, model.Status{}, nil)
		}
		b.edge(ref, b.ref(model.KindHost, host), model.RelRunsOn)
		b.on[b.ref(model.KindHost, host)] = append(b.on[b.ref(model.KindHost, host)], t)
	}
	b.w.scraped[ref] = scrapeKey{job, inst}
	if d, err := time.ParseDuration(t.ScrapeInterval); err == nil {
		b.w.interval = max(b.w.interval, d)
	}
}

// merge adds a host scraped as a target, keeping the worse status when several jobs scrape it.
func (b *builder) merge(host string, st model.Status, attrs map[string]model.Value) model.EntityRef {
	ref := b.ref(model.KindHost, host)
	if old, ok := b.w.ents[ref]; ok && old.Status.Level.Worse(st.Level) {
		return ref
	}
	return b.add(model.KindHost, host, host, st, attrs)
}

func (b *builder) ref(kind model.Kind, native string) model.EntityRef {
	r, _ := model.NewEntityRef(string(b.src), kind, native)
	return r
}

func (b *builder) add(kind model.Kind, native, name string, st model.Status, attrs map[string]model.Value) model.EntityRef {
	ref := b.ref(kind, native)
	b.w.ents[ref] = model.Entity{Ref: ref, Kind: kind, Name: name, Status: st, Attrs: attrs, Source: b.src}
	return ref
}

func (b *builder) edge(from, to model.EntityRef, rel model.Relation) {
	e := model.Edge{From: from, To: to, Rel: rel, Source: b.src}
	b.w.edges[e.Key()] = e
}

// hostOf is an instance's host without its port.
func hostOf(instance string) string {
	if h, _, err := net.SplitHostPort(instance); err == nil {
		return h
	}
	return strings.Trim(instance, "[]")
}

func targetStatus(t target) model.Status {
	switch t.Health {
	case "up":
		return model.Status{Level: model.StatusOK}
	case "down":
		return model.Status{Level: model.StatusDown, Reason: clip(firstLine(t.LastError), reasonCap)}
	}
	return model.Status{Level: model.StatusUnknown, Reason: "not scraped yet"}
}

// targetAttrs are the target's own labels and how its scrapes go.
func targetAttrs(t target) map[string]model.Value {
	attrs := map[string]model.Value{
		"job": model.String(t.Labels["job"]), "instance": model.String(t.Labels["instance"]),
		"scrape_url": model.String(t.ScrapeURL), "scrape_interval": model.String(t.ScrapeInterval),
		"scrape_duration": model.Number(t.LastScrapeDuration),
	}
	for k, v := range t.Labels {
		if k != "job" && k != "instance" {
			attrs["label."+k] = model.String(v)
		}
	}
	if t.LastError != "" {
		attrs["last_error"] = model.String(clip(t.LastError, errorCap))
	}
	return attrs
}

// hostStatus is a host's reachability from the targets on it: OK when any answers, Down when
// none does.
func hostStatus(on []target) model.Status {
	firstErr := ""
	for _, t := range on {
		switch {
		case t.Health == "up":
			return model.Status{Level: model.StatusOK}
		case t.Health == "down" && firstErr == "":
			firstErr = cmp.Or(firstLine(t.LastError), "down")
		}
	}
	if firstErr == "" {
		return model.Status{Level: model.StatusUnknown, Reason: "not scraped yet"}
	}
	return model.Status{Level: model.StatusDown, Reason: clip("no target on it answers: "+firstErr, reasonCap)}
}

// jobStatus is Crit when every target is down, Warn when some are.
func jobStatus(health []string) model.Status {
	down := 0
	for _, h := range health {
		if h == "down" {
			down++
		}
	}
	switch {
	case down == 0:
		return model.Status{Level: model.StatusOK}
	case down == len(health):
		return model.Status{Level: model.StatusCrit, Reason: "every target is down"}
	}
	return model.Status{Level: model.StatusWarn, Reason: fmt.Sprintf("%d of %d targets down", down, len(health))}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}
