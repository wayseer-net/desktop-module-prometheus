package prometheus

import (
	"cmp"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"time"

	"wayseer.dev/sdk"
)

// Kinds and relations the module adds to the core vocabulary.
const (
	KindJob sdk.Kind = "prometheus/job"
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
	ents      map[sdk.EntityRef]sdk.Entity
	edges     map[sdk.EdgeKey]sdk.Edge
	src       sdk.ModuleID
	server    sdk.EntityRef
	hostNames map[string]string           // a node exporter's address to its nodename
	scraped   map[sdk.EntityRef]scrapeKey // the targets, to query their series
	interval  time.Duration               // the longest scrape interval, for rate ranges
}

// builder turns targets into a world.
type builder struct {
	src   sdk.ModuleID
	kinds map[string]sdk.Kind
	nodes map[scrapeKey]string // node exporters, by their nodename
	names map[string]string    // a node exporter's address to its nodename
	w     world
	on    map[sdk.EntityRef][]target // the hosts only named by instances, and the targets on them
}

// buildWorld lists the server, its jobs, their targets and the hosts they run on; nodes are the
// node exporters among the targets, by nodename.
func buildWorld(src sdk.ModuleID, base string, kinds map[string]sdk.Kind, nodes map[scrapeKey]string, ts []target) world {
	b := builder{src: src, kinds: kinds, nodes: nodes, names: map[string]string{}, w: world{
		ents: map[sdk.EntityRef]sdk.Entity{}, edges: map[sdk.EdgeKey]sdk.Edge{},
		scraped: map[sdk.EntityRef]scrapeKey{}, src: src,
	}, on: map[sdk.EntityRef][]target{}}
	b.w.server = b.add(sdk.KindService, "server", base, sdk.Status{Level: sdk.StatusOK}, map[string]sdk.Value{"url": sdk.String(base)})
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
		ref := b.add(KindJob, job, job, jobStatus(jobs[job]), map[string]sdk.Value{"targets": sdk.Number(float64(len(jobs[job])))})
		b.edge(ref, b.w.server, sdk.RelMemberOf)
	}
	for ref, key := range b.w.scraped {
		b.edge(ref, b.ref(KindJob, key.job), sdk.RelMemberOf)
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
		kind = sdk.KindHost
	default:
		kind = sdk.KindService
	}
	status, attrs := targetStatus(t), targetAttrs(t)
	var ref sdk.EntityRef
	if kind == sdk.KindHost {
		ref = b.merge(host, status, attrs)
	} else {
		ref = b.add(kind, job+"/"+inst, job+"/"+inst, status, attrs)
		if _, ok := b.w.ents[b.ref(sdk.KindHost, host)]; !ok {
			b.add(sdk.KindHost, host, host, sdk.Status{}, nil)
		}
		b.edge(ref, b.ref(sdk.KindHost, host), sdk.RelRunsOn)
		b.on[b.ref(sdk.KindHost, host)] = append(b.on[b.ref(sdk.KindHost, host)], t)
	}
	b.w.scraped[ref] = scrapeKey{job, inst}
	if d, err := time.ParseDuration(t.ScrapeInterval); err == nil {
		b.w.interval = max(b.w.interval, d)
	}
}

// merge adds a host scraped as a target, keeping the worse status when several jobs scrape it.
func (b *builder) merge(host string, st sdk.Status, attrs map[string]sdk.Value) sdk.EntityRef {
	ref := b.ref(sdk.KindHost, host)
	if old, ok := b.w.ents[ref]; ok && old.Status.Level.Worse(st.Level) {
		return ref
	}
	return b.add(sdk.KindHost, host, host, st, attrs)
}

func (b *builder) ref(kind sdk.Kind, native string) sdk.EntityRef {
	r, _ := sdk.NewEntityRef(string(b.src), kind, native)
	return r
}

func (b *builder) add(kind sdk.Kind, native, name string, st sdk.Status, attrs map[string]sdk.Value) sdk.EntityRef {
	ref := b.ref(kind, native)
	b.w.ents[ref] = sdk.Entity{Ref: ref, Kind: kind, Name: name, Status: st, Attrs: attrs, Source: b.src}
	return ref
}

func (b *builder) edge(from, to sdk.EntityRef, rel sdk.Relation) {
	e := sdk.Edge{From: from, To: to, Rel: rel, Source: b.src}
	b.w.edges[e.Key()] = e
}

// hostOf is an instance's host without its port.
func hostOf(instance string) string {
	if h, _, err := net.SplitHostPort(instance); err == nil {
		return h
	}
	return strings.Trim(instance, "[]")
}

func targetStatus(t target) sdk.Status {
	switch t.Health {
	case "up":
		return sdk.Status{Level: sdk.StatusOK}
	case "down":
		return sdk.Status{Level: sdk.StatusDown, Reason: clip(firstLine(t.LastError), reasonCap)}
	}
	return sdk.Status{Level: sdk.StatusUnknown, Reason: "not scraped yet"}
}

// targetAttrs are the target's own labels and how its scrapes go.
func targetAttrs(t target) map[string]sdk.Value {
	attrs := map[string]sdk.Value{
		"job": sdk.String(t.Labels["job"]), "instance": sdk.String(t.Labels["instance"]),
		"scrape_url": sdk.String(t.ScrapeURL), "scrape_interval": sdk.String(t.ScrapeInterval),
		"scrape_duration": sdk.Number(t.LastScrapeDuration).In(sdk.UnitSeconds),
	}
	for k, v := range t.Labels {
		if k != "job" && k != "instance" {
			attrs["label."+k] = sdk.String(v)
		}
	}
	if t.LastError != "" {
		attrs["last_error"] = sdk.String(clip(t.LastError, errorCap))
	}
	return attrs
}

// hostStatus is a host's reachability from the targets on it: OK when any answers, Down when
// none does.
func hostStatus(on []target) sdk.Status {
	firstErr := ""
	for _, t := range on {
		switch {
		case t.Health == "up":
			return sdk.Status{Level: sdk.StatusOK}
		case t.Health == "down" && firstErr == "":
			firstErr = cmp.Or(firstLine(t.LastError), "down")
		}
	}
	if firstErr == "" {
		return sdk.Status{Level: sdk.StatusUnknown, Reason: "not scraped yet"}
	}
	return sdk.Status{Level: sdk.StatusDown, Reason: clip("no target on it answers: "+firstErr, reasonCap)}
}

// jobStatus is Crit when every target is down, Warn when some are.
func jobStatus(health []string) sdk.Status {
	down := 0
	for _, h := range health {
		if h == "down" {
			down++
		}
	}
	switch {
	case down == 0:
		return sdk.Status{Level: sdk.StatusOK}
	case down == len(health):
		return sdk.Status{Level: sdk.StatusCrit, Reason: "every target is down"}
	}
	return sdk.Status{Level: sdk.StatusWarn, Reason: fmt.Sprintf("%d of %d targets down", down, len(health))}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}
