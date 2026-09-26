package prometheus

import (
	"cmp"
	"context"
	"maps"
	"mindseye/internal/model"
	"slices"
	"strconv"
	"strings"
	"time"
)

// amAlert is one alert from Alertmanager's /api/v2/alerts, which lists only unresolved ones.
type amAlert struct {
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    time.Time         `json:"startsAt"`
	Fingerprint string            `json:"fingerprint"`
	Status      struct {
		State       string   `json:"state"` // active, suppressed or unprocessed
		SilencedBy  []string `json:"silencedBy"`
		InhibitedBy []string `json:"inhibitedBy"`
	} `json:"status"`
}

func (a *amAlert) name() string { return cmp.Or(a.Labels["alertname"], a.Fingerprint) }

// muted is why a suppressed alert does not notify: silenced, inhibited or muted; "" when it does.
func (a *amAlert) muted() string {
	switch {
	case a.Status.State != "suppressed":
		return ""
	case len(a.Status.SilencedBy) > 0:
		return "silenced"
	case len(a.Status.InhibitedBy) > 0:
		return "inhibited"
	}
	return "muted"
}

// alertOn is an alert and the entity it was matched to.
type alertOn struct {
	amAlert
	on model.EntityRef
}

// grade is the status an alert's severity label gives its entity, and its events' severity;
// info and none alerts leave the status alone.
func grade(severity string) (model.StatusLevel, model.Severity) {
	switch severity {
	case "critical":
		return model.StatusCrit, model.SevCritical
	case "info", "none":
		return model.StatusUnknown, model.SevInfo
	}
	return model.StatusWarn, model.SevWarn
}

// readAlerts reads the alerts, matches them to w's entities and returns the events since the
// last read; when Alertmanager fails the last alerts stand and the error is noted.
func (m *Module) readAlerts(ctx context.Context, w *world, now time.Time) []model.Event {
	m.mu.Lock()
	c := m.am
	m.mu.Unlock()
	if c == nil {
		return nil
	}
	var got []amAlert
	err := c.get(ctx, "/api/v2/alerts", nil, &got)
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		m.amNote = "alertmanager: " + err.Error()
		m.alerts = rematched(w, m.alerts)
		return nil
	}
	cur := matched(w, got)
	evs := alertEvents(m.name, m.alerts, cur, now)
	m.alerts, m.amNote = cur, ""
	return evs
}

// matched is each alert on the entity its labels name in w, by fingerprint.
func matched(w *world, as []amAlert) map[string]alertOn {
	out := make(map[string]alertOn, len(as))
	for _, a := range as {
		out[a.Fingerprint] = alertOn{a, w.alertTarget(a.Labels)}
	}
	return out
}

// rematched is alerts matched again to w's entities.
func rematched(w *world, alerts map[string]alertOn) map[string]alertOn {
	out := make(map[string]alertOn, len(alerts))
	for fp, a := range alerts {
		out[fp] = alertOn{a.amAlert, w.alertTarget(a.Labels)}
	}
	return out
}

// alertTarget is the target a job and instance name, else the host an instance names, else the
// job, else the server.
func (w *world) alertTarget(labels map[string]string) model.EntityRef {
	job, inst := labels["job"], labels["instance"]
	for ref, key := range w.scraped {
		if key == (scrapeKey{job, inst}) {
			return ref
		}
	}
	if inst != "" {
		if ref, ok := w.has(model.KindHost, cmp.Or(w.hostNames[hostOf(inst)], hostOf(inst))); ok {
			return ref
		}
	}
	if ref, ok := w.has(KindJob, job); ok && job != "" {
		return ref
	}
	return w.server
}

func (w *world) has(kind model.Kind, native string) (model.EntityRef, bool) {
	ref, err := model.NewEntityRef(string(w.src), kind, native)
	if err != nil {
		return "", false
	}
	_, ok := w.ents[ref]
	return ref, ok
}

// alertEvents are the alerts that fired, were muted or unmuted, or resolved between two reads.
func alertEvents(src model.ModuleID, was, is map[string]alertOn, now time.Time) []model.Event {
	var out []model.Event
	for _, fp := range slices.Sorted(maps.Keys(is)) {
		a, old, seen := is[fp], was[fp], false
		if _, seen = was[fp]; seen && old.muted() == a.muted() {
			continue
		}
		switch {
		case a.muted() != "":
			out = append(out, alertEvent(src, &a, a.muted(), model.SevInfo, now))
		case !seen:
			_, sev := grade(a.Labels["severity"])
			out = append(out, alertEvent(src, &a, "firing", sev, a.StartsAt))
		default:
			_, sev := grade(a.Labels["severity"])
			out = append(out, alertEvent(src, &a, "firing", sev, now))
		}
	}
	for _, fp := range slices.Sorted(maps.Keys(was)) {
		if _, ok := is[fp]; !ok {
			a := was[fp]
			out = append(out, alertEvent(src, &a, "resolved", model.SevInfo, now))
		}
	}
	return out
}

func alertEvent(src model.ModuleID, a *alertOn, what string, sev model.Severity, at time.Time) model.Event {
	msg := a.name() + " " + what
	if s := a.Annotations["summary"]; s != "" && what == "firing" {
		msg += ": " + clip(firstLine(s), reasonCap)
	}
	fields := map[string]model.Value{"alertname": model.String(a.name()), "fingerprint": model.String(a.Fingerprint)}
	for k, v := range a.Labels {
		if k != "alertname" {
			fields["label."+k] = model.String(v)
		}
	}
	if s := a.Annotations["summary"]; s != "" {
		fields["summary"] = model.String(clip(s, errorCap))
	}
	return model.Event{
		ID: "alert:" + a.Fingerprint + ":" + what + ":" + strconv.FormatInt(at.UnixNano(), 10), Entity: a.on,
		At: at, Severity: sev, Kind: "alert", Message: msg, Fields: fields, Source: src,
	}
}

// showAlerts lists each entity's alerts in its attributes and raises its status to the worst
// unmuted one's; muted alerts are listed apart and leave the status alone.
func showAlerts(w *world, alerts map[string]alertOn) {
	type onEntity struct{ firing, muted []*alertOn }
	by := map[model.EntityRef]*onEntity{}
	for _, fp := range slices.Sorted(maps.Keys(alerts)) {
		a := alerts[fp]
		o := by[a.on]
		if o == nil {
			o = &onEntity{}
			by[a.on] = o
		}
		if a.muted() != "" {
			o.muted = append(o.muted, &a)
		} else {
			o.firing = append(o.firing, &a)
		}
	}
	for ref, o := range by {
		e, ok := w.ents[ref]
		if !ok {
			continue
		}
		e.Attrs = maps.Clone(e.Attrs)
		if e.Attrs == nil {
			e.Attrs = map[string]model.Value{}
		}
		setNames(e.Attrs, "alerts", o.firing)
		setNames(e.Attrs, "alerts_muted", o.muted)
		if st := worst(o.firing); st.Level.Worse(e.Status.Level) {
			e.Status = st
		}
		w.ents[ref] = e
	}
}

func setNames(attrs map[string]model.Value, key string, as []*alertOn) {
	if len(as) > 0 {
		attrs[key] = model.String(strings.Join(names(as), ", "))
	}
}

// worst is the status the most severe alerts give, named by them; Unknown when none raises it.
func worst(as []*alertOn) model.Status {
	level := model.StatusUnknown
	for _, a := range as {
		l, _ := grade(a.Labels["severity"])
		level = max(level, l)
	}
	if level == model.StatusUnknown {
		return model.Status{}
	}
	at := slices.DeleteFunc(slices.Clone(as), func(a *alertOn) bool { l, _ := grade(a.Labels["severity"]); return l != level })
	return model.Status{Level: level, Reason: clip(strings.Join(names(at), ", "), reasonCap)}
}

func names(as []*alertOn) []string {
	out := make([]string, len(as))
	for i, a := range as {
		out[i] = a.name()
	}
	slices.Sort(out)
	return slices.Compact(out)
}
