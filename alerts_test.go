package prometheus

import (
	"context"
	"errors"
	"mindseye/modules/prometheus/promtest"
	"mindseye/pkg/sdk"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const amURL = "http://amhost:9093"

// withAlerts is the lab with an Alertmanager answering from the stage it is set to.
type withAlerts struct {
	targets http.RoundTripper
	stage   atomic.Pointer[promtest.Replayer]
}

func (w *withAlerts) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == "amhost:9093" {
		if rp := w.stage.Load(); rp != nil {
			return rp.RoundTrip(r)
		}
		return nil, errConnRefused
	}
	return w.targets.RoundTrip(r)
}

var errConnRefused = errors.New("connection refused")

// set makes the Alertmanager answer as recorded in testdata/prometheus/alerts-<stage>.json;
// "" makes it refuse.
func (w *withAlerts) set(t *testing.T, stage string) {
	t.Helper()
	if stage == "" {
		w.stage.Store(nil)
		return
	}
	xs, err := promtest.Load("../../testdata/prometheus/alerts-" + stage + ".json")
	if err != nil {
		t.Fatal(err)
	}
	w.stage.Store(&promtest.Replayer{Exchanges: xs})
}

func alerting(t *testing.T) (*Module, *withAlerts) {
	t.Helper()
	rt := &withAlerts{targets: lab(t, true)}
	return configured(t, rt, "url: "+labURL+"\nalertmanager:\n  url: "+amURL), rt
}

// read is one refresh's changes, failing on an error.
func read(t *testing.T, m *Module) *sdk.ChangeSet {
	t.Helper()
	cs, err := m.refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return cs
}

func eventFor(t *testing.T, cs *sdk.ChangeSet, name string) sdk.Event {
	t.Helper()
	i := slices.IndexFunc(cs.Events, func(e sdk.Event) bool { return e.Kind == "alert" && e.Fields["alertname"].Str() == name })
	if i < 0 {
		t.Fatalf("no event for %s in %+v", name, cs.Events)
	}
	return cs.Events[i]
}

func TestAnAlertsLifecycleShowsAsStatusAndEvents(t *testing.T) {
	m, am := alerting(t)
	host, target := labRef(sdk.KindHost, "promhost"), labRef(sdk.KindService, "api/127.0.0.1:19999")

	am.set(t, "firing")
	checkFiring(t, m, read(t, m))
	if len(read(t, m).Events) != 0 {
		t.Error("an unchanged alert was sent again")
	}

	am.set(t, "silenced")
	cs := read(t, m)
	if got := m.world.ents[host]; got.Status.Level != sdk.StatusOK || got.Attrs["alerts_muted"].Str() != "HighLoad" {
		t.Errorf("silenced: the host reads %+v with %v", got.Status, got.Attrs)
	}
	if e := eventFor(t, cs, "HighLoad"); e.Message != "HighLoad silenced" || e.Severity != sdk.SevInfo {
		t.Errorf("silenced: %q at %v", e.Message, e.Severity)
	}

	am.set(t, "resolved")
	cs = read(t, m)
	if e := eventFor(t, cs, "TargetDown"); e.Message != "TargetDown resolved" || e.Entity != target || e.Severity != sdk.SevInfo {
		t.Errorf("resolved: %q on %s at %v", e.Message, e.Entity, e.Severity)
	}
	if _, ok := m.world.ents[target].Attrs["alerts"]; ok {
		t.Error("a resolved alert is still on its target")
	}
}

// checkFiring checks the lab's alerts as first read: on their entities, graded by severity.
func checkFiring(t *testing.T, m *Module, cs *sdk.ChangeSet) {
	t.Helper()
	host, target := labRef(sdk.KindHost, "promhost"), labRef(sdk.KindService, "api/127.0.0.1:19999")
	if got := m.world.ents[host].Status; got != (sdk.Status{Level: sdk.StatusWarn, Reason: "HighLoad"}) {
		t.Errorf("firing: the host reads %+v", got)
	}
	if got := m.world.ents[host].Attrs["alerts"].Str(); got != "DiskFilling, HighLoad" {
		t.Errorf("firing: the host's alerts are %q", got)
	}
	for name, want := range map[string]struct {
		on  sdk.EntityRef
		sev sdk.Severity
	}{
		"TargetDown":  {target, sdk.SevCritical},
		"HighLoad":    {host, sdk.SevWarn},
		"JobDegraded": {labRef(KindJob, "api"), sdk.SevWarn},
		"DiskFilling": {host, sdk.SevInfo},
		"Watchdog":    {labRef(sdk.KindService, "server"), sdk.SevInfo},
	} {
		e := eventFor(t, cs, name)
		if e.Entity != want.on || e.Severity != want.sev || !strings.HasPrefix(e.Message, name+" firing") {
			t.Errorf("%s: %s %v %q; want on %s at %v", name, e.Entity, e.Severity, e.Message, want.on, want.sev)
		}
	}
}

func TestAnAlertRaisesButNeverLowersStatus(t *testing.T) {
	m, am := alerting(t)
	am.set(t, "firing")
	read(t, m)
	target := m.world.ents[labRef(sdk.KindService, "api/127.0.0.1:19999")]
	if target.Status.Level != sdk.StatusDown || !strings.Contains(target.Status.Reason, "connection refused") {
		t.Errorf("a critical alert on a down target reads %+v", target.Status)
	}
	if job := m.world.ents[labRef(KindJob, "api")]; job.Status.Level != sdk.StatusCrit || job.Attrs["alerts"].Str() != "JobDegraded" {
		t.Errorf("a warning on a crit job reads %+v with %v", job.Status, job.Attrs)
	}
}

func TestUnmatchedAlertsAttachToTheServer(t *testing.T) {
	m, am := alerting(t)
	am.set(t, "firing")
	read(t, m)
	if got := m.world.ents[labRef(sdk.KindService, "server")].Attrs["alerts"].Str(); got != "Watchdog" {
		t.Errorf("the server's alerts are %q", got)
	}
}

func TestAnUnreachableAlertmanagerResolvesNothing(t *testing.T) {
	m, am := alerting(t)
	am.set(t, "firing")
	read(t, m)
	am.set(t, "")
	cs := read(t, m)
	if len(cs.Events) != 0 {
		t.Errorf("an unreachable Alertmanager sent %+v", cs.Events)
	}
	if note := m.Health().Note; !strings.Contains(note, "alertmanager") || !strings.Contains(note, "connection refused") {
		t.Errorf("health note %q", note)
	}
	if m.Health().Err != nil {
		t.Errorf("an unreachable Alertmanager fails the module: %v", m.Health().Err)
	}
	if got := m.world.ents[labRef(sdk.KindHost, "promhost")].Status.Reason; got != "HighLoad" {
		t.Errorf("the last alerts were forgotten: %q", got)
	}
}

func TestSeverityLabelsGrade(t *testing.T) {
	for sev, want := range map[string]struct {
		level sdk.StatusLevel
		event sdk.Severity
	}{
		"critical": {sdk.StatusCrit, sdk.SevCritical},
		"warning":  {sdk.StatusWarn, sdk.SevWarn},
		"":         {sdk.StatusWarn, sdk.SevWarn},
		"page":     {sdk.StatusWarn, sdk.SevWarn},
		"info":     {sdk.StatusUnknown, sdk.SevInfo},
		"none":     {sdk.StatusUnknown, sdk.SevInfo},
	} {
		if l, e := grade(sev); l != want.level || e != want.event {
			t.Errorf("%q grades %v %v, want %v %v", sev, l, e, want.level, want.event)
		}
	}
}

func TestFiringEventsAreDatedWhenTheAlertStarted(t *testing.T) {
	m, am := alerting(t)
	am.set(t, "firing")
	e := eventFor(t, read(t, m), "TargetDown")
	if want := time.Date(2026, 9, 26, 6, 1, 28, 0, time.UTC); !e.At.Equal(want) {
		t.Errorf("fired at %v, want %v", e.At, want)
	}
}

func TestAlertmanagerOptionsAreChecked(t *testing.T) {
	for _, tc := range []struct{ opts, want string }{
		{"alertmanager:\n  url: ftp://amhost", "alertmanager: url"},
		{"alertmanager:\n  url: http://amhost:9093\n  auth: bearer", "alertmanager: auth bearer needs"},
	} {
		err := NewWithTransport(stall{}).Configure(context.Background(), cfg(t, tc.opts))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%q: %v; want %q", tc.opts, err, tc.want)
		}
	}
}

func TestAlertmanagerHasItsOwnCredentials(t *testing.T) {
	t.Setenv("AM_TOKEN", token)
	rt := &withAlerts{targets: lab(t, true)}
	m := configured(t, rt, "url: "+labURL+"\nalertmanager:\n  url: "+amURL+"\n  auth: bearer\n  secret_env: AM_TOKEN")
	rt.set(t, "firing")
	read(t, m)
	for _, r := range rt.stage.Load().Requests() {
		if got := r.Header.Get("Authorization"); got != "Bearer "+token {
			t.Errorf("%s sent %q", r.URL.Path, got)
		}
	}
	for _, r := range rt.targets.(*promtest.Replayer).Requests() {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Prometheus was sent Alertmanager's secret on %s", r.URL.Path)
		}
	}
}

func TestEachAlertIsAnEntityOnWhatItIsAbout(t *testing.T) {
	m, am := alerting(t)
	host, highLoad, targetDown := labRef(sdk.KindHost, "promhost"), labRef(sdk.KindAlert, "239ed139539f9c50"), labRef(sdk.KindAlert, "7c9dec2ac85c10cf")
	am.set(t, "firing")
	read(t, m)
	e := m.world.ents[highLoad]
	if e.Name != "HighLoad" || e.Kind != sdk.KindAlert || e.Status != (sdk.Status{Level: sdk.StatusWarn, Reason: "load is high on localhost:19100"}) ||
		e.Attrs["state"].Str() != "firing" || e.Attrs["label.severity"].Str() != "warning" {
		t.Errorf("firing: %+v", e)
	}
	if _, ok := m.world.edges[sdk.Edge{From: highLoad, To: host, Rel: sdk.RelMemberOf}.Key()]; !ok {
		t.Error("the alert is not a member of its host")
	}

	am.set(t, "silenced")
	read(t, m)
	if e := m.world.ents[highLoad]; e.Status != (sdk.Status{Level: sdk.StatusUnknown, Reason: "silenced"}) || e.Attrs["state"].Str() != "silenced" {
		t.Errorf("silenced: %+v", e)
	}

	am.set(t, "resolved")
	read(t, m)
	if _, ok := m.world.ents[targetDown]; ok {
		t.Error("a resolved alert is still an entity")
	}
}
