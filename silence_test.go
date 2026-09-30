package prometheus

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mindseye/pkg/sdk"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAM is an Alertmanager that serves alerts and silences, and records the changes it is sent.
type fakeAM struct {
	mu       sync.Mutex
	alerts   []amAlert
	silences map[string]amSilence
	posted   []amSilence
	expired  []string
	auth     []string
	refuse   int    // answer every change with this status, when set
	body     string // and this body
}

func (f *fakeAM) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	id, byID := strings.CutPrefix(r.URL.Path, "/api/v2/silence/")
	switch {
	case f.refuse != 0 && r.Method != http.MethodGet:
		w.WriteHeader(f.refuse)
		_, _ = io.WriteString(w, f.body)
	case r.Method == http.MethodGet && r.URL.Path == "/api/v2/alerts":
		_ = json.NewEncoder(w).Encode(f.alerts)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v2/silences":
		var s amSilence
		if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.posted = append(f.posted, s)
		_, _ = io.WriteString(w, `{"silenceID":"made-here"}`)
	case r.Method == http.MethodGet && byID:
		s, ok := f.silences[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(s)
	case r.Method == http.MethodDelete && byID:
		f.expired = append(f.expired, id)
	default:
		http.NotFound(w, r)
	}
}

// toHost sends requests for host to next, and the rest to rest.
type toHost struct {
	host       string
	next, rest http.RoundTripper
}

func (h toHost) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Host == h.host {
		return h.next.RoundTrip(r)
	}
	return h.rest.RoundTrip(r)
}

var (
	highLoad = amAlert{
		Labels:      map[string]string{"alertname": "HighLoad", "instance": "localhost:19100", "job": "hosts", "severity": "warning"},
		Fingerprint: "239ed139539f9c50",
	}
	highLoadRef = labRef(sdk.KindAlert, highLoad.Fingerprint)
)

// silencing is the lab with fake as its Alertmanager, its alerts read once, and extra options.
func silencing(t *testing.T, fake *fakeAM, extra string) *Module {
	t.Helper()
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	rt := toHost{u.Host, http.DefaultTransport, lab(t, true)}
	m := configured(t, rt, "url: "+labURL+"\nalertmanager:\n  url: "+srv.URL+extra)
	read(t, m)
	return m
}

func act(m *Module, action string, ref sdk.EntityRef, params map[string]string) (sdk.ActionResult, error) {
	return m.Do(context.Background(), sdk.ActionRequest{Instance: "lab", Action: action, Entity: ref, Params: params})
}

var active = &silenceStatus{State: "active"}

func silencedBy(a amAlert, ids ...string) amAlert {
	a.Status.State, a.Status.SilencedBy = "suppressed", ids
	return a
}

func TestTheCatalogueIsValidAndSilenceIsBounded(t *testing.T) {
	acts := New().Actions()
	if err := sdk.ValidateActions(acts); err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(acts, func(a sdk.Action) bool { return a.ID == "silence" })
	if i < 0 || acts[i].Params[0].Min != int64(15*time.Minute) || acts[i].Params[0].Max != int64(7*24*time.Hour) {
		t.Errorf("silence %+v", acts)
	}
}

func TestSilenceMatchesTheAlertsLabelsExactly(t *testing.T) {
	fake := &fakeAM{alerts: []amAlert{highLoad}}
	m := silencing(t, fake, "")
	before := time.Now().Add(-time.Second)
	res, err := act(m, "silence", highLoadRef, map[string]string{"for": "2h"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.posted) != 1 {
		t.Fatalf("posted %+v", fake.posted)
	}
	s := fake.posted[0]
	want := []amMatcher{
		{Name: "alertname", Value: "HighLoad", IsEqual: true},
		{Name: "instance", Value: "localhost:19100", IsEqual: true},
		{Name: "job", Value: "hosts", IsEqual: true},
		{Name: "severity", Value: "warning", IsEqual: true},
	}
	if !slices.Equal(s.Matchers, want) {
		t.Errorf("matchers %+v", s.Matchers)
	}
	if s.StartsAt.Before(before) || s.EndsAt.Sub(s.StartsAt) != 2*time.Hour {
		t.Errorf("from %v to %v", s.StartsAt, s.EndsAt)
	}
	if s.CreatedBy != "lab" || s.Comment != silenceComment {
		t.Errorf("created by %q with %q", s.CreatedBy, s.Comment)
	}
	if !strings.Contains(res.Message, "HighLoad") || !strings.Contains(res.Message, "2h") {
		t.Errorf("message %q", res.Message)
	}
}

func TestSilenceOutsideItsBoundsSendsNothing(t *testing.T) {
	fake := &fakeAM{alerts: []amAlert{highLoad}}
	m := silencing(t, fake, "")
	for _, d := range []string{"14m", "169h", "soon"} {
		if _, err := act(m, "silence", highLoadRef, map[string]string{"for": d}); err == nil {
			t.Errorf("for=%s ran", d)
		}
	}
	if len(fake.posted) != 0 {
		t.Errorf("posted %+v", fake.posted)
	}
}

func TestSilenceRefusesAnAlertGoneOrSilenced(t *testing.T) {
	fake := &fakeAM{alerts: []amAlert{silencedBy(highLoad, "theirs")}}
	m := silencing(t, fake, "")
	for ref, want := range map[sdk.EntityRef]string{
		highLoadRef:                         "already silenced",
		labRef(sdk.KindAlert, "0000000000"): "no longer firing",
		labRef(sdk.KindHost, "promhost"):    "does not apply",
	} {
		if _, err := act(m, "silence", ref, map[string]string{"for": "1h"}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", ref, err, want)
		}
	}
	if len(fake.posted) != 0 {
		t.Errorf("posted %+v", fake.posted)
	}
}

func TestUnsilenceExpiresOnlyItsOwnSilence(t *testing.T) {
	ours := amSilence{ID: "ours", CreatedBy: "lab", Comment: silenceComment, Status: active}
	theirs := amSilence{ID: "theirs", CreatedBy: "oncall", Comment: "maintenance", Status: active}
	fake := &fakeAM{alerts: []amAlert{silencedBy(highLoad, "theirs", "ours")}, silences: map[string]amSilence{"ours": ours, "theirs": theirs}}
	m := silencing(t, fake, "")
	res, err := act(m, "unsilence", highLoadRef, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.expired, []string{"ours"}) || !strings.Contains(res.Message, "HighLoad") {
		t.Errorf("expired %v, message %q", fake.expired, res.Message)
	}
}

func TestUnsilenceLeavesSilencesMadeElsewhere(t *testing.T) {
	theirs := amSilence{ID: "theirs", CreatedBy: "oncall", Comment: silenceComment, Status: active}
	fake := &fakeAM{alerts: []amAlert{silencedBy(highLoad, "theirs")}, silences: map[string]amSilence{"theirs": theirs}}
	m := silencing(t, fake, "")
	for ref, want := range map[sdk.EntityRef]string{highLoadRef: "made elsewhere", labRef(sdk.KindAlert, "0000000000"): "no longer firing"} {
		if _, err := act(m, "unsilence", ref, nil); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", ref, err, want)
		}
	}
	if len(fake.expired) != 0 {
		t.Errorf("expired %v", fake.expired)
	}
}

func TestARefusedSilenceNamesNoCredential(t *testing.T) {
	t.Setenv("AM_TOKEN", token)
	fake := &fakeAM{alerts: []amAlert{highLoad}, refuse: http.StatusUnauthorized, body: "bad token " + token}
	m := silencing(t, fake, "\n  auth: bearer\n  secret_env: AM_TOKEN")
	_, err := act(m, "silence", highLoadRef, map[string]string{"for": "1h"})
	if err == nil || !strings.Contains(err.Error(), "Alertmanager refused the credentials") || strings.Contains(err.Error(), token) {
		t.Errorf("err %v", err)
	}
	if fake.auth[len(fake.auth)-1] != "Bearer "+token {
		t.Errorf("sent %q", fake.auth[len(fake.auth)-1])
	}
}

func TestAnAlertmanagerErrorIsOneLine(t *testing.T) {
	fake := &fakeAM{alerts: []amAlert{highLoad}, refuse: http.StatusBadRequest, body: "silence invalid: bad matcher\ngoroutine 1 [running]"}
	m := silencing(t, fake, "")
	_, err := act(m, "silence", highLoadRef, map[string]string{"for": "1h"})
	if err == nil || !strings.Contains(err.Error(), "bad matcher") || strings.Contains(err.Error(), "goroutine") {
		t.Errorf("err %v", err)
	}
}

func TestACancelledSilenceSendsNothing(t *testing.T) {
	fake := &fakeAM{alerts: []amAlert{highLoad}}
	m := silencing(t, fake, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.Do(ctx, sdk.ActionRequest{Instance: "lab", Action: "silence", Entity: highLoadRef, Params: map[string]string{"for": "1h"}})
	if !errors.Is(err, context.Canceled) || len(fake.posted) != 0 {
		t.Errorf("err %v, posted %+v", err, fake.posted)
	}
}

func TestSilenceWithoutAnAlertmanagerIsRefused(t *testing.T) {
	m := configured(t, lab(t, true), "url: "+labURL)
	if _, err := act(m, "silence", highLoadRef, map[string]string{"for": "1h"}); err == nil || !strings.Contains(err.Error(), "no alertmanager") {
		t.Errorf("err %v", err)
	}
}
