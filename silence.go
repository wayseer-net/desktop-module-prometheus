package prometheus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"
	"wayseer/pkg/sdk"
)

// silenceComment marks the silences Wayseer makes, with the instance's name as createdBy.
const silenceComment = "Wayseer"

// amSilence is a silence as Alertmanager's /api/v2 takes and gives it.
type amSilence struct {
	ID        string         `json:"id,omitempty"`
	Matchers  []amMatcher    `json:"matchers"`
	StartsAt  time.Time      `json:"startsAt"`
	EndsAt    time.Time      `json:"endsAt"`
	CreatedBy string         `json:"createdBy"`
	Comment   string         `json:"comment"`
	Status    *silenceStatus `json:"status,omitempty"` // only in answers
}

type silenceStatus struct {
	State string `json:"state"` // active, pending or expired
}

type amMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"isRegex"`
	IsEqual bool   `json:"isEqual"`
}

// Actions are silence and unsilence, on alerts.
func (m *Module) Actions() []sdk.Action {
	onAlerts := []sdk.Kind{sdk.KindAlert}
	return []sdk.Action{
		{
			ID: "silence", Title: "Silence", Kinds: onAlerts,
			Changes: "Alertmanager stops notifying for this alert, matched by all its labels, for a while",
			Params:  []sdk.Param{sdk.DurationParam("for", "For", 15*time.Minute, 7*24*time.Hour).WithDefault("1h")},
		},
		{
			ID: "unsilence", Title: "Unsilence", Kinds: onAlerts,
			Changes: "Expires the silence Wayseer made for this alert; silences made elsewhere stay",
		},
	}
}

// Do silences or unsilences the alert req names, as last read; it checks req again.
func (m *Module) Do(ctx context.Context, req sdk.ActionRequest) (sdk.ActionResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ActionResult{}, err
	}
	acts := m.Actions()
	i := slices.IndexFunc(acts, func(a sdk.Action) bool { return a.ID == req.Action })
	if i < 0 {
		return sdk.ActionResult{}, fmt.Errorf("no action %q", req.Action)
	}
	params, err := acts[i].Check(req.Params)
	if err != nil {
		return sdk.ActionResult{}, err
	}
	req.Params = params
	if req.Entity.Kind() != sdk.KindAlert {
		return sdk.ActionResult{}, fmt.Errorf("the action does not apply to %s", req.Entity)
	}
	m.mu.Lock()
	c, name := m.am, m.name
	a, ok := m.alerts[req.Entity.Native()]
	m.mu.Unlock()
	switch {
	case c == nil:
		return sdk.ActionResult{}, errors.New("no alertmanager is configured for this instance")
	case !ok:
		return sdk.ActionResult{}, errors.New("the alert is no longer firing")
	case req.Action == "silence":
		return silence(ctx, c, &a.amAlert, string(name), req.Duration("for"))
	}
	return unsilence(ctx, c, &a.amAlert, string(name))
}

// silence makes a silence matching a's labels exactly, from now for d.
func silence(ctx context.Context, c *client, a *amAlert, by string, d time.Duration) (sdk.ActionResult, error) {
	if len(a.Status.SilencedBy) > 0 {
		return sdk.ActionResult{}, fmt.Errorf("%s is already silenced", a.name())
	}
	now := time.Now()
	s := amSilence{Matchers: matchersOf(a.Labels), StartsAt: now, EndsAt: now.Add(d), CreatedBy: by, Comment: silenceComment}
	var made struct {
		ID string `json:"silenceID"`
	}
	if err := c.send(ctx, http.MethodPost, "/api/v2/silences", s, &made); err != nil {
		return sdk.ActionResult{}, amError(err)
	}
	return sdk.ActionResult{Message: fmt.Sprintf("silenced %s for %s, until %s", a.name(), shortSpan(d), s.EndsAt.Format("Mon 15:04"))}, nil
}

// unsilence expires the silences on a that this instance made, leaving the rest.
func unsilence(ctx context.Context, c *client, a *amAlert, by string) (sdk.ActionResult, error) {
	if len(a.Status.SilencedBy) == 0 {
		return sdk.ActionResult{}, fmt.Errorf("%s is not silenced", a.name())
	}
	n := 0
	for _, id := range a.Status.SilencedBy {
		if id == "" || strings.ContainsAny(id, "/?#%") {
			continue
		}
		var s amSilence
		if err := c.get(ctx, "/api/v2/silence/"+id, nil, &s); err != nil {
			return sdk.ActionResult{}, amError(err)
		}
		if !madeBy(&s, by) {
			continue
		}
		if err := c.send(ctx, http.MethodDelete, "/api/v2/silence/"+id, nil, nil); err != nil {
			return sdk.ActionResult{}, amError(err)
		}
		n++
	}
	if n == 0 {
		return sdk.ActionResult{}, fmt.Errorf("%s has no silence Wayseer made; silences made elsewhere are left alone", a.name())
	}
	return sdk.ActionResult{Message: "unsilenced " + a.name()}, nil
}

// madeBy reports whether s is an active silence this instance made.
func madeBy(s *amSilence, by string) bool {
	return s.CreatedBy == by && (s.Comment == silenceComment || s.Comment == mindsEyeComment) && s.Status != nil && s.Status.State == "active"
}

// matchersOf matches each label exactly, by name.
func matchersOf(labels map[string]string) []amMatcher {
	out := make([]amMatcher, 0, len(labels))
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		out = append(out, amMatcher{Name: k, Value: labels[k], IsEqual: true})
	}
	return out
}

// amError says why Alertmanager failed a change; a refusal of the credentials says only that.
func amError(err error) error {
	var ae *authError
	if errors.As(err, &ae) {
		return fmt.Errorf("the Alertmanager refused the credentials (HTTP %d)", ae.code)
	}
	return fmt.Errorf("alertmanager: %w", err)
}

// shortSpan is d without trailing zero units, such as "2h" or "1h30m".
func shortSpan(d time.Duration) string {
	s := strings.TrimSuffix(d.String(), "0s")
	if strings.HasSuffix(s, "h0m") {
		s = strings.TrimSuffix(s, "0m")
	}
	return s
}
