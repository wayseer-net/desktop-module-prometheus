package prometheus

import (
	"slices"
	"testing"
)

func TestUnsilenceExpiresASilenceMindsEyeMade(t *testing.T) {
	old := amSilence{ID: "old", CreatedBy: "lab", Comment: "Mind's Eye", Status: active}
	fake := &fakeAM{alerts: []amAlert{silencedBy(highLoad, "old")}, silences: map[string]amSilence{"old": old}}
	if _, err := act(silencing(t, fake, ""), "unsilence", highLoadRef, nil); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(fake.expired, []string{"old"}) {
		t.Errorf("expired %v, want the silence Mind's Eye made", fake.expired)
	}
}
