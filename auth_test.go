package prometheus

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"mindseye/pkg/sdk/sdktest"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const token = "s3cret-t0ken"

func TestOptionsAreChecked(t *testing.T) {
	t.Setenv("PROM_TOKEN", token)
	for _, tc := range []struct{ opts, want string }{
		{"url: ftp://promhost", "must be http:// or https://"},
		{"url: http://user:pw@promhost:9090", "must not hold credentials"},
		{"url: http://promhost:9090/?x=1", "must not have a query"},
		{"timeout: 10ms", "timeout 10ms"},
		{"interval: 2h", "interval 2h0m0s"},
		{"auth: digest\nsecret_env: PROM_TOKEN", `auth "digest"`},
		{"auth: bearer", "needs one of secret_file or secret_env"},
		{"auth: basic\nsecret_env: PROM_TOKEN", "needs a username"},
		{"auth: bearer\nusername: u\nsecret_env: PROM_TOKEN", "takes no username"},
		{"secret_env: PROM_TOKEN", "need auth: basic or bearer"},
		{"auth: bearer\nsecret_env: PROM_UNSET_VAR", "PROM_UNSET_VAR is not set"},
		{"auth: bearer\nsecret_file: /no/such/secret", "/no/such/secret"},
		{"kinds: {node: Host}", `job "node"`},
	} {
		err := NewWithTransport(stall{}).Configure(context.Background(), cfg(t, tc.opts))
		switch {
		case err == nil:
			t.Errorf("%q was accepted", tc.opts)
		case !strings.Contains(err.Error(), tc.want):
			t.Errorf("%q: %v; want %q", tc.opts, err, tc.want)
		case strings.Contains(err.Error(), token):
			t.Errorf("%q: the error holds the secret", tc.opts)
		}
	}
}

func TestTheSecretIsSentAsTheAuthorizationHeader(t *testing.T) {
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PROM_PASSWORD", token)
	for _, tc := range []struct{ opts, want string }{
		{"auth: bearer\nsecret_file: " + file, "Bearer " + token},
		{"auth: basic\nusername: grafana\nsecret_env: PROM_PASSWORD", "Basic " + base64.StdEncoding.EncodeToString([]byte("grafana:"+token))},
		{"auth: none", ""},
	} {
		rp := lab(t, true)
		m := configured(t, rp, "url: "+labURL+"\n"+tc.opts)
		if _, err := m.Discover(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, r := range rp.Requests() {
			if got := r.Header.Get("Authorization"); got != tc.want {
				t.Errorf("%s: Authorization %q, want %q", tc.opts, got, tc.want)
			}
		}
	}
}

func TestTheSecretNeverAppearsInErrors(t *testing.T) {
	t.Setenv("PROM_PASSWORD", token)
	for _, tc := range []struct {
		status int
		body   string
		want   string
	}{
		{http.StatusUnauthorized, `{"status":"error","error":"bad token ` + token + `"}`, "authentication failed (HTTP 401)"},
		{http.StatusBadRequest, `{"status":"error","errorType":"bad_data","error":"header was Basic ` + token + `"}`, "bad_data: header was Basic [secret]"},
		{http.StatusBadGateway, `proxy saw ` + token, "HTTP 502"},
	} {
		m := configured(t, echo{tc.status, tc.body}, "url: "+labURL+"\nauth: basic\nusername: u\nsecret_env: PROM_PASSWORD")
		sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
		sdktest.Eventually(t, func() bool { return m.Health().Err != nil })
		got := m.Health().Err.Error()
		if !strings.Contains(got, tc.want) || strings.Contains(got, token) {
			t.Errorf("HTTP %d reads %q; want %q, without the secret", tc.status, got, tc.want)
		}
	}
}

// echo answers every request with status and body.
type echo struct {
	status int
	body   string
}

func (e echo) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: e.status, Body: io.NopCloser(bytes.NewBufferString(e.body)), Request: r, Header: http.Header{}}, nil
}
