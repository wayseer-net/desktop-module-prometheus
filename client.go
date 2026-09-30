package prometheus

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mindseye/pkg/sdk"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	bodyCap  = 64 << 20 // largest answer read
	errorCap = 300      // longest server message kept in an error
)

// client calls the Prometheus or Alertmanager HTTP API, adding credentials and keeping them out
// of errors.
type client struct {
	base    *url.URL
	http    *http.Client
	timeout time.Duration
	header  string // Authorization value; empty for none
	bare    bool   // answers are the data itself, as Alertmanager's are, not in an envelope
}

func newClient(e *endpoint, timeout time.Duration, rt http.RoundTripper, s sdk.Secret) *client {
	c := &client{base: e.base, http: &http.Client{Transport: rt}, timeout: timeout}
	switch e.Auth {
	case authBasic:
		c.header = "Basic " + base64.StdEncoding.EncodeToString([]byte(e.Username+":"+s.Reveal()))
	case authBearer:
		c.header = "Bearer " + s.Reveal()
	}
	return c
}

// envelope is every API answer's wrapping.
type envelope struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
}

// get calls path with form as its query, decoding the answer's data into out.
func (c *client) get(ctx context.Context, path string, form url.Values, out any) error {
	return c.call(ctx, request{method: http.MethodGet, path: path, query: form}, out)
}

// post calls path with form as its body, as Prometheus accepts for long queries.
func (c *client) post(ctx context.Context, path string, form url.Values, out any) error {
	body := []byte(form.Encode())
	return c.call(ctx, request{method: http.MethodPost, path: path, body: body, contentType: "application/x-www-form-urlencoded"}, out)
}

// send calls path with v as a JSON body, decoding the answer into out unless it is nil.
func (c *client) send(ctx context.Context, method, path string, v, out any) error {
	var rq request
	rq.method, rq.path = method, path
	if v != nil {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		rq.body, rq.contentType = b, "application/json"
	}
	return c.call(ctx, rq, out)
}

// request is one call to the API.
type request struct {
	method, path string
	query        url.Values
	body         []byte
	contentType  string
}

func (c *client) call(ctx context.Context, rq request, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	err := c.do(ctx, rq, out)
	var ae *authError
	switch {
	case err == nil:
		return nil
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("%s: no answer within %v: %w", rq.path, c.timeout, context.DeadlineExceeded)
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.As(err, &ae):
		return fmt.Errorf("%s: %w", rq.path, ae) // holds no server text
	}
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err // drop the request's URL
	}
	return c.redact(fmt.Errorf("%s: %w", rq.path, err))
}

func (c *client) do(ctx context.Context, rq request, out any) error {
	u := *c.base
	u.Path += rq.path
	u.RawQuery = rq.query.Encode()
	var body io.Reader
	if rq.body != nil {
		body = bytes.NewReader(rq.body)
	}
	req, err := http.NewRequestWithContext(ctx, rq.method, u.String(), body)
	if err != nil {
		return err
	}
	if rq.contentType != "" {
		req.Header.Set("Content-Type", rq.contentType)
	}
	if c.header != "" {
		req.Header.Set("Authorization", c.header)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if c.bare {
		return decodeBare(resp, out)
	}
	return decode(resp, out)
}

// decode reads an answer into out, or the error it reports.
func decode(resp *http.Response, out any) error {
	if err := authFailed(resp); err != nil {
		return err
	}
	var env envelope
	err := json.NewDecoder(io.LimitReader(resp.Body, bodyCap)).Decode(&env)
	switch {
	case err != nil && resp.StatusCode/100 != 2:
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	case err != nil:
		return fmt.Errorf("unreadable answer: %w", err)
	case env.Status != "success":
		return &apiError{kind: cmp.Or(env.ErrorType, "error"), msg: clip(env.Error, errorCap)}
	}
	return json.Unmarshal(env.Data, out)
}

// decodeBare reads an answer that is the data itself into out.
func decodeBare(resp *http.Response, out any) error {
	if err := authFailed(resp); err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, errorCap))
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, clip(firstLine(strings.TrimSpace(string(b))), errorCap))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, bodyCap)).Decode(out); err != nil {
		return fmt.Errorf("unreadable answer: %w", err)
	}
	return nil
}

// apiError is the server refusing a request, with its kind of error and what it said.
type apiError struct{ kind, msg string }

func (e *apiError) Error() string { return e.kind + ": " + e.msg }

// authError is a server refusing the credentials; it holds nothing the server said.
type authError struct{ code int }

func (e *authError) Error() string { return fmt.Sprintf("authentication failed (HTTP %d)", e.code) }

func authFailed(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return &authError{resp.StatusCode}
	}
	return nil
}

// redact removes the credentials from err's message, in case something echoed them.
func (c *client) redact(err error) error {
	if c.header == "" {
		return err
	}
	_, cred, _ := strings.Cut(c.header, " ")
	msg := strings.NewReplacer(c.header, "[secret]", cred, "[secret]").Replace(err.Error())
	if basic, decErr := base64.StdEncoding.DecodeString(cred); decErr == nil {
		if _, pass, ok := strings.Cut(string(basic), ":"); ok && pass != "" {
			msg = strings.ReplaceAll(msg, pass, "[secret]")
		}
	}
	return &redacted{msg, err}
}

// redacted is an error whose message has had the credentials removed; what it wraps has not.
type redacted struct {
	msg string
	err error
}

func (e *redacted) Error() string { return e.msg }
func (e *redacted) Unwrap() error { return e.err }

// clip shortens s to at most n bytes without splitting a character.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
