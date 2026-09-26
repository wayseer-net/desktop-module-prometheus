package prometheus

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

func newClient(e *endpoint, timeout time.Duration, rt http.RoundTripper, s secret) *client {
	c := &client{base: e.base, http: &http.Client{Transport: rt}, timeout: timeout}
	switch e.Auth {
	case authBasic:
		c.header = "Basic " + base64.StdEncoding.EncodeToString([]byte(e.Username+":"+string(s)))
	case authBearer:
		c.header = "Bearer " + string(s)
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
	return c.call(ctx, http.MethodGet, path, form, out)
}

// post calls path with form as its body, as Prometheus accepts for long queries.
func (c *client) post(ctx context.Context, path string, form url.Values, out any) error {
	return c.call(ctx, http.MethodPost, path, form, out)
}

func (c *client) call(ctx context.Context, method, path string, form url.Values, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	err := c.do(ctx, method, path, form, out)
	switch {
	case err == nil:
		return nil
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("%s: no answer within %v: %w", path, c.timeout, context.DeadlineExceeded)
	case ctx.Err() != nil:
		return ctx.Err()
	}
	return c.redact(fmt.Errorf("%s: %w", path, err))
}

func (c *client) do(ctx context.Context, method, path string, form url.Values, out any) error {
	u := *c.base
	u.Path += path
	var body io.Reader
	if method == http.MethodGet {
		u.RawQuery = form.Encode()
	} else {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
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
		return fmt.Errorf("%s: %s", cmp.Or(env.ErrorType, "error"), clip(env.Error, errorCap))
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
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, clip(strings.TrimSpace(string(b)), errorCap))
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, bodyCap)).Decode(out); err != nil {
		return fmt.Errorf("unreadable answer: %w", err)
	}
	return nil
}

func authFailed(resp *http.Response) error {
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("authentication failed (HTTP %d)", resp.StatusCode)
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
	return errors.New(msg)
}

// clip shortens s to at most n bytes without splitting a character.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
