package prometheus

import (
	"cmp"
	"errors"
	"fmt"
	"mindseye/pkg/sdk"
	"net/url"
	"strings"
	"time"
)

type options struct {
	endpoint     `yaml:",inline"`
	Timeout      time.Duration     `yaml:"timeout"`      // longest wait for one request, 100ms to 5m; default 10s
	Interval     time.Duration     `yaml:"interval"`     // how often targets are read, 1s to 1h; default 30s
	Kinds        map[string]string `yaml:"kinds"`        // job name to entity kind; other jobs' targets are services; default node and node-exporter are host
	Alertmanager *endpoint         `yaml:"alertmanager"` // an Alertmanager whose firing alerts show on their entities, with its own credentials

	kinds map[string]sdk.Kind
}

// endpoint is a server and how to authenticate to it.
type endpoint struct {
	URL               string           `yaml:"url"`      // the server's address, http:// or https://
	Auth              string           `yaml:"auth"`     // how to authenticate: none, basic or bearer; default none
	Username          string           `yaml:"username"` // the user, for basic
	sdk.SecretOptions `yaml:",inline"` // the password or token

	base *url.URL
}

const (
	authNone   = "none"
	authBasic  = "basic"
	authBearer = "bearer"
)

func defaults() options {
	return options{
		endpoint: endpoint{URL: "http://localhost:9090", Auth: authNone},
		Timeout:  10 * time.Second, Interval: 30 * time.Second, Kinds: map[string]string{"node": string(sdk.KindHost), "node-exporter": string(sdk.KindHost)},
	}
}

func (o *options) validate() error {
	switch {
	case o.Timeout < 100*time.Millisecond || o.Timeout > 5*time.Minute:
		return fmt.Errorf("timeout %v must be between 100ms and 5m", o.Timeout)
	case o.Interval < time.Second || o.Interval > time.Hour:
		return fmt.Errorf("interval %v must be between 1s and 1h", o.Interval)
	}
	return errors.Join(o.endpoint.validate(), o.parseKinds(), o.checkAlertmanager())
}

func (o *options) checkAlertmanager() error {
	if o.Alertmanager == nil {
		return nil
	}
	o.Alertmanager.Auth = cmp.Or(o.Alertmanager.Auth, authNone)
	if err := o.Alertmanager.validate(); err != nil {
		return fmt.Errorf("alertmanager: %w", err)
	}
	return nil
}

func (o *endpoint) validate() error { return errors.Join(o.parseURL(), o.checkAuth()) }

func (o *endpoint) parseURL() error {
	u, err := url.Parse(o.URL)
	switch {
	case err != nil:
		return fmt.Errorf("url: %w", err)
	case u.Scheme != "http" && u.Scheme != "https" || u.Host == "":
		return fmt.Errorf("url %q must be http:// or https:// with a host", o.URL)
	case u.User != nil:
		return errors.New("url must not hold credentials; use auth with secret_file or secret_env")
	case u.RawQuery != "" || u.Fragment != "":
		return fmt.Errorf("url %q must not have a query or fragment", o.URL)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	o.base = u
	return nil
}

func (o *endpoint) checkAuth() error {
	secrets := 0
	for _, s := range []string{o.SecretFile, o.SecretEnv} {
		if s != "" {
			secrets++
		}
	}
	switch {
	case o.Auth == authNone && (secrets > 0 || o.Username != ""):
		return errors.New("username, secret_file and secret_env need auth: basic or bearer")
	case o.Auth == authNone:
		return nil
	case o.Auth != authBasic && o.Auth != authBearer:
		return fmt.Errorf("auth %q is not one of none, basic, bearer", o.Auth)
	case secrets != 1:
		return fmt.Errorf("auth %s needs one of secret_file or secret_env", o.Auth)
	case o.Auth == authBasic && o.Username == "":
		return errors.New("auth basic needs a username")
	case o.Auth == authBearer && o.Username != "":
		return errors.New("auth bearer takes no username")
	}
	return nil
}

func (o *options) parseKinds() error {
	o.kinds = map[string]sdk.Kind{}
	for job, k := range o.Kinds {
		if err := sdk.Kind(k).Validate(); err != nil {
			return fmt.Errorf("kinds: job %q: %w", job, err)
		}
		o.kinds[job] = sdk.Kind(k)
	}
	return nil
}
