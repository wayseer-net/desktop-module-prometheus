package prometheus

import (
	"cmp"
	"errors"
	"fmt"
	"mindseye/internal/model"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type options struct {
	endpoint     `yaml:",inline"`
	Timeout      time.Duration     `yaml:"timeout"`      // longest wait for one request
	Interval     time.Duration     `yaml:"interval"`     // how often targets are read
	Kinds        map[string]string `yaml:"kinds"`        // job name to entity kind; others are services
	Alertmanager *endpoint         `yaml:"alertmanager"` // optional, for alerts

	kinds map[string]model.Kind
}

// endpoint is a server and how to authenticate to it.
type endpoint struct {
	URL        string `yaml:"url"`         // e.g. http://localhost:9090
	Auth       string `yaml:"auth"`        // none, basic or bearer
	Username   string `yaml:"username"`    // for basic
	SecretFile string `yaml:"secret_file"` // file holding the password or token
	SecretEnv  string `yaml:"secret_env"`  // or the environment variable holding it

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
		Timeout:  10 * time.Second, Interval: 30 * time.Second, Kinds: map[string]string{"node": string(model.KindHost), "node-exporter": string(model.KindHost)},
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
	o.kinds = map[string]model.Kind{}
	for job, k := range o.Kinds {
		if err := model.Kind(k).Validate(); err != nil {
			return fmt.Errorf("kinds: job %q: %w", job, err)
		}
		o.kinds[job] = model.Kind(k)
	}
	return nil
}

// secret is a password or token; it formats as a placeholder so it cannot be logged by mistake.
type secret string

func (secret) String() string   { return "[secret]" }
func (secret) GoString() string { return "[secret]" }

// readSecret loads the secret named by the options; errors name where it was sought, never it.
func (o *endpoint) readSecret() (secret, error) {
	var s string
	switch {
	case o.SecretEnv != "":
		v, ok := os.LookupEnv(o.SecretEnv)
		if !ok {
			return "", fmt.Errorf("secret_env: %s is not set", o.SecretEnv)
		}
		s = v
	case o.SecretFile != "":
		b, err := os.ReadFile(expandHome(o.SecretFile))
		if err != nil {
			return "", fmt.Errorf("secret_file: %w", pathOnly(err))
		}
		s = string(b)
	default:
		return "", nil
	}
	if s = strings.TrimSpace(s); s == "" {
		return "", errors.New("the secret is empty")
	}
	return secret(s), nil
}

// pathOnly keeps a file error's operation, path and cause, which hold nothing read from the file.
func pathOnly(err error) error {
	var pe *os.PathError
	if errors.As(err, &pe) {
		return pe
	}
	return errors.New("cannot read it")
}

func expandHome(p string) string {
	if rest, ok := strings.CutPrefix(p, "~/"); ok {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, rest)
		}
	}
	return p
}
