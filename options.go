package prometheus

import (
	"cmp"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
	"wayseer/pkg/sdk"
)

type options struct {
	endpoint     `yaml:",inline"`
	Timeout      time.Duration     `yaml:"timeout"`      // longest wait for one request, 100ms to 5m; default 10s
	Interval     time.Duration     `yaml:"interval"`     // how often targets are read, 1s to 1h; default 30s
	Kinds        map[string]string `yaml:"kinds"`        // job name to entity kind; other jobs' targets are services; default node and node-exporter are host
	Alertmanager *endpoint         `yaml:"alertmanager"` // an Alertmanager whose firing alerts show on their entities, with its own credentials
	Flows        []flowQuery       `yaml:"flows"`        // queries whose answers are traffic between entities in the world
	MaxMade      int               `yaml:"max_made"`     // most entities flow ends with make may make, 1 to 10000; default 1000
	Series       []seriesQuery     `yaml:"series"`       // queries whose answers are a metric of entities in the world

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

// flowQuery turns each series a query answers into traffic from the entity one label names to
// the entity another names.
type flowQuery struct {
	Query string          `yaml:"query"` // PromQL giving a rate per second for each source and destination; required
	From  flowEnd         `yaml:"from"`  // where traffic comes from; required
	To    flowEnd         `yaml:"to"`    // where traffic goes; required
	Unit  sdk.TrafficUnit `yaml:"unit"`  // what the rate counts: requests, bytes or messages; required
}

// flowEnd is the label naming one end of a flow, and the kind of entity it names.
type flowEnd struct {
	Label string   `yaml:"label"` // the label whose value names the entity; required
	Kind  sdk.Kind `yaml:"kind"`  // the entity's kind; default service
	Make  bool     `yaml:"make"`  // make an entity of kind for a value no module found; needs kind
}

// seriesQuery turns each series a query answers into a metric of the entity its labels name.
type seriesQuery struct {
	Query       string       `yaml:"query"`       // PromQL giving a series for each entity; required
	Metric      string       `yaml:"metric"`      // the metric's name, such as queue.depth; required
	Unit        sdk.Unit     `yaml:"unit"`        // its unit, such as count, bytes or percent; default none, a plain number
	Description string       `yaml:"description"` // what it measures, shown with it
	Entity      seriesEntity `yaml:"entity"`      // the entity each series is of; required
}

// seriesEntity is the labels whose values, joined by "/", name an entity, and its kind.
type seriesEntity struct {
	Labels []string `yaml:"labels"` // such as [namespace, pod]; the last alone is tried too; required
	Kind   sdk.Kind `yaml:"kind"`   // the entity's kind; default service
}

// labelName is what Prometheus allows a label to be called.
var labelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

const (
	authNone   = "none"
	authBasic  = "basic"
	authBearer = "bearer"
)

func defaults() options {
	return options{
		endpoint: endpoint{URL: "http://localhost:9090", Auth: authNone},
		Timeout:  10 * time.Second, Interval: 30 * time.Second, MaxMade: 1000, Kinds: map[string]string{"node": string(sdk.KindHost), "node-exporter": string(sdk.KindHost)},
	}
}

func (o *options) validate() error {
	switch {
	case o.Timeout < 100*time.Millisecond || o.Timeout > 5*time.Minute:
		return fmt.Errorf("timeout %v must be between 100ms and 5m", o.Timeout)
	case o.Interval < time.Second || o.Interval > time.Hour:
		return fmt.Errorf("interval %v must be between 1s and 1h", o.Interval)
	case o.MaxMade < 1 || o.MaxMade > 10000:
		return fmt.Errorf("max_made %d must be between 1 and 10000", o.MaxMade)
	}
	return errors.Join(o.endpoint.validate(), o.parseKinds(), o.checkAlertmanager(), o.checkFlows(), o.checkSeries())
}

func (o *options) checkFlows() error {
	var errs []error
	for i := range o.Flows {
		f := &o.Flows[i]
		if err := f.validate(); err != nil {
			errs = append(errs, fmt.Errorf("flows[%d]: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (f *flowQuery) validate() error {
	var errs []error
	if strings.TrimSpace(f.Query) == "" {
		errs = append(errs, errors.New("needs a query"))
	}
	if f.Unit == sdk.TrafficNone {
		errs = append(errs, errors.New("needs a unit: requests, bytes or messages"))
	}
	return errors.Join(append(errs, f.From.validate("from"), f.To.validate("to"))...)
}

// validate checks the end, defaulting its kind to service unless it makes entities.
func (e *flowEnd) validate(end string) error {
	if e.Make && e.Kind == "" {
		return fmt.Errorf("%s.make needs a kind, the kind of entity it makes", end)
	}
	e.Kind = cmp.Or(e.Kind, sdk.KindService)
	if !labelName.MatchString(e.Label) {
		return fmt.Errorf("%s.label %q is not a label name", end, e.Label)
	}
	if err := e.Kind.Validate(); err != nil {
		return fmt.Errorf("%s.kind: %w", end, err)
	}
	return nil
}

// metricName is what a series' metric may be called.
var metricName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.:]*$`)

func (o *options) checkSeries() error {
	var errs []error
	seen := map[string]bool{}
	for i := range o.Series {
		s := &o.Series[i]
		s.Entity.Kind = cmp.Or(s.Entity.Kind, sdk.KindService)
		err := s.validate()
		if seen[s.Metric] {
			err = errors.Join(err, fmt.Errorf("metric %q is named twice", s.Metric))
		}
		seen[s.Metric] = true
		if err != nil {
			errs = append(errs, fmt.Errorf("series[%d]: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

func (s *seriesQuery) validate() error {
	var errs []error
	if strings.TrimSpace(s.Query) == "" {
		errs = append(errs, errors.New("needs a query"))
	}
	if !metricName.MatchString(s.Metric) {
		errs = append(errs, fmt.Errorf("metric %q is not a metric name", s.Metric))
	}
	if err := s.Unit.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("unit: %w", err))
	}
	if len(s.Entity.Labels) == 0 {
		errs = append(errs, errors.New("entity.labels needs a label"))
	}
	for _, l := range s.Entity.Labels {
		if !labelName.MatchString(l) {
			errs = append(errs, fmt.Errorf("entity.labels: %q is not a label name", l))
		}
	}
	if err := s.Entity.Kind.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("entity.kind: %w", err))
	}
	return errors.Join(errs...)
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
		return errors.New("url must not hold credentials; use auth with secret_file, secret_env or secret_keyring")
	case u.RawQuery != "" || u.Fragment != "":
		return fmt.Errorf("url %q must not have a query or fragment", o.URL)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	o.base = u
	return nil
}

func (o *endpoint) checkAuth() error {
	if err := o.Validate(); err != nil {
		return err
	}
	switch {
	case o.Auth == authNone && (o.Set() || o.Username != ""):
		return errors.New("username and secrets need auth: basic or bearer")
	case o.Auth == authNone:
		return nil
	case o.Auth != authBasic && o.Auth != authBearer:
		return fmt.Errorf("auth %q is not one of none, basic, bearer", o.Auth)
	case !o.Set():
		return fmt.Errorf("auth %s needs one of secret_file, secret_env or secret_keyring", o.Auth)
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
