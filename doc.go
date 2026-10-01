// Package prometheus is the `prometheus` module: a Prometheus server's active scrape targets as
// entities, grouped by job, on the hosts named by their instance labels, with series from
// query_range. A target is up, down (with its scrape error) or not yet scraped; a job warns
// when some of its targets are down. Targets of jobs mapped to hosts are the hosts themselves,
// and they also answer the shared host metrics (cpu.utilisation, memory.utilisation, disk and
// network rates) from node_exporter's families. Every gauge and counter family the server
// describes is in the catalogue by its own name; counters are queried as per-second rates.
//
// With an Alertmanager, each alert is an `alert` entity, a member of the target its job and
// instance labels name, else the host its instance names, else its job, else the server. A critical alert raises the entity to
// Crit and any other but info or none to Warn, never lowering it, with the alert names as the
// reason and in the `alerts` attribute. Silenced or inhibited alerts are listed in
// `alerts_muted` and leave the status alone. Firing, muting and resolving are alert events. If
// Alertmanager cannot be read, the last alerts stand and Health notes why. The owner may allow
// `silence`, matching an alert's labels exactly for 15m to 7d, and `unsilence`, which expires
// only the silences this instance made (createdBy its name, comment "Wayseer").
//
//	modules:
//	  - kind: prometheus
//	    name: prom
//	    options:
//	      url: http://localhost:9090  # the server; no credentials in the URL
//	      timeout: 10s                # longest wait for one request
//	      interval: 30s               # how often targets are read
//	      auth: none                  # none, basic or bearer
//	      username: ""                # for basic
//	      secret_file: ""             # file holding the password or token, e.g. ~/.config/wayseer/prom-token
//	      secret_env: ""              # or the environment variable holding it
//	      secret_keyring: ""          # or the keyring entry holding it, <service>/<account>
//	      kinds: {node: host, node-exporter: host}  # job name to entity kind; other jobs' targets are services
//	      alertmanager:               # optional
//	        url: http://localhost:9093
//	        auth: none                # with its own username and secret
//
// The secret is read once at configuration and sent only as the Authorization header; it never
// appears in logs, errors or entities, and fixtures recorded with promtest leave headers out.
package prometheus
