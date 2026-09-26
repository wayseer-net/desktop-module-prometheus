// Package prometheus is the `prometheus` module: a Prometheus server's active scrape targets as
// entities, grouped by job, on the hosts named by their instance labels, with series from
// query_range. A target is up, down (with its scrape error) or not yet scraped; a job warns
// when some of its targets are down. Targets of jobs mapped to hosts are the hosts themselves,
// and they also answer the shared host metrics (cpu.utilisation, memory.utilisation, disk and
// network rates) from node_exporter's families. Every gauge and counter family the server
// describes is in the catalogue by its own name; counters are queried as per-second rates.
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
//	      secret_file: ""             # file holding the password or token, e.g. ~/.config/mindseye/prom-token
//	      secret_env: ""              # or the environment variable holding it
//	      kinds: {node: host, node-exporter: host}  # job name to entity kind; other jobs' targets are services
//
// The secret is read once at configuration and sent only as the Authorization header; it never
// appears in logs, errors or entities, and fixtures recorded with promtest leave headers out.
package prometheus
