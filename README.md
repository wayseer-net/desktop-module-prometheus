# Wayseer module: prometheus

The `prometheus` module of Wayseer Desktop: a Prometheus server's scrape targets as entities,
grouped by job on the hosts their instances name, with series from `query_range`, flows
between entities from a service graph, and, with an Alertmanager, alerts that can be silenced.

```
go get wayseer.dev/modules/prometheus
```

Wayseer links it in, so users do not install it. Its options and what it shows are in Wayseer's
user guide, under "Prometheus".

`promtest` records a server's answers and replays them, so tests run without one. `testdata`
holds recordings of a lab server and an Alertmanager, with their hostnames scrubbed.
`scripts/recordalerts` re-records the Alertmanager's from a throwaway one; read its comment
first.

## Working on it

```
make check   # what CI runs: tests with the conformance suite, vet and lint for every platform, a key scan
make help    # every target
```

It imports only the SDK (`wayseer.dev/sdk`), the standard library and its own
dependencies; `TestImportsOnlyTheSDK` keeps it that way. golangci-lint is pinned in
`tools/go.mod`, and gitleaks runs at a pinned version through `go run`.

To change it alongside the SDK or the app, use a Go workspace: `go.work` here with
`use . ../../sdk`, or the app's `make workspace`, which writes one for the whole of Wayseer
Desktop. `go.work` is ignored by git.

Until `wayseer.dev` serves the SDK's page and its repository is public, fetching it needs
`GOPRIVATE=github.com/wayseer-net/*` and git access to GitHub over HTTPS.

## Licence

MIT; see `LICENSE`.
