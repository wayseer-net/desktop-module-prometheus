#!/usr/bin/env bash
# What CI runs: test (the conformance suite among them), vet and lint as every platform sees the
# code, and a scan for keys. Needs only Go; gitleaks runs at a pinned version through go run.
set -euo pipefail
cd "$(dirname "$0")/.."

lint=$(GOWORK=off go tool -n -modfile=tools/go.mod golangci-lint) # built for this machine, whatever GOOS says
echo "==> go test"
go test -count=1 ./...
for goos in linux darwin windows; do
	echo "==> go vet, lint ($goos)"
	GOOS=$goos go vet ./...
	GOOS=$goos "$lint" run ./...
done
scripts/keyscan.sh
echo "==> ok"
