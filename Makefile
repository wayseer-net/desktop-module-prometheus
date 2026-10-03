# Thin wrappers over go commands and scripts/; every target works without make too.
# The linter is built outside any workspace, which -modfile needs, and lints in whichever is set.
LINT = $(shell GOWORK=off go tool -n -modfile=tools/go.mod golangci-lint)
.DEFAULT_GOAL := help

.PHONY: help test lint fmt check

help: ## List targets
	@awk 'BEGIN{FS=":.*## "} /^[a-z0-9-]+:.*## /{printf "  %-8s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

test: ## Run the tests, the conformance suite among them
	go test -count=1 ./...

lint: ## Run golangci-lint (pinned in tools/go.mod)
	$(LINT) run ./...

fmt: ## Format code with gofumpt via golangci-lint
	$(LINT) fmt ./...

check: ## Test, vet and lint for every platform, scan for keys (what CI runs)
	scripts/check.sh
