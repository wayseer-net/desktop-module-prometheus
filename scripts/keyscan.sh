#!/usr/bin/env bash
# Fails if any commit holds a key or a secret; .gitleaksignore lists the tests' dummy values.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> keys and secrets"
go run github.com/zricethezav/gitleaks/v8@v8.30.1 git --no-banner --redact --log-level warn .
if git grep -nE 'PRIVATE KEY|WAYSEER-1\.|ws-dev-' -- ':!scripts/keyscan.sh'; then
	echo "keyscan: a key marker is in the tree" >&2
	exit 1
fi
