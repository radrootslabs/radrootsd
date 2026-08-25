#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

for forbidden_root in docs .github .act; do
  test ! -e "$forbidden_root"
  test ! -L "$forbidden_root"
done
test "$(cargo public-api --version)" = "cargo-public-api 0.52.0"
temporary_api="$(mktemp)"
trap 'rm -f "$temporary_api"' EXIT
cargo +nightly-2026-07-16 public-api --all-features -sss -p radrootsd >"$temporary_api"
cmp "$temporary_api" contracts/api_baselines/radrootsd.txt

if git ls-files | grep -E -i '(^|/)(\.env|id_rsa|id_ed25519|credentials|[^/]+\.(pem|key|p12|pfx|jks|keystore))$' >/dev/null; then
  echo "boundary_invalid: sensitive credential path is tracked" >&2
  exit 1
fi
if git grep -I -n -E -e '-----BEGIN ([A-Z0-9 ]+ )?PRIVATE KEY-----|AKIA[0-9A-Z]{16}|gh[pousr]_[A-Za-z0-9_]{36,}|nsec1[023456789acdefghjklmnpqrstuvwxyz]{40,}' -- src >/dev/null; then
  echo "boundary_invalid: production source contains credential material" >&2
  exit 1
fi

echo "boundary ok: root-only API, fresh baseline, no forbidden or credential surface"
