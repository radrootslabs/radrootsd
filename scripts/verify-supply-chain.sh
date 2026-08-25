#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

test "$(cargo deny --version)" = "cargo-deny 0.19.8"
test "$(cargo vet --version)" = "cargo-vet 0.10.2"
cargo vet --locked
cargo deny -L error --all-features --locked check advisories bans licenses sources
