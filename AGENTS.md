# radrootsd — agent specification

## Scope and authority

- This file applies to the complete standalone `radrootsd` repository. A
  closer `AGENTS.md` overrides it only for that subtree.
- This repository owns the public `radrootsd` daemon. Keep it cloneable,
  inspectable, buildable, testable, and operable from its checked-in source and
  public dependency surface.
- Read `README`, `Cargo.toml`, `radroots.lib.source-lock.v1.toml`, the relevant
  implementation, and nearby tests before changing behavior. Treat checked-in
  manifests, lockfiles, source locks, tests, and public protocol behavior as
  implementation authority; do not invent private or parent-repository
  requirements.
- Preserve exact public Git dependency revisions and source-lock agreement.
  Never replace them with sibling paths, unpublished artifacts, private
  repositories, or ambient monorepo state.

## Repository boundaries

- Keep daemon lifecycle, configuration, path resolution, identity storage,
  service state, domain policy, and network transports explicit and separate.
- `src/app/**` owns process-facing CLI, configuration, paths, identity-storage,
  and runtime composition. `src/core/**` owns daemon state and protocol policy.
  `src/host_nostr.rs` owns the private upstream Nostr client edge, while
  `src/transport/**` owns typed daemon transport protocols and JSON-RPC/Nostr
  ingress and egress. Do not move policy into transport glue or process
  behavior into reusable core modules.
- Preserve typed JSON-RPC and NIP-46 boundaries. Public Nostr behavior must
  remain protocol-interoperable, and signed events must retain their author,
  canonical event-id, and signature verification guarantees.
- Do not make this repository responsible for platform-wide release contracts,
  builder selection, publication, promotion, deployment transport, or private
  dependency coordination.
- `.github/**` and capsule-local CI workflows are forbidden; keep validation
  forge-agnostic, and place any required monorepo orchestration exclusively
  under the parent monorepo's root `.act/**` authority.
- Do not add or retain tracked `docs/**` or `.act/**`. Keep standalone
  contributor and operator guidance in `README` or `AGENTS.md`, and keep
  machine authority in explicit repository-root contracts.

## Change discipline

- Prefer the smallest coherent target-state change. Do not mix unrelated
  cleanup, speculative abstractions, compatibility scaffolding, or roadmap
  work into the same checkpoint.
- Service hardening is clean-slate. Do not add or preserve prototype
  configuration readers, environment-file configuration, JSON or JSONL mutable
  service state, fallback path searches, compatibility aliases, deprecated
  APIs or re-exports, dual wire encodings, or old/new behavior switches. Update
  affected callers directly.
- Use Rust `1.97.1`, edition `2024`, and resolver `3` as declared by the
  repository. Use dependency versions and feature choices from `Cargo.toml`.
- Prefer typed models, explicit state transitions, deterministic behavior,
  narrow side effects, and precise error enums. Avoid hidden production
  panics. Avoid `unsafe`; if it is strictly necessary, document the invariant
  next to the smallest possible unsafe block.
- Keep `lib.rs` and `main.rs` thin. Put reusable behavior in focused modules,
  and inject clocks, entropy, transports, and other nondeterministic inputs
  where tests need control.
- Bound requests, responses, queues, collections, retries, and retained
  diagnostics. Make startup, shutdown, interruption, recovery, and partial
  failure deterministic and observable.

## Security and data handling

- Never expose secrets, private keys, credentials, tokens, invite codes,
  private identifiers, sensitive user data, or sensitive event content in
  source, logs, errors, status output, tests, fixtures, docs, or examples.
- Keep key material and identity state behind narrow ownership boundaries;
  zeroize sensitive buffers where the existing type contract supports it.
- Reject ambiguous or invalid configuration, paths, RPC input, event data, and
  transport responses. Do not silently fall back, broaden permissions, or
  accept partially verified signed content.

## Validation

- Use Nix as the canonical standalone environment. Run the smallest relevant
  repository-owned lane first, then broaden in proportion to the change:
  `nix run .#fmt`, `nix run .#check`, and `nix run .#test`.
- Narrow iteration may use `cargo fmt --all --check`,
  `cargo check --workspace --all-targets --locked`,
  `cargo test --workspace --locked`, and
  `cargo clippy --workspace --all-targets --locked -- -D warnings` from
  `nix develop`.
- Add deterministic tests for new behavior, failure modes, parsing, recovery,
  protocol verification, and security boundaries. Prefer stable public or
  repo-owned interfaces over implementation-detail tests.
- If a lane cannot run, report the exact command, failure, and affected
  confidence. Never claim a check passed unless it completed successfully.

## Commits and irreversible actions

- Keep commits focused and reviewable. Use
  `<scope>: <imperative summary>` unless a more specific repository convention
  is added later.
- Do not publish, push, tag, sign, deploy, rotate credentials, or mutate remote
  service or repository state without explicit authority.
