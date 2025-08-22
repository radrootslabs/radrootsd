# radrootsd - code directives

- this repo defines `radrootsd`, the standalone Radroots daemon
- keep this repo cloneable, inspectable, buildable, testable, and usable from its own checked-in source and public dependency surface
- do not make this repo responsible for platform-wide release contracts, signed artifacts, builder selection, publication, promotion, deployment transport, or private dependency coordination unless represented in this repo's public contract
- prefer the smallest coherent change that fully addresses the request; do not mix unrelated cleanup, speculative refactors, compatibility scaffolding, or roadmap work into the same change
- inspect the relevant implementation, tests, manifests, and docs before changing behavior
- do not invent requirements, APIs, dependencies, release processes, or external integration behavior
- do not depend on private repositories, unpublished artifacts, local machine layouts, absolute paths, or internal monorepo context
- preserve explicit boundaries between daemon lifecycle, configuration, service state, storage, network integration, and domain behavior
- keep startup, shutdown, error handling, and recovery behavior deterministic and observable without leaking sensitive data
- prefer explicit typed models, deterministic behavior, narrow side effects, and direct service boundaries over stringly or implicit behavior
- avoid hidden production panics; use typed errors for expected failure modes
- avoid `unsafe` unless it is strictly necessary, locally justified, and documented with nearby invariants
- do not expose secrets, private keys, credentials, tokens, invite codes, private identifiers, sensitive user data, or sensitive event content in code, logs, tests, fixtures, docs, or examples
- prefer tests that exercise daemon behavior through stable public or repo-owned interfaces
- use checked-in, repo-owned validation first; run the smallest documented validation that credibly covers the change
- if validation cannot run, report exactly what was skipped and why; never claim validation passed unless it actually ran
- keep commits focused and reviewable, using `<scope>: <imperative summary>` unless a repo convention overrides it
