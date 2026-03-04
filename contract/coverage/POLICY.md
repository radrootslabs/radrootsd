# radrootsd coverage policy

this policy defines the required rust coverage gate for this repository.

## gate contract

- executable lines coverage: 100.0
- function coverage: 100.0
- branch coverage: 100.0
- region coverage: 100.0
- branch records must be present in coverage data

all thresholds are merge-blocking and release-blocking.

## toolchain contract

- use nightly rust for coverage runs
- use `cargo llvm-cov` with `--branch`
- export summary json and lcov artifacts per run
- evaluate gates from `contract/coverage/thresholds.toml`

## enforcement contract

- evaluate coverage for the repository crate, not only aggregated workspace totals
- fail hard when any required metric is below threshold, including regions
- fail hard when required branch records are missing
- fail hard when required covered files in `contract/coverage/include.txt` are absent from summary output

## local and ci contract

- local development may run report-only commands
- ci must run strict gate commands with default thresholds
