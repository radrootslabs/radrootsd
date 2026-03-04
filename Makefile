COVERAGE_OUTPUT_DIR := target/coverage
COVERAGE_SUMMARY := $(COVERAGE_OUTPUT_DIR)/summary.json
COVERAGE_LCOV := $(COVERAGE_OUTPUT_DIR)/lcov.info
COVERAGE_THRESHOLDS := contract/coverage/thresholds.toml
COVERAGE_INCLUDE := contract/coverage/include.txt

.PHONY: coverage-report coverage-gate

coverage-report:
	mkdir -p $(COVERAGE_OUTPUT_DIR)
	cargo +nightly llvm-cov clean --workspace
	cargo +nightly llvm-cov --workspace --all-features --branch --no-report
	cargo +nightly llvm-cov report --json --summary-only --output-path $(COVERAGE_SUMMARY)
	cargo +nightly llvm-cov report --lcov --output-path $(COVERAGE_LCOV)
	cargo +nightly llvm-cov report --summary-only
	@echo "coverage summary: $(COVERAGE_SUMMARY)"
	@echo "coverage lcov: $(COVERAGE_LCOV)"

coverage-gate: coverage-report
	python3 scripts/ci/verify_coverage.py --thresholds $(COVERAGE_THRESHOLDS) --summary $(COVERAGE_SUMMARY) --lcov $(COVERAGE_LCOV) --include $(COVERAGE_INCLUDE)
