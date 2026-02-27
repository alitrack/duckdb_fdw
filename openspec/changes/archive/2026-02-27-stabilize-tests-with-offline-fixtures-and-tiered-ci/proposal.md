## Why

Current test coverage relies heavily on networked examples and optional cloud credentials, which makes CI and local validation non-deterministic. This prevents reliable release gating.

## What Changes

- Split tests into deterministic offline core suite and optional online/integration suites.
- Add stable local fixtures for pushdown, type mapping, import, and DML behavior.
- Rework CI to always run offline core matrix and gate online tests behind explicit conditions.
- Add clear pass/fail quality gates tied to required suites.

## Capabilities

### New Capabilities
- `deterministic-offline-test-suite`: Core functionality validation without external network or cloud credentials.
- `tiered-ci-pipeline`: CI layers that separate required core checks from optional integration checks.

### Modified Capabilities
- None.

## Impact

- Affected files: `run_tests.sh`, SQL test assets, GitHub workflow, docs.
- CI reliability and developer feedback loops improve.
- Cloud scenario tests remain available but no longer block core correctness validation.
