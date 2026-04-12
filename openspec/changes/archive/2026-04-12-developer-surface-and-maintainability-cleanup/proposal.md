## Why

The core runtime is in much better shape after the first two hardening slices, but the repository still leaks maintenance debt into daily development: token parsing uses non-reentrant `strtok`, the DuckDB bootstrap script resolves the latest release instead of a deterministic version, contributor guidance is missing, and the changelog does not clearly describe release history or current maintenance work.

## What Changes

- Replace non-reentrant token parsing with a reusable reentrant helper path.
- Make DuckDB bootstrap deterministic by pinning a default library version while preserving an explicit override path.
- Add a contributor guide that explains build, test, verification, and environment expectations.
- Rewrite the changelog into a structured, versioned format without inventing unsupported historical release dates.
- Remove stale maintenance residue such as the unused `duckdb_optimization.c` stub from the build.

## Capabilities

### New Capabilities
- `contributor-onboarding`: contributors get a clear, repo-local guide for prerequisites, build/test commands, and verification expectations.
- `version-pinned-bootstrap-dependency`: the DuckDB bootstrap script resolves a deterministic default library version and documents explicit override behavior.

### Modified Capabilities
- None.

## Impact

- Affected code: `connection.c`, `deparse.c`, `sql_utils.c`, `duckdb_fdw.h`, `Makefile`, `download_libduckdb.sh`.
- Affected docs: `README.md`, `CHANGELOG`, new `CONTRIBUTING.md`.
- Build impact: remove one unused object file and make bootstrap downloads reproducible by default.
