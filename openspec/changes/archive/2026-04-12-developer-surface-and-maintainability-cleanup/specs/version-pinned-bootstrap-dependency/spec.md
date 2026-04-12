## ADDED Requirements

### Requirement: DuckDB bootstrap SHALL resolve a deterministic default version
The bootstrap script for downloading DuckDB client libraries MUST use a pinned default version rather than the latest upstream release.

#### Scenario: Default bootstrap run
- **WHEN** a developer runs the bootstrap script without extra environment variables
- **THEN** the script downloads the repository's pinned default DuckDB version

### Requirement: Bootstrap version SHALL remain explicitly overridable
The bootstrap script MUST support an explicit override for maintainers who need to test another DuckDB release.

#### Scenario: Maintainer tests alternate DuckDB version
- **WHEN** `DUCKDB_VERSION` is set explicitly
- **THEN** the bootstrap script downloads that requested version instead of the default pin
