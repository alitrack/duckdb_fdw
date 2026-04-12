# admin-execution-surface Specification

## Purpose
TBD - created by archiving change phase1-safety-and-capability-alignment. Update Purpose after archive.
## Requirements
### Requirement: Admin-like helper functions SHALL be restricted by default
Helper functions that execute arbitrary DuckDB SQL or create DuckDB-managed secrets MUST NOT grant `EXECUTE` to `PUBLIC` by default on fresh install.

#### Scenario: Unprivileged role checks helper privilege
- **WHEN** a new non-owner PostgreSQL role inspects function privileges after `CREATE EXTENSION duckdb_fdw`
- **THEN** `duckdb_execute` and `duckdb_create_s3_secret` are not executable without an explicit `GRANT EXECUTE`

### Requirement: Upgrade path SHALL preserve restricted helper defaults
Extension upgrades to the hardened version MUST leave admin-like helper functions non-executable by `PUBLIC`.

#### Scenario: Existing installation upgrades to hardened version
- **WHEN** `ALTER EXTENSION duckdb_fdw UPDATE` installs the hardened migration
- **THEN** upgraded helper functions do not retain implicit `PUBLIC` execute access

