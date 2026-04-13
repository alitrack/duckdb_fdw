# pg-duckdb-runtime-coexistence Specification

## Purpose
Define the supported coexistence contract between `duckdb_fdw` and `pg_duckdb` inside the same PostgreSQL backend, including runtime blocking, unsupported override handling, install-time preflight, and operator diagnostics.

## Requirements

### Requirement: duckdb_fdw SHALL detect peer-loaded pg_duckdb inside the current backend

The runtime guard MUST determine whether `pg_duckdb` is already loaded in the current PostgreSQL backend before `duckdb_fdw` proceeds into guarded DuckDB API entry points.

#### Scenario: Current backend does not contain pg_duckdb

- **WHEN** a guarded `duckdb_fdw` execution path runs in a backend that has not loaded `pg_duckdb`
- **THEN** the guard returns `NoPeerLoaded`
- **AND** `duckdb_fdw` continues normally

#### Scenario: Current backend has loaded pg_duckdb

- **WHEN** a guarded `duckdb_fdw` execution path runs in a backend that has already loaded `pg_duckdb`
- **THEN** the guard enters peer-loaded validation before allowing execution

### Requirement: v1 SHALL block peer-loaded coexistence unless explicitly overridden

The first implementation MUST treat peer-loaded coexistence as blocked by default unless an explicit unsupported override is enabled.

#### Scenario: Peer-loaded backend without proof of compatibility

- **WHEN** `pg_duckdb` is loaded in the current backend and `duckdb_fdw` cannot prove compatible runtime coexistence
- **THEN** `duckdb_fdw` raises an `ERROR`
- **AND** the error explains that strict coexistence policy rejected the runtime combination

#### Scenario: Peer-loaded backend with unsupported override

- **WHEN** `pg_duckdb` is loaded in the current backend and the session enables the unsupported coexistence override
- **THEN** `duckdb_fdw` allows execution to continue
- **AND** emits a `WARNING` that the session is using unsupported coexistence mode

### Requirement: Install-time preflight SHALL remain informational

Extension install and preflight flows MUST warn about possible coexistence risk without turning static discovery into the final runtime verdict.

#### Scenario: pg_duckdb is installed or available during extension setup

- **WHEN** `duckdb_fdw` install-time preflight can discover `pg_duckdb` in catalogs or available extensions
- **THEN** it emits an informational warning
- **AND** states that runtime behavior is decided by backend-local validation, not install order alone

### Requirement: Runtime diagnostics SHALL expose guard status

`duckdb_fdw` MUST provide a supported way to inspect the runtime guard outcome and local fingerprint details.

#### Scenario: Operator asks why coexistence is blocked

- **WHEN** an operator invokes the runtime diagnostics surface
- **THEN** it returns the current compatibility status
- **AND** includes the local version/source details used by the guard
