## 1. OpenSpec and planning

- [ ] 1.1 Add proposal, design, tasks, and spec delta for pg_duckdb coexistence guarding
- [ ] 1.2 Add repo-local implementation plan with file map, phased tasks, and verification commands

## 2. Runtime guard skeleton

- [ ] 2.1 Add a dedicated guard module and wire it into the build
- [ ] 2.2 Define compatibility statuses, runtime fingerprint structs, and Linux-first backend detection entry points
- [ ] 2.3 Register a session-scoped unsupported override GUC in `_PG_init`

## 3. Execution-path enforcement

- [ ] 3.1 Guard `duckdb_get_connection()`
- [ ] 3.2 Guard direct DuckDB API entry points such as `duckdb_fdw_version()`
- [ ] 3.3 Ensure blocked states surface explicit `ERROR` messages and override mode surfaces `WARNING`

## 4. Preflight and diagnostics

- [ ] 4.1 Add install-time preflight SQL surface with informational warnings only
- [ ] 4.2 Add runtime diagnostics SQL functions for fingerprint and compatibility status
- [ ] 4.3 Update documentation to describe strict coexistence policy, Linux-first scope, and unsupported override semantics

## 5. Verification

- [ ] 5.1 Add focused tests or diagnostic verification for `NoPeerLoaded`, blocked peer-loaded states, and override behavior
- [ ] 5.2 Add a Linux-first integration lane or scripted validation flow for runtime detection
- [ ] 5.3 Keep true `pg_duckdb` integration outside default `installcheck`
