## Context

The repository currently has no active OpenSpec change, but the code and docs show a clear mismatch between intended and actual behavior. `duckdb_execute` and `duckdb_create_s3_secret` are created without any default privilege tightening in extension SQL scripts, high-risk SQL generation remains in a few call sites, and helper cleanup relies on code paths that can be bypassed by PostgreSQL error unwinds.

The first execution slice should focus on production-safety primitives that are both high impact and locally reviewable:
- Default helper privileges
- Remaining SQL-construction hardening
- DuckDB result cleanup on error
- Capability-claim alignment in public docs

## Goals / Non-Goals

**Goals:**
- Make admin-like helper functions restricted-by-default for fresh installs and upgrades.
- Remove remaining obvious string interpolation hazards in the first-batch execution surface.
- Ensure helper SQL command paths destroy DuckDB results even when `ereport(ERROR)` unwinds control flow.
- Bring README/TODO claims back in sync with the code that exists now.
- Add at least one regression check that fails before the privilege hardening lands.

**Non-Goals:**
- Implement the full Arrow C Data read path.
- Finish transaction-scoped connection lifecycle management.
- Implement a real planner cost model.
- Decide the long-term fate of dead/unregistered Direct Modify code in this change.

## Decisions

1. Restrict helper functions in extension SQL, not only in runtime code.
   - Apply `REVOKE EXECUTE ... FROM PUBLIC` in both `duckdb_fdw--2.0.0.sql` and `duckdb_fdw--1.4.1--2.0.0.sql`.
   - Rationale: the default privilege surface is created by extension DDL, so the safest place to harden it is the install/upgrade path itself.
   - Alternative considered: check caller role inside C functions.
   - Rejected because it duplicates PostgreSQL privilege semantics and still leaves metadata claiming public executability.

2. Reuse the existing quoting helpers instead of introducing a new SQL-builder abstraction in this phase.
   - Use `duckdb_fdw_quote_literal()` for `duckdb_deparse_analyze()` inputs and the derived S3 Tables endpoint string.
   - Rationale: this closes the remaining high-risk holes with small, reviewable changes.

3. Use PostgreSQL error-control primitives to protect DuckDB cleanup.
   - Wrap `ereport(level, ...)` in `PG_TRY()/PG_FINALLY()` in `duckdb_do_sql_command()`.
   - Zero-initialize result structs where cleanup may run after an error path.
   - Rationale: this directly addresses the longjmp cleanup gap without forcing the whole FDW onto a larger memory-context redesign.

4. Treat documentation alignment as part of the safety change, not a separate cleanup.
   - Update README/TODO claim language in the same change that establishes the current safety baseline.
   - Rationale: future planning and verification depend on accurate capability statements.

## Risks / Trade-offs

- [Risk] Existing users may rely on implicit execute privileges for helper functions.
  -> Mitigation: keep the functions available to owners/superusers and document that non-owner use now requires explicit `GRANT EXECUTE`.

- [Risk] This change does not solve broader connection lifecycle issues.
  -> Mitigation: record those as follow-up work and keep this change narrowly focused on the first production-safety slice.

- [Risk] Regression coverage remains partial because some unsafe paths are not easily reachable in the current offline harness.
  -> Mitigation: add privilege regression now and document the remaining unverified paths in tasks/open questions.

## Migration Plan

1. Add OpenSpec artifacts that define the first production-safety slice.
2. Add regression checks for helper-function default privileges.
3. Harden install/upgrade SQL and C execution paths.
4. Update public capability claims to match implementation reality.
5. Validate the OpenSpec change and run the locally available verification commands.

Rollback:
- SQL privilege changes can be reverted with a follow-up migration or explicit grants.
- C-level cleanup and quoting changes are localized and can be reverted independently if needed.

## Open Questions

- Should the next change remove or finish the unregistered Direct Modify path?
- Do we want a dedicated spec for transaction-bound connection lifecycle, or fold it into a broader production-hardening phase?
- High-risk follow-up items already identified for the next hardening phase:
  - transaction-scoped connection cleanup and `RegisterXactCallback`
  - planner costing that actually feeds `create_foreignscan_path`
  - deterministic runtime verification in an environment with `pg_config` or a working `postgres:17` image source
  - broader regression coverage for helper error-path cleanup beyond static inspection
