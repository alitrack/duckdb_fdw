## Context

`duckdb_fdw` currently caches connections in `CacheMemoryContext` keyed by server OID and never closes them. That means `ALTER SERVER ... OPTIONS (SET database ...)` can leave the backend talking to the old DuckDB database indefinitely. In parallel, the code has a costing helper stub that is never used, while actual foreign paths still get constant startup/total costs.

The same file also carries Direct Modify helper functions that are not registered in the FDW routine, while the visible write behavior remains `UPDATE not supported` and `DELETE not supported`.

## Goals / Non-Goals

**Goals:**
- Ensure cached DuckDB connections are closed at transaction end so changed server options take effect predictably.
- Route foreign path creation through a shared cost helper for base, join, and upper paths.
- Remove obviously dead Direct Modify code without changing the current user-facing unsupported-write behavior.
- Add regression coverage for connection refresh after `ALTER SERVER` and for unsupported update/delete operations.

**Non-Goals:**
- Implement remote transaction semantics comparable to `postgres_fdw`.
- Add streaming read APIs or a full Arrow C Data path.
- Implement `UPDATE` or `DELETE` support.

## Decisions

1. Use a single Xact callback to clean the connection cache.
   - Register `RegisterXactCallback()` once and close all cached DuckDB handles on commit, abort, and parallel variants.
   - Rationale: this is smaller and safer than trying to emulate remote transaction nesting with the current codebase.

2. Keep cost estimation intentionally simple but data-driven.
   - Use `rows`, `width`, and PostgreSQL CPU cost parameters to compute startup/total costs in one helper.
   - Wire that helper into base, join, and upper path creation.
   - Rationale: it is materially better than fixed constants and satisfies the existing spec without introducing an overfit model.

3. Delete unregistered Direct Modify scaffolding.
   - Preserve `ExecForeignUpdate`/`ExecForeignDelete` as explicit unsupported paths.
   - Remove `duckdbPlanDirectModify`, `duckdbBeginDirectModify`, `duckdbIterateDirectModify`, and `duckdbEndDirectModify` because they are dead code today.
   - Rationale: unsupported behavior should be explicit and minimal, not split across reachable and unreachable code.

## Risks / Trade-offs

- [Risk] Closing all cached connections at transaction end may reduce reuse in long-lived sessions.
  -> Mitigation: correctness and option-refresh predictability are more important than a cache that currently has no safe lifecycle.

- [Risk] The new cost model is still approximate.
  -> Mitigation: keep the implementation centralized so it can be tuned later without another wiring pass.

- [Risk] Regression coverage still depends on a working PostgreSQL build/runtime environment.
  -> Mitigation: keep SQL regression files updated now and re-run them when environment prerequisites are available.
