## Why

Planner and deparser already emit placeholders, but execution does not fully bind parameters and costing remains mostly static defaults. This causes plan quality issues and makes prepared/parameterized query behavior fragile.

## What Changes

- Complete end-to-end parameter propagation from deparse to execution (`fdw_exprs`, bind values, prepared statement execution).
- Add robust parameter support for base, join, and upper relation pushdown paths.
- Implement remote cost estimation when `use_remote_estimate` is enabled.
- Preserve deterministic fallback cost behavior when remote estimate is disabled/unavailable.
- Add regression tests for prepared statements and cost-sensitive plans.

## Capabilities

### New Capabilities
- `parameterized-remote-query-execution`: Execute pushdown queries with correctly bound runtime parameters.
- `remote-cost-estimation`: Use optional remote statistics/estimation to improve planner decisions.

### Modified Capabilities
- None.

## Impact

- Affected code: `duckdb_fdw.c`, `deparse.c`, planner/cost helpers, tests.
- Query planning behavior may change due to better costing.
- No SQL API break; option `use_remote_estimate` becomes effective.
