## Why

The first hardening slice closed the most obvious execution-surface risks, but the runtime still keeps DuckDB connections alive with no transaction-bound cleanup and the planner still uses placeholder foreign path costs. Those gaps make server option changes sticky across transactions, leave lifecycle behavior hard to reason about, and keep query planning less trustworthy than it needs to be.

## What Changes

- Add transaction-scoped cleanup for cached DuckDB connections so server option changes take effect on subsequent transactions.
- Replace fixed foreign path costs with a shared costing helper and wire it into base, join, and upper foreign paths.
- Remove or simplify unregistered dead Direct Modify code paths while preserving the current user-visible `UPDATE`/`DELETE not supported` behavior.
- Add regression coverage for server option refresh behavior and unsupported write operations.

## Capabilities

### New Capabilities
- `transaction-scoped-connection-cache`: cached DuckDB connections are cleaned up deterministically at transaction end so changed server options take effect in the next transaction.

### Modified Capabilities
- `remote-cost-estimation`: remote row estimates and width data must feed actual foreign path costing instead of fixed placeholder costs.

## Impact

- Affected code: `connection.c`, `duckdb_fdw.c`, regression SQL/output.
- Affected behavior: altered server `database` options take effect after the current transaction ends instead of reusing a stale cached connection.
- Planner impact: foreign path costs become data-driven rather than fixed constants.
