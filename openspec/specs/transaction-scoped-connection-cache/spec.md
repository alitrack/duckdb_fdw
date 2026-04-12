# transaction-scoped-connection-cache Specification

## Purpose
TBD - created by archiving change runtime-and-planner-hardening. Update Purpose after archive.
## Requirements
### Requirement: Cached DuckDB connections SHALL be reset at transaction end
Connections opened through the FDW cache MUST be closed when the current PostgreSQL transaction ends so that subsequent transactions observe updated server options.

#### Scenario: ALTER SERVER changes database target
- **WHEN** a transaction changes a server `database` option and a later transaction reuses that server
- **THEN** the FDW opens a fresh DuckDB connection using the new option value instead of reusing the previous database handle

