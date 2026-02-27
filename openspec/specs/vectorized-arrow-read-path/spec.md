# vectorized-arrow-read-path Specification

## Purpose
TBD - created by archiving change implement-arrow-streaming-and-batch-write-paths. Update Purpose after archive.
## Requirements
### Requirement: FDW scans SHALL consume Arrow chunks for row retrieval
The foreign scan execution path MUST use DuckDB Arrow query/chunk APIs and iterate rows from chunk buffers instead of per-row `duckdb_value_*` access.

#### Scenario: Large table scan uses chunk iteration
- **WHEN** a query scans a large foreign table
- **THEN** scan state advances through Arrow chunks and emits rows without calling row-wise value APIs

### Requirement: Arrow conversion SHALL preserve PostgreSQL type correctness
Vectorized conversion MUST produce PostgreSQL values equivalent to legacy semantics for supported scalar types.

#### Scenario: Scalar type parity
- **WHEN** scan returns BOOL, INT, FLOAT, DATE, TIMESTAMP, UUID, and DECIMAL columns
- **THEN** returned PostgreSQL rows match expected values and null behavior

