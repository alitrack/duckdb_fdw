## ADDED Requirements

### Requirement: Inserts SHALL use DuckDB Appender for supported types
Foreign inserts MUST use Appender APIs to append typed values for supported PostgreSQL column types.

#### Scenario: Single-row insert through appender
- **WHEN** PostgreSQL executes INSERT into a foreign table
- **THEN** FDW appends values via Appender and persists the row in DuckDB

### Requirement: Batch insert hooks SHALL be implemented for PG14+
FDW MUST implement batch insert interfaces to process grouped rows efficiently for COPY/INSERT SELECT workloads.

#### Scenario: COPY/INSERT SELECT triggers batch path
- **WHEN** PostgreSQL sends a batch insert workload
- **THEN** FDW processes rows in batches and commits through Appender with no per-row SQL string generation
