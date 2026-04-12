## ADDED Requirements

### Requirement: Helper SQL command execution SHALL clean up DuckDB results on PostgreSQL error paths
Helper execution paths that call DuckDB SQL APIs MUST destroy DuckDB result objects before returning, including when PostgreSQL raises through `ereport(ERROR)`.

#### Scenario: Helper execution raises a PostgreSQL error
- **WHEN** a DuckDB helper command fails and the FDW surfaces the failure through PostgreSQL error handling
- **THEN** the associated DuckDB result is destroyed before control unwinds out of the helper

### Requirement: Planner-side remote estimate cleanup SHALL be deterministic
Planner-side remote estimate probes MUST destroy their DuckDB result objects regardless of success or failure.

#### Scenario: Remote estimate probe fails
- **WHEN** `use_remote_estimate` triggers a failing probe during planning
- **THEN** planning falls back safely and the intermediate DuckDB result object is not leaked
