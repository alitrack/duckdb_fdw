# safe-duckdb-sql-construction Specification

## Purpose
TBD - created by archiving change harden-sql-generation-and-credential-safety. Update Purpose after archive.
## Requirements
### Requirement: Dynamic SQL inputs SHALL be escaped or validated before execution
All user-controlled values included in generated DuckDB SQL MUST be passed through approved escaping helpers (for literals/identifiers) or strict token validation before query execution.

#### Scenario: Literal payload is safely escaped
- **WHEN** a server option includes a literal payload containing single quotes and semicolons
- **THEN** generated SQL encodes the payload as data and does not execute additional statements

#### Scenario: Invalid token is rejected
- **WHEN** an extension name or alias contains unsupported characters
- **THEN** FDW returns a validation error before sending SQL to DuckDB

### Requirement: SQL construction SHALL use centralized helper APIs
SQL assembly sites in FDW code MUST call shared helper APIs for escaping/validation instead of ad-hoc `%s` interpolation.

#### Scenario: New SQL builder path uses helper
- **WHEN** a new SQL statement is introduced in the codebase
- **THEN** static checks and tests confirm helper usage and fail if raw interpolation is used for user-controlled inputs

