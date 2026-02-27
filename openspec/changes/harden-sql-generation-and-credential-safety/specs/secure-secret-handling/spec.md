## ADDED Requirements

### Requirement: Secrets SHALL NOT appear in FDW error or log output
Sensitive values such as S3 key IDs, secret keys, and secret SQL definitions MUST be redacted from user-visible error messages and logs.

#### Scenario: Secret creation fails
- **WHEN** DuckDB returns an error during secret creation
- **THEN** PostgreSQL error text excludes raw key/secret values and includes only sanitized context

### Requirement: Secret-related APIs SHALL validate names and encode values safely
Secret names and related SQL inputs MUST be validated/escaped before query execution.

#### Scenario: Secret name injection attempt
- **WHEN** a caller passes a secret name containing SQL control characters
- **THEN** the function rejects the input with a validation error and does not execute SQL
