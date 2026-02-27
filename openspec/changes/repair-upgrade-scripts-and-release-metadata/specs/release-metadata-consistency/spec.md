## ADDED Requirements

### Requirement: Release metadata SHALL be internally consistent
Version, compatibility, and license values in release metadata files MUST match the shipped extension state.

#### Scenario: Version consistency check
- **WHEN** release validation runs
- **THEN** `duckdb_fdw.control`, `META.json`, and migration chain report the same target version

#### Scenario: License consistency check
- **WHEN** release validation runs
- **THEN** metadata license fields align with repository license text
