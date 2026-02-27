# deterministic-offline-test-suite Specification

## Purpose
TBD - created by archiving change stabilize-tests-with-offline-fixtures-and-tiered-ci. Update Purpose after archive.
## Requirements
### Requirement: Core validation SHALL run without external network dependencies
The required test suite MUST pass in an environment without network access and without cloud credentials.

#### Scenario: Offline CI environment
- **WHEN** CI runner has no network access
- **THEN** core test profile executes and validates main FDW functionality successfully

### Requirement: Core tests SHALL cover claimed baseline features
Offline core tests MUST cover baseline read path, insert path, pushdown basics, type mapping, and extension lifecycle operations.

#### Scenario: Core feature regression
- **WHEN** a baseline feature regresses
- **THEN** offline core suite fails with a deterministic, actionable error

