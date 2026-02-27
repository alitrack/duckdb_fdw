# tiered-ci-pipeline Specification

## Purpose
TBD - created by archiving change stabilize-tests-with-offline-fixtures-and-tiered-ci. Update Purpose after archive.
## Requirements
### Requirement: CI SHALL separate required and optional test tiers
The pipeline MUST classify jobs into required core checks and optional integration/cloud checks.

#### Scenario: Optional tier failure does not block core merge gate
- **WHEN** integration or cloud tier fails due to external dependency
- **THEN** merge gate outcome is determined by required core tier status only

### Requirement: Tier selection SHALL be explicit and reproducible
Test runner and CI config MUST expose explicit profiles and conditions for each tier.

#### Scenario: Local developer reproduces CI core run
- **WHEN** developer runs core profile locally
- **THEN** executed test set matches required CI core job scope

