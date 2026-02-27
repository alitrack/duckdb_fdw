# extension-upgrade-compatibility Specification

## Purpose
TBD - created by archiving change repair-upgrade-scripts-and-release-metadata. Update Purpose after archive.
## Requirements
### Requirement: Supported extension upgrades SHALL complete without manual intervention
For all officially supported source versions, `ALTER EXTENSION ... UPDATE` MUST succeed without requiring manual SQL surgery.

#### Scenario: Function return-type transition
- **WHEN** an upgrade crosses a version where function return type changed
- **THEN** migration script applies safe drop/create sequence and upgrade completes successfully

### Requirement: Upgrade scripts SHALL preserve expected extension objects
Migration scripts MUST leave required functions and FDW objects in expected definitions for target version.

#### Scenario: Post-upgrade object verification
- **WHEN** upgrade completes
- **THEN** object definition checks confirm expected signatures and object presence

