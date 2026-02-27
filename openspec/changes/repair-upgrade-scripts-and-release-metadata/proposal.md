## Why

Current extension SQL scripts and package metadata are inconsistent across versions (function signatures, version numbers, and license fields). This can break upgrades and weakens release reliability.

## What Changes

- Repair extension upgrade scripts to safely handle function signature/return-type transitions.
- Ensure all upgrade paths from supported historical versions are valid and tested.
- Normalize version values across control, SQL migration files, and `META.json`.
- Align declared license and compatibility metadata with repository reality.
- Add release validation checks to prevent metadata drift.

## Capabilities

### New Capabilities
- `extension-upgrade-compatibility`: Deterministic extension upgrades across supported versions.
- `release-metadata-consistency`: Single-source-consistent version/license metadata across release artifacts.

### Modified Capabilities
- None.

## Impact

- Affected files: `duckdb_fdw.control`, `duckdb_fdw--*.sql`, `META.json`, release docs.
- Users gain safer upgrade behavior and fewer deployment surprises.
- Potential change in packaging/release automation checks.
