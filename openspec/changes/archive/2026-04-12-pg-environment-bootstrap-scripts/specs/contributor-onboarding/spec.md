## MODIFIED Requirements

### Requirement: Repository SHALL provide contributor onboarding guidance
The repository MUST include a contributor guide covering prerequisites, build steps, test entry points, verification expectations, and runnable environment bootstrap scripts for local setup.

#### Scenario: New contributor starts local setup
- **WHEN** a developer opens the repository for the first time
- **THEN** they can identify the required toolchain, use repo-local scripts to prepare PostgreSQL prerequisites, and find the main validation commands without reverse-engineering maintainer workflow
