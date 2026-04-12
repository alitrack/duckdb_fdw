## MODIFIED Requirements

### Requirement: Repository SHALL provide PostgreSQL development environment bootstrap scripts
The repository MUST include scripts that prepare and verify PostgreSQL development prerequisites for supported Debian/Ubuntu-style environments.

#### Scenario: Contributor prepares local PostgreSQL toolchain
- **WHEN** a contributor needs `pg_config`, PGXS, PostgreSQL headers, and common PostgreSQL tools for local builds
- **THEN** the repository provides a script to install the required packages and a script to verify the resulting environment

#### Scenario: PostgreSQL tools are installed under bindir but not exported on PATH
- **WHEN** `pg_ctl` and `initdb` exist under `pg_config --bindir` but are not on the shell `PATH`
- **THEN** the verification script reports those tools as present using the resolved bindir paths
