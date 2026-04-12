## ADDED Requirements

### Requirement: Repository SHALL provide PostgreSQL development environment bootstrap scripts
The repository MUST include scripts that prepare and verify PostgreSQL development prerequisites for supported Debian/Ubuntu-style environments.

#### Scenario: Contributor prepares local PostgreSQL toolchain
- **WHEN** a contributor needs `pg_config`, PGXS, PostgreSQL headers, and common PostgreSQL tools for local builds
- **THEN** the repository provides a script to install the required packages and a script to verify the resulting environment

### Requirement: Environment bootstrap SHALL avoid accidental package installation by default
Bootstrap scripts with package-management side effects MUST require an explicit opt-in before running installation commands.

#### Scenario: Contributor inspects planned installation steps
- **WHEN** a contributor runs the PostgreSQL bootstrap script without the apply flag
- **THEN** the script prints the planned repository/package steps and exits without changing the system
