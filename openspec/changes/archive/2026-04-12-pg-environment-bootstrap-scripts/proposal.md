## Why

Local build validation is currently blocked in environments where PostgreSQL development packages are missing, most visibly when `pg_config` is absent. The repository now documents the prerequisites, but the user asked for runnable environment scripts so PostgreSQL-related setup can be installed and verified directly.

## What Changes

- Add a Debian/Ubuntu-oriented PostgreSQL development environment install script that prints the plan by default and only performs package/repository changes with explicit opt-in.
- Add a PostgreSQL environment verification script that checks `pg_config`, PGXS, server headers, and common PostgreSQL CLI tools.
- Document both scripts in repository docs so local contributors know how to bootstrap and validate their environment.

## Capabilities

### New Capabilities
- `pg-development-environment-bootstrap`: repository scripts can prepare and verify PostgreSQL development prerequisites for supported local environments.

### Modified Capabilities
- `contributor-onboarding`: contributor guidance now includes runnable environment setup and verification scripts rather than prose-only package notes.

## Impact

- Affected code: new `scripts/install_pg_env.sh`, `scripts/verify_pg_env.sh`.
- Affected docs: `README.md`, `CONTRIBUTING.md`.
- Environment impact: PostgreSQL-related installation remains user-driven but is now encoded as a reproducible repo-local workflow.
