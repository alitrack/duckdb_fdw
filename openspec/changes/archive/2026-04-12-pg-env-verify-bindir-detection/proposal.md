## Why

`verify_pg_env.sh` currently reports `pg_ctl` and `initdb` as missing when they are installed under PostgreSQL's bindir but not exported on `PATH`. That creates a false negative in exactly the environment it is supposed to diagnose.

## What Changes

- Add a regression script covering the case where PostgreSQL tools exist under `pg_config --bindir` but not on `PATH`.
- Update `verify_pg_env.sh` to resolve `pg_ctl` and `initdb` from PostgreSQL's bindir when direct `PATH` lookup fails.

## Capabilities

### New Capabilities
- None.

### Modified Capabilities
- `pg-development-environment-bootstrap`: environment verification must treat PostgreSQL tools found via `pg_config --bindir` as present, even if they are not exported on the shell `PATH`.

## Impact

- Affected code: `scripts/verify_pg_env.sh`, new `scripts/test_verify_pg_env.sh`.
- User impact: fewer false negatives after PostgreSQL packages are installed from distro packages or PGDG.
