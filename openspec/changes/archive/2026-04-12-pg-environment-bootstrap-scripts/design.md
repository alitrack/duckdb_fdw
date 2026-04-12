## Context

The repository can now describe its development prerequisites accurately, but that is still one step removed from a usable local setup flow. The immediate blocker seen in this environment was missing `pg_config`, which prevents `make USE_PGXS=1` from even starting.

The user explicitly asked for scripts so they can perform PostgreSQL-related installation themselves. That means the repository should supply an install helper and a verification helper rather than only more documentation.

## Goals / Non-Goals

**Goals:**
- Add a script that prepares PostgreSQL development prerequisites for Debian/Ubuntu-style environments.
- Ensure the install script is safe-by-default and requires an explicit flag before making system changes.
- Add a verification script that confirms the local PostgreSQL toolchain needed by `duckdb_fdw`.
- Document how to use both scripts.

**Non-Goals:**
- Automatically install DuckDB itself.
- Support every Linux distribution family in one pass.
- Bypass package-manager permissions or run privileged commands silently.

## Decisions

1. Keep the install script plan-only by default.
   - Require `--apply` before executing apt/repository commands.
   - Rationale: environment bootstrap scripts should be inspectable and non-destructive until the user opts in.

2. Target Debian/Ubuntu/WSL-style setups first.
   - Use `/etc/os-release` detection and the official PostgreSQL PGDG APT flow when requested.
   - Rationale: this matches the current user environment and keeps the script concrete instead of speculative.

3. Separate install and verify responsibilities.
   - `install_pg_env.sh` handles package/repository setup.
   - `verify_pg_env.sh` checks `pg_config`, PGXS, server headers, and PostgreSQL CLI tools.
   - Rationale: verification remains useful even when the user installs packages manually.

## Risks / Trade-offs

- [Risk] Packaging guidance can drift over time.
  -> Mitigation: keep the script narrowly aligned with official PostgreSQL APT guidance and document supported environments explicitly.

- [Risk] Users on unsupported distros may expect the script to work anyway.
  -> Mitigation: fail fast with a clear message and keep the docs honest about supported systems.
