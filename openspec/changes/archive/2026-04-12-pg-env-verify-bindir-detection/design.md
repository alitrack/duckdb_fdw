## Context

The current machine already has `/usr/lib/postgresql/17/bin/pg_ctl` and `/usr/lib/postgresql/17/bin/initdb`, but `verify_pg_env.sh` only checks `command -v`, so it reports them as missing when the bin directory is not on `PATH`.

This is not a package-install problem. It is a detection bug in the verification script.

## Goals / Non-Goals

**Goals:**
- Make `verify_pg_env.sh` prefer `PATH` lookup but fall back to `pg_config --bindir`.
- Keep the script output explicit about the resolved tool path.
- Add a deterministic regression script for this exact case.

**Non-Goals:**
- Change the install script.
- Modify shell startup files or export PostgreSQL bin directories globally.

## Decisions

1. Resolve PostgreSQL tools via a helper function.
   - First try `command -v`.
   - Then try `${BINDIR}/<tool>` if `pg_config --bindir` is available.
   - Rationale: this matches how PostgreSQL packages are often installed on Debian/Ubuntu.

2. Keep the regression self-contained.
   - Use a temporary fake `pg_config`, fake `psql`, and a separate bindir with `pg_ctl`/`initdb`.
   - Rationale: the test should fail against the old script and pass after the fix without depending on host package layout.
