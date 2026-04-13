## Context

The approved design spec lives at `docs/superpowers/specs/2026-04-13-pg-duckdb-coexistence-design.md`.

The first deliverable is intentionally guard-first:

- public support remains strict
- Linux is the only v1 runtime-detection target
- a peer-loaded backend gets blocked by default
- there is no v1 public success path for `duckdb_fdw` + `pg_duckdb` inside the same backend
- a future cooperative fingerprint path is reserved but not implemented now

## Goals / Non-Goals

**Goals**

- Centralize runtime coexistence decisions in one guard module.
- Add a session-scoped unsupported override GUC.
- Ensure every reachable DuckDB API entry point passes through the guard.
- Expose enough diagnostics to explain block decisions and preflight warnings.

**Non-Goals**

- Shipping a peer-loaded public success path in v1.
- Requiring `pg_duckdb` changes.
- Adding non-Linux runtime detection in v1.
- Moving this feature into default `installcheck` with a real `pg_duckdb` integration environment.

## Decisions

1. **Linux-first backend detection**
   - Use a Linux dynamic-loader enumeration path as the primary runtime signal for whether the current backend has loaded `pg_duckdb`.
   - If the primary signal is not reliable enough during implementation spike work, stop Phase 1 and tighten detection before expanding the guard surface.

2. **Guard-first v1 outcome model**
   - `NoPeerLoaded` passes.
   - `PeerLoaded + unproven` blocks.
   - `PeerLoaded + incompatible` blocks.
   - Explicit unsupported override allows execution with a warning.
   - `CompatibleProven` remains a reserved future state in the model but is not produced in v1.

3. **Single source of truth**
   - Add a dedicated guard module rather than spreading coexistence checks across `connection.c` and `duckdb_fdw.c`.

4. **Install-time preflight is informational only**
   - SQL installation path may warn when `pg_duckdb` is present or available, but final allow/block decisions happen at runtime inside the current backend.

5. **Diagnostics are part of v1**
   - A blocked feature without inspection hooks will be misread as arbitrary breakage, so the first release includes diagnostic SQL surfaces and explicit error messages.

## Risks / Trade-offs

- [Risk] v1 will conservatively block some apparently safe peer-loaded setups.
  -> Mitigation: keep the public contract strict and offer diagnostics plus an explicit unsupported override for experiments.

- [Risk] Linux dynamic-loader inspection may be brittle across environments.
  -> Mitigation: scope v1 to Linux-first, keep fallback reporting explicit, and do not pretend unsupported platforms are covered.

- [Risk] Users may treat the override as a supported path.
  -> Mitigation: use a high-friction GUC name, warning logs, and documentation that clearly marks the mode unsupported.
