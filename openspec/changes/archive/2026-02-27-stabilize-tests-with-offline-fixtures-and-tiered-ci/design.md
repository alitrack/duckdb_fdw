## Context

The current test entrypoint executes many example scripts, including cloud/S3 and public network resources. Failures in external dependencies can fail builds unrelated to code correctness.

## Goals / Non-Goals

**Goals:**
- Make core FDW correctness tests fully offline and deterministic.
- Keep integration/cloud coverage, but classify it as separate optional tier.
- Define explicit quality gates for merge/release.

**Non-Goals:**
- Removing cloud/integration tests entirely.
- Building a full external service emulation stack in the first iteration.

## Decisions

1. Test tiers:
- Tier 1 (required): offline core functional tests + regression expected outputs.
- Tier 2 (optional by default): network/public dataset integration tests.
- Tier 3 (credential-gated): S3/Iceberg/cloud tests.
Rationale: clear reliability boundaries and predictable CI.

2. Script organization:
- Refactor `run_tests.sh` to accept profile flags (`core`, `integration`, `cloud`).
- Keep core profile default.
Rationale: easier local usage and CI mapping.

3. Fixture strategy:
- Add local SQL/data fixtures for representative functionality claims.
- Avoid dependence on remote URLs in required tests.
Rationale: deterministic reproducibility.

## Risks / Trade-offs

- [Risk] Reduced default coverage of cloud edge cases.
  -> Mitigation: schedule periodic cloud pipeline and keep scripts maintained.
- [Risk] Initial effort to build fixtures and expected outputs.
  -> Mitigation: incremental migration from existing examples.

## Migration Plan

1. Define profile taxonomy and update test runner.
2. Move required checks to offline fixture-backed scripts.
3. Update CI workflow with tiered jobs and clear gating.
4. Document how to run each tier locally and in CI.

Rollback: keep legacy monolithic script as temporary fallback for one release cycle.

## Open Questions

- Should cloud tier run nightly or on-demand only?
- Which minimum PG version matrix is required for merge gating vs release gating?
