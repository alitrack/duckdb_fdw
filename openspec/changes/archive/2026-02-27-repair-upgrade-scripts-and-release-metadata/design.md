## Context

The extension history includes multiple versioned SQL scripts and migration steps. Some function type changes are done with `CREATE OR REPLACE` even when return type changed, which is upgrade-fragile in PostgreSQL. Metadata values differ between control file and META.

## Goals / Non-Goals

**Goals:**
- Make upgrades from each supported source version deterministic.
- Ensure metadata consistency across all release-facing files.
- Add pre-release validation gate for migration and metadata coherence.

**Non-Goals:**
- Rewriting historical changelog details outside required corrections.
- Expanding support window beyond currently declared version lineage.

## Decisions

1. Safe SQL migration pattern:
- Use explicit `DROP FUNCTION ...` + `CREATE FUNCTION ...` where signatures/return types changed.
- Keep idempotent guards where possible, but prioritize correctness.
Rationale: PostgreSQL function replacement rules require explicit handling.

2. Canonical version source:
- Treat extension version as canonical in `duckdb_fdw.control`, then mirror into META/release artifacts.
Rationale: avoids drift and packaging inconsistencies.

3. Release validation:
- Add script/check that verifies version, license, and upgrade script chain consistency.
- Add install/upgrade smoke test matrix for key version hops.
Rationale: catches breakage before release.

## Risks / Trade-offs

- [Risk] Stricter migration scripts could fail on unusual user-modified environments.
  -> Mitigation: explicit migration notes and guard checks.
- [Risk] Historical version support complexity increases test matrix cost.
  -> Mitigation: focus on representative major hops plus latest-minus-one.

## Migration Plan

1. Audit function signatures across versioned SQL files.
2. Rewrite fragile migrations with explicit safe transitions.
3. Align metadata files and add validation script.
4. Run install/upgrade matrix and publish migration guidance.

Rollback: ship hotfix migration scripts with conservative fallback path.

## Open Questions

- Which historical versions are officially guaranteed for direct upgrade?
- Should metadata consistency checks be hard-fail in CI for all branches?
