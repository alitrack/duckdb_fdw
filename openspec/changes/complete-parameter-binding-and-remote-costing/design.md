## Context

`deparse.c` emits `?` placeholders and collects params, but planning/execution currently does not reliably carry `fdw_exprs` and bind values through runtime execution. Costing uses fixed row defaults in core paths and ignores configured remote estimate options.

## Goals / Non-Goals

**Goals:**
- Make parameterized pushdown queries reliable for prepared and nested execution contexts.
- Improve plan quality by integrating optional remote cost estimation.
- Keep fallback planning deterministic when remote estimation is not used.

**Non-Goals:**
- Building a full remote statistics subsystem.
- Replacing PostgreSQL planner internals.

## Decisions

1. Parameter lifecycle completion:
- Populate `fdw_exprs` in foreign plan from `params_list`.
- Evaluate/bind param values at execution start/rescan.
- Execute prepared statements with bound values for pushdown queries.
Rationale: aligns with FDW parameter model and avoids ad-hoc string interpolation.

2. Unified handling across relation kinds:
- Base, join, and upper relations share a common parameter extraction/binding utility.
Rationale: avoids diverging logic and inconsistent behavior.

3. Cost estimation:
- Respect `use_remote_estimate` and issue lightweight remote estimate queries.
- If estimate fails/timeouts, fallback to deterministic defaults and surface DEBUG message.
Rationale: better plan quality without introducing brittle hard failures.

## Risks / Trade-offs

- [Risk] Added planning overhead from remote estimation calls.
  -> Mitigation: make opt-in, cache estimates per planning cycle.
- [Risk] Param type mismatch at bind time.
  -> Mitigation: explicit OID/type conversion map and test coverage.

## Migration Plan

1. Add param propagation and binding scaffolding.
2. Switch execution to prepared/bound flow for queries with params.
3. Implement remote estimate option path and fallback.
4. Add regress tests for prepared statements and plan shape checks.

Rollback: keep legacy non-parameterized execution path for non-param queries intact.

## Open Questions

- Which estimate query strategy is most stable across DuckDB versions?
- Should remote estimate calls include a strict timeout guard?
