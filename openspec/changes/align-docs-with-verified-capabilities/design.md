## Context

The project includes ambitious feature claims and extensive examples, but validation coverage is uneven. Documentation needs a stricter traceability model linking claims to evidence.

## Goals / Non-Goals

**Goals:**
- Ensure every high-level claim is backed by code path and/or test evidence.
- Distinguish fully implemented features from partial or planned work.
- Make example prerequisites explicit to avoid confusing failures.

**Non-Goals:**
- Reducing scope of long-term roadmap.
- Replacing technical design docs with marketing content.

## Decisions

1. Evidence-linked claims:
- Every major capability entry includes pointers to implementation files and test references.
Rationale: improves trust and reviewability.

2. Status matrix model:
- Add matrix fields: capability, status, validation evidence, prerequisites.
Rationale: concise communication of maturity and operability.

3. Example taxonomy:
- Tag examples as `offline-core`, `network-integration`, `credential-required`.
Rationale: avoids treating all examples as equally runnable in CI/local defaults.

4. Docs QA gate:
- Add lint/check to ensure status matrix exists and contains no unqualified "fully supported" claims without evidence links.
Rationale: keeps docs honest over time.

## Risks / Trade-offs

- [Risk] Documentation becomes more conservative and less flashy.
  -> Mitigation: keep roadmap section to communicate forward direction.
- [Risk] Maintaining evidence links requires ongoing discipline.
  -> Mitigation: include doc checks in PR review checklist/CI.

## Migration Plan

1. Audit current claims and map each to evidence or downgrade status.
2. Publish capability status matrix and update examples taxonomy.
3. Add known-limits section and changelog sync process.
4. Add lightweight doc QA checks in CI.

Rollback: none required; this is non-breaking documentation correction.

## Open Questions

- Should matrix live only in README or additionally in a dedicated docs page?
- What minimum evidence threshold is required before status can move to implemented?
