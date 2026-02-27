## Context

`duckdb_fdw` currently constructs SQL in many places with `%s` interpolation. Inputs include server options (`attach_catalogs`, `extensions`, credentials), FDW table options, and helper function arguments. These fields are not consistently escaped or validated.

## Goals / Non-Goals

**Goals:**
- Eliminate SQL injection vectors in DuckDB SQL generation paths.
- Ensure secrets are never emitted in user-visible error text or logs.
- Keep behavior and UX stable for valid configurations.

**Non-Goals:**
- Rewriting the entire planner/deparser architecture.
- Introducing external crypto or secret-storage dependencies.

## Decisions

1. Add shared helper functions:
- `duckdb_quote_literal(const char *)` for SQL string literals.
- `duckdb_quote_identifier(const char *)` for identifiers used in generated SQL.
- `duckdb_validate_token(const char *, enum token_kind)` for allowlisted tokens (extension names, alias names, secret names).
Rationale: one implementation point reduces drift and future regressions.

2. Convert high-risk call sites first:
- Secret creation (`duckdb_create_s3_secret`, connection setup secrets)
- ATTACH/catalog SQL generation
- IMPORT FOREIGN SCHEMA discovery SQL
- Relation/table option handling in deparse
Rationale: these paths consume directly user-provided strings.

3. Introduce sensitive-value redaction in error handling:
- Replace raw SQL echoing for failing secret operations with sanitized messages.
- Keep actionable context while masking key/secret values.
Rationale: operational debugging without credential leakage.

4. Add explicit tests with malicious payloads:
- Quote/semicolon payloads in table, schema, secret inputs.
- Assertions that payloads are escaped or rejected.
Rationale: verifies fix and prevents regression.

## Risks / Trade-offs

- [Risk] Stricter validation rejects existing but unsafe configurations.
  -> Mitigation: clear validation errors, migration notes, and explicit accepted token format.
- [Risk] Partial refactor leaves one unsafe call site behind.
  -> Mitigation: static grep checks in CI for raw SQL interpolation patterns.

## Migration Plan

1. Land helper APIs and unit tests.
2. Refactor high-risk call sites in small commits.
3. Add compatibility tests for normal (non-malicious) configs.
4. Publish release notes describing stricter validation.

Rollback: keep refactor behind focused commits so specific call-site changes can be reverted independently.

## Open Questions

- Should extension names support quoted mixed-case or only lowercase token format?
- Do we keep legacy permissive behavior behind a temporary compatibility option?
