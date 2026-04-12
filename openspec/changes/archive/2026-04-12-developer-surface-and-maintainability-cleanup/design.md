## Context

The repository currently mixes production code with leftover scaffolding and underspecified contributor workflows. `strtok()` still appears in parsing paths, `download_libduckdb.sh` fetches the latest GitHub release by default, there is no `CONTRIBUTING.md`, and `CHANGELOG` is too thin to serve as a reliable maintenance record.

This change intentionally stays out of new runtime semantics. It is about making the repository safer to maintain and easier to reproduce.

## Goals / Non-Goals

**Goals:**
- Replace `strtok()` usage with a reentrant helper path.
- Pin the default DuckDB bootstrap version while allowing explicit overrides.
- Add contributor guidance for build, test, verification, and environment prerequisites.
- Replace the free-form changelog blob with a versioned structure that does not fabricate unknown historical dates.
- Remove obviously unused maintenance residue from the build.

**Non-Goals:**
- Change FDW query semantics.
- Introduce a full release automation pipeline.
- Reconstruct historical release dates that are not present in the repository.

## Decisions

1. Add a shared tokenization helper instead of scattering `strtok_r` calls.
   - Keep the portability surface in one helper implemented in `sql_utils.c`.
   - Rationale: token parsing appears in more than one file, so the compatibility logic should not be duplicated.

2. Pin a default DuckDB version in the bootstrap script and allow override with `DUCKDB_VERSION`.
   - Use a deterministic default rather than the latest GitHub release.
   - Rationale: reproducible builds are more important than silently drifting to whatever release was published most recently.

3. Write a maintenance-oriented contributor guide.
   - Document the known verification constraints (`pg_config`, Docker mirror stability) and the repo-preferred validation commands.
   - Rationale: the repository should externalize the maintainer's tacit knowledge.

4. Remove the unused optimization stub instead of leaving dead residue in the build graph.
   - Rationale: dead placeholders increase confusion and suggest capabilities that do not exist.

## Risks / Trade-offs

- [Risk] Pinning a default DuckDB version may lag behind upstream bug fixes.
  -> Mitigation: keep `DUCKDB_VERSION` override support and make the default version easy to update intentionally.

- [Risk] Reentrant tokenization helper adds a small compatibility shim.
  -> Mitigation: centralize it in one helper and keep call sites simple.

- [Risk] Changelog restructuring may omit unknown historical timing details.
  -> Mitigation: explicitly mark unreconstructed history instead of inventing dates.
