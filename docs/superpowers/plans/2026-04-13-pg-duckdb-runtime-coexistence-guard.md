# pg_duckdb Runtime Coexistence Guard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Linux-first, guard-first coexistence layer so `duckdb_fdw` detects peer-loaded `pg_duckdb`, blocks unsupported same-backend coexistence by default, and exposes diagnostics plus an explicit experimental override.

**Architecture:** Introduce a dedicated runtime guard module that owns backend detection, local runtime fingerprint capture, compatibility-status decisions, and user-facing messages. Wire every direct DuckDB entry path through that guard, register a session-scoped override in `_PG_init()`, and expose SQL diagnostics plus install-time soft preflight without adding a peer-loaded public success path in v1.

**Tech Stack:** PostgreSQL extension C APIs, DuckDB C API, Linux dynamic loader inspection, PGXS build, SQL extension upgrade scripts, `make USE_PGXS=1 installcheck`, scripted `psql` verification.

---

## File Map

- Create: `runtime_guard.c`
  Runtime coexistence guard implementation, Linux-first backend detection, local fingerprint capture, status evaluation, and message helpers.
- Create: `runtime_guard.h`
  Public interface for guard status enums, fingerprint structs, and guard entry points used by other modules.
- Modify: `duckdb_fdw.h`
  Shared declarations needed across `duckdb_fdw.c`, `connection.c`, and the new guard module.
- Modify: `Makefile`
  Add the new guard module to `OBJS` and Linux runtime-detection link flags such as `-ldl` if the chosen loader APIs require them.
- Modify: `duckdb_fdw.c`
  Register the override GUC in `_PG_init()`, guard `duckdb_fdw_version()`, and add SQL-callable preflight/diagnostic functions.
- Modify: `connection.c`
  Guard `duckdb_get_connection()` before any DuckDB runtime work begins.
- Modify: `duckdb_fdw--2.0.0.sql`
  Legacy install script kept only for older extension versions; do not rely on it for the new default version.
- Create: `duckdb_fdw--2.0.0--2.0.1.sql`
  Mandatory upgrade path for new SQL-callable functions and install-time soft preflight behavior.
- Create: `duckdb_fdw--2.0.1.sql`
  Fresh-install script for the new default version, including automatic soft preflight invocation.
- Modify: `duckdb_fdw.control`
  Bump `default_version` to `2.0.1`.
- Modify: `README.md`
  Document strict coexistence policy, Linux-first scope, diagnostics, and unsupported override semantics.
- Create or modify: `sql/pg_duckdb_runtime_guard.sql`
  Scripted verification for guard paths if repo regression style supports it.
- Create or modify: `expected/pg_duckdb_runtime_guard.out`
  Expected output for scripted verification if added to regression.
- Create: `scripts/verify_pg_duckdb_coexistence.sh`
  Linux-first manual/integration verification helper for peer-loaded detection and override behavior.
- Modify: `run_tests.sh`
  Optionally hook the scripted verification lane without pushing true `pg_duckdb` integration into default `installcheck`.

### Task 1: Lock the OpenSpec surface and planning scaffolding

**Files:**
- Create: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/.openspec.yaml`
- Create: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/README.md`
- Create: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/proposal.md`
- Create: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/design.md`
- Create: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/tasks.md`
- Create: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/specs/pg-duckdb-runtime-coexistence/spec.md`
- Create: `docs/superpowers/plans/2026-04-13-pg-duckdb-runtime-coexistence-guard.md`

- [ ] **Step 1: Verify the active spec and implementation scope are v1 guard-first**

Run: `sed -n '1,260p' docs/superpowers/specs/2026-04-13-pg-duckdb-coexistence-design.md`
Expected: the spec explicitly says v1 has no peer-loaded public success path and is Linux-first.

- [ ] **Step 2: Verify and complete the OpenSpec change scaffold**

Verify the existing change directory contains `proposal.md`, `design.md`, `tasks.md`, `.openspec.yaml`, `README.md`, and one spec delta under `specs/pg-duckdb-runtime-coexistence/spec.md`. Add any missing piece instead of recreating files blindly.

```md
## Why
duckdb_fdw needs a backend-local coexistence contract before it can safely run beside pg_duckdb.
```

- [ ] **Step 3: Add the implementation plan document**

Write this plan into `docs/superpowers/plans/2026-04-13-pg-duckdb-runtime-coexistence-guard.md` with exact files, tasks, commands, and commit points.

- [ ] **Step 4: Review the scaffolding for scope creep**

Run: `find openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard -maxdepth 3 -type f | sort`
Expected: only coexistence-guard planning artifacts exist; no unrelated specs or runtime promises appear.

- [ ] **Step 5: Commit**

```bash
git add -f docs/superpowers/plans/2026-04-13-pg-duckdb-runtime-coexistence-guard.md openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard
git commit -m "Define the execution lane for pg_duckdb coexistence guarding" \
  -m "Constraint: Planning artifacts must stay aligned with the approved v1 guard-first coexistence spec
Confidence: high
Scope-risk: narrow
Directive: Do not fold future cooperative success paths into the first implementation plan
Tested: Planning artifacts reviewed for spec alignment
Not-tested: No runtime changes in this commit"
```

### Task 2: Add the runtime guard skeleton and build wiring

**Files:**
- Create: `runtime_guard.c`
- Create: `runtime_guard.h`
- Modify: `duckdb_fdw.h`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing compile-time slice**

Add the new module names to `Makefile`, Linux link flags needed by the chosen loader-inspection API, and declarations to `duckdb_fdw.h`/`runtime_guard.h` before implementing bodies.

```c
typedef enum DuckDBRuntimeCompatibilityStatus
{
    DUCKDB_RUNTIME_NO_PEER_LOADED,
    DUCKDB_RUNTIME_PEER_LOADED_NEED_VALIDATION,
    DUCKDB_RUNTIME_COMPATIBLE_PROVEN,
    DUCKDB_RUNTIME_COMPATIBLE_UNPROVEN,
    DUCKDB_RUNTIME_INCOMPATIBLE
} DuckDBRuntimeCompatibilityStatus;
```

- [ ] **Step 2: Run the build to confirm the missing-symbol failure**

Run: `make USE_PGXS=1`
Expected: FAIL with unresolved references to the new guard functions or incomplete type declarations.

- [ ] **Step 3: Write the minimal guard skeleton**

Implement placeholder-safe functions in `runtime_guard.c` for:

```c
DuckDBRuntimeCompatibilityStatus duckdb_runtime_guard_status(void);
void duckdb_runtime_guard_check(void);
const char *duckdb_runtime_status_name(DuckDBRuntimeCompatibilityStatus status);
```

Use Linux-first placeholder detection hooks, but keep the initial implementation conservative so peer-loaded paths resolve to `COMPATIBLE_UNPROVEN` until richer signals are added.

- [ ] **Step 4: Rebuild to confirm the skeleton links**

Run: `make USE_PGXS=1`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add Makefile duckdb_fdw.h runtime_guard.h runtime_guard.c
git commit -m "Create the runtime guard skeleton before wiring execution paths" \
  -m "Constraint: Linux-first runtime detection is the only v1 supported platform target
Confidence: medium
Scope-risk: moderate
Directive: Keep peer-loaded success paths out of v1; unresolved peer-loaded states must stay blocked by default
Tested: make USE_PGXS=1
Not-tested: No real pg_duckdb peer-loaded integration yet"
```

### Task 3: Register the override GUC and guard DuckDB entry points

**Files:**
- Modify: `duckdb_fdw.c`
- Modify: `connection.c`
- Modify: `runtime_guard.c`
- Test: `sql/duckdb_fdw.sql` or dedicated verification SQL if needed

- [ ] **Step 1: Write the failing behavior check for blocked peer-loaded execution**

Add or script a verification path that expects guarded entry points to block when the status is forced to `COMPATIBLE_UNPROVEN`.

Example target behavior:

```sql
SELECT duckdb_fdw_version();
-- ERROR: strict coexistence policy rejected execution ...
```

- [ ] **Step 2: Guard `duckdb_get_connection()` and `duckdb_fdw_version()`**

Add guard calls before any DuckDB C API invocation.

```c
duckdb_runtime_guard_check();
PG_RETURN_TEXT_P(cstring_to_text(duckdb_library_version()));
```

- [ ] **Step 3: Register the unsupported override GUC in `_PG_init()`**

Add a session-scoped boolean GUC with an explicit high-risk name.

```c
DefineCustomBoolVariable(
    "duckdb_fdw.allow_unsupported_pg_duckdb_coexistence",
    ...
);
```

- [ ] **Step 4: Re-run focused verification**

Run: `make USE_PGXS=1`
Expected: PASS.

Run: `PGHOST=/tmp PGPORT=5433 PGUSER=lhy make USE_PGXS=1 installcheck`
Expected: PASS, or if regression files were extended for the guard, expected output matches the new policy.

- [ ] **Step 5: Commit**

```bash
git add duckdb_fdw.c connection.c runtime_guard.c
git commit -m "Block unproven same-backend coexistence before DuckDB runtime calls" \
  -m "Constraint: Public support must reject unproven pg_duckdb coexistence in v1
Confidence: high
Scope-risk: moderate
Directive: Keep the unsupported override explicit and session-scoped; do not make it the default path
Tested: make USE_PGXS=1; PGHOST=/tmp PGPORT=5433 PGUSER=lhy make USE_PGXS=1 installcheck
Not-tested: Real peer-loaded pg_duckdb lane unless a local environment is available"
```

### Task 4: Add SQL diagnostics and install-time soft preflight

**Files:**
- Modify: `duckdb_fdw.c`
- Create: `duckdb_fdw--2.0.0--2.0.1.sql`
- Create: `duckdb_fdw--2.0.1.sql`
- Modify: `duckdb_fdw.control`
- Modify: `duckdb_fdw.h`

- [ ] **Step 1: Write the failing SQL surface expectations**

Define how operators will inspect the guard state.

```sql
SELECT duckdb_fdw_runtime_compatibility_status();
SELECT duckdb_fdw_runtime_fingerprint();
SELECT duckdb_fdw_preflight();
```

- [ ] **Step 2: Implement SQL-callable diagnostic functions**

Return simple, stable values first:

```c
PG_FUNCTION_INFO_V1(duckdb_fdw_runtime_compatibility_status);
PG_FUNCTION_INFO_V1(duckdb_fdw_runtime_fingerprint);
PG_FUNCTION_INFO_V1(duckdb_fdw_preflight);
```

Use `NOTICE/WARNING` only for preflight; do not let preflight become a hard blocker.

- [ ] **Step 3: Expose the functions in extension SQL and trigger install-time soft preflight**

Create `duckdb_fdw--2.0.1.sql` with the new SQL surfaces and an automatic `SELECT duckdb_fdw_preflight();` call during `CREATE EXTENSION duckdb_fdw`. Add `duckdb_fdw--2.0.0--2.0.1.sql` for upgrades and bump `duckdb_fdw.control` `default_version` to `2.0.1`.

- [ ] **Step 4: Run extension upgrade/install verification**

Run:

```bash
make USE_PGXS=1
PGHOST=/tmp PGPORT=5433 PGUSER=lhy make USE_PGXS=1 installcheck
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add duckdb_fdw.c duckdb_fdw.h duckdb_fdw.control duckdb_fdw--2.0.0--2.0.1.sql duckdb_fdw--2.0.1.sql
git commit -m "Expose coexistence diagnostics through a safe extension upgrade path" \
  -m "Constraint: Supported ALTER EXTENSION upgrades must succeed without manual SQL surgery
Confidence: high
Scope-risk: moderate
Directive: Any new SQL-visible surface must land with both fresh-install and upgrade coverage
Tested: make USE_PGXS=1; PGHOST=/tmp PGPORT=5433 PGUSER=lhy make USE_PGXS=1 installcheck
Not-tested: Cross-version upgrade from every historical release unless explicitly scripted"
```

### Task 5: Add Linux-first detection verification and user documentation

**Files:**
- Create: `scripts/verify_pg_duckdb_coexistence.sh`
- Modify: `README.md`
- Modify: `run_tests.sh`
- Test: `scripts/verify_pg_duckdb_coexistence.sh`

- [ ] **Step 1: Write the verification helper contract**

Create a script that can exercise:

- no peer-loaded backend
- simulated or real peer-loaded blocked path
- override path warning behavior

Keep the script Linux-first and explicit about unsupported platforms.

- [ ] **Step 2: Add the script and hook it into the test surface**

Add a focused shell verifier, and wire it into `run_tests.sh` only as a dedicated lane or optional check, not as the default `installcheck` path. Keep the verification split explicit:

- default `installcheck`: only `NoPeerLoaded` behavior and SQL surface checks
- dedicated script: real or simulated peer-loaded block/override verification

```bash
./scripts/verify_pg_duckdb_coexistence.sh --pg-port 5433
```

- [ ] **Step 3: Update README**

Document:

- strict coexistence policy
- Linux-first scope
- no public peer-loaded success path in v1
- unsupported override semantics
- diagnostic SQL entry points

- [ ] **Step 4: Run the scripted verification**

Run: `bash scripts/verify_pg_duckdb_coexistence.sh --pg-port 5433`
Expected: PASS for the scripted checks, or a clear skip message when the optional peer-loaded environment is unavailable.

- [ ] **Step 5: Commit**

```bash
git add README.md run_tests.sh scripts/verify_pg_duckdb_coexistence.sh
git commit -m "Document the supported coexistence boundary and verify the Linux-first guard" \
  -m "Constraint: v1 support is Linux-first and guard-first only
Confidence: medium
Scope-risk: narrow
Directive: Documentation must state that peer-loaded success is not part of public v1 support
Tested: bash scripts/verify_pg_duckdb_coexistence.sh --pg-port 5433
Not-tested: Optional peer-loaded checks when local pg_duckdb is unavailable"
```

### Task 6: Final verification and change closure

**Files:**
- Modify: `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/tasks.md`
- Review: all files touched above

- [ ] **Step 1: Run full planned verification**

Run:

```bash
make USE_PGXS=1
PGHOST=/tmp PGPORT=5433 PGUSER=lhy make USE_PGXS=1 installcheck
bash scripts/verify_pg_duckdb_coexistence.sh --pg-port 5433
```

Expected: all required checks pass; any optional peer-loaded lane either passes or reports a documented skip condition.

- [ ] **Step 2: Update OpenSpec tasks**

Mark completed tasks in `openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/tasks.md` and ensure unfinished future-work items remain unchecked.

- [ ] **Step 3: Review the user-facing contract**

Verify that the implementation still matches the spec:

- v1 blocks peer-loaded coexistence by default
- no public peer-loaded success path
- override remains explicitly unsupported

- [ ] **Step 4: Prepare archive readiness notes**

Capture any remaining future-only items, especially cooperative fingerprint work, in the change README or final summary rather than silently expanding scope.

- [ ] **Step 5: Commit**

```bash
git add openspec/changes/2026-04-13-pg-duckdb-runtime-coexistence-guard/tasks.md
git commit -m "Close the first coexistence-guard delivery after verification" \
  -m "Constraint: OpenSpec task closure must reflect verified behavior, not intended behavior
Confidence: high
Scope-risk: narrow
Directive: Leave cooperative fingerprint follow-up out of the archived v1 scope unless it was actually delivered
Tested: make USE_PGXS=1; PGHOST=/tmp PGPORT=5433 PGUSER=lhy make USE_PGXS=1 installcheck; bash scripts/verify_pg_duckdb_coexistence.sh --pg-port 5433
Not-tested: Future cooperative pg_duckdb fingerprint path"
```
