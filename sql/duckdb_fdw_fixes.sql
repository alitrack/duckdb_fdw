-- Regression checks for the P0/P1/P2 fixes.
--
-- Run with:  psql -d <db> -f sql/duckdb_fdw_fixes.sql
-- Every assertion returns a boolean in a "passed" column, so the output is
-- stable across PostgreSQL / DuckDB patch releases and can be turned into a
-- pg_regress test simply by adding an expected/ file and registering this
-- script in Makefile's REGRESS list.
--
-- NOTE: this script was added together with the source fixes but has NOT
-- been executed locally (no pg_config / libduckdb in the build environment).
-- Calibrate the expected output on a machine that can build the extension.

CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

CREATE SERVER fixes_srv FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdb_fdw_fixes_main.db');
CREATE USER MAPPING FOR PUBLIC SERVER fixes_srv;

-- ------------------------------------------------------------------
-- P0-1: a NULL required argument must raise a PostgreSQL error rather
-- than dereference a NULL datum and crash the backend.
-- ------------------------------------------------------------------
CREATE FUNCTION fixes_test_p0_1_null_arg() RETURNS boolean AS $$
BEGIN
    BEGIN
        PERFORM duckdb_create_s3_secret(NULL::name, 'k', 'id', 'secret');
        RETURN false;
    EXCEPTION WHEN null_value_not_allowed THEN
        RETURN true;
    END;
END $$ LANGUAGE plpgsql;

SELECT 'p0_1_null_required_arg' AS test,
       fixes_test_p0_1_null_arg() AS passed;

-- Companion check: region is optional.  Declaring the SQL function STRICT
-- would skip the call entirely when region is omitted, so assert that the
-- five-argument call still creates the secret successfully.
CREATE FUNCTION fixes_test_optional_region() RETURNS boolean AS $$
BEGIN
    BEGIN
        PERFORM duckdb_create_s3_secret(
            'fixes_srv', 'opt_region_fix', 'key_id', 'secret_value');
        RETURN true;
    EXCEPTION WHEN OTHERS THEN
        RETURN false;
    END;
END $$ LANGUAGE plpgsql;

SELECT 'p0_1_optional_region_still_works' AS test,
       fixes_test_optional_region() AS passed;

-- Also cover NULL in each of the other required positions.
CREATE FUNCTION fixes_test_p0_1_all_null_positions() RETURNS boolean AS $$
DECLARE ok boolean := true;
BEGIN
    BEGIN
        PERFORM duckdb_create_s3_secret('fixes_srv', NULL, 'id', 'secret');
        ok := false;
    EXCEPTION WHEN null_value_not_allowed THEN NULL;
    END;
    BEGIN
        PERFORM duckdb_create_s3_secret('fixes_srv', 'k', NULL, 'secret');
        ok := false;
    EXCEPTION WHEN null_value_not_allowed THEN NULL;
    END;
    BEGIN
        PERFORM duckdb_create_s3_secret('fixes_srv', 'k', 'id', NULL);
        ok := false;
    EXCEPTION WHEN null_value_not_allowed THEN NULL;
    END;
    RETURN ok;
END $$ LANGUAGE plpgsql;

SELECT 'p0_1_all_null_positions' AS test,
       fixes_test_p0_1_all_null_positions() AS passed;

-- ------------------------------------------------------------------
-- P0-2: CASE whose ELSE contains a PostgreSQL-only expression must not
-- be pushed down.  If it is, DuckDB raises "function not found" instead
-- of returning a value.
-- ------------------------------------------------------------------
SELECT duckdb_execute('fixes_srv', 'DROP TABLE IF EXISTS fixes_case');
SELECT duckdb_execute('fixes_srv', 'CREATE TABLE fixes_case (i INTEGER)');
SELECT duckdb_execute('fixes_srv', 'INSERT INTO fixes_case VALUES (-1), (1)');

CREATE FOREIGN TABLE fixes_case (i int4)
SERVER fixes_srv OPTIONS (table 'fixes_case');

CREATE FUNCTION fixes_local_add(x int) RETURNS int
AS $$ SELECT x + 1000 $$ LANGUAGE sql IMMUTABLE;

SELECT i,
       CASE WHEN i > 0 THEN i ELSE fixes_local_add(i) END AS v
FROM fixes_case ORDER BY i;

-- Simple CASE has its own operand; verify it too.
SELECT i,
       CASE i WHEN 1 THEN 'one' ELSE fixes_local_add(i)::text END AS v
FROM fixes_case ORDER BY i;

-- ------------------------------------------------------------------
-- P1-4: with no pg_duckdb in the backend the guard reports a stable
-- status.  (With pg_duckdb loaded, the same-version case now resolves
-- to compatible_proven and a mismatched version to incompatible; those
-- two states cannot be forced from this script.)
-- ------------------------------------------------------------------
SELECT 'p1_4_no_peer_status' AS test,
       duckdb_fdw_runtime_compatibility_status() = 'no_peer_loaded' AS passed;

-- ------------------------------------------------------------------
-- P2: force_readonly must reject DDL/DML issued via duckdb_execute.
-- An in-memory database is used deliberately: file-backed databases would
-- need a pre-existing file and can hit DuckDB's single-writer file lock.
-- The FDW-level gate covers :memory: even though DuckDB cannot open it
-- READ_ONLY itself.
-- ------------------------------------------------------------------
CREATE SERVER fixes_ro FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database ':memory:', force_readonly 'true');
CREATE USER MAPPING FOR PUBLIC SERVER fixes_ro;

CREATE FUNCTION fixes_test_readonly_dml() RETURNS boolean AS $$
BEGIN
    BEGIN
        PERFORM duckdb_execute('fixes_ro',
                               'CREATE TABLE should_not_exist (i INTEGER)');
        RETURN false;
    EXCEPTION WHEN OTHERS THEN
        RETURN true;
    END;
END $$ LANGUAGE plpgsql;

SELECT 'p2_force_readonly_rejects_dml' AS test,
       fixes_test_readonly_dml() AS passed;

-- ------------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------------
DROP FUNCTION fixes_test_p0_1_null_arg();
DROP FUNCTION fixes_test_optional_region();
DROP FUNCTION fixes_test_p0_1_all_null_positions();
DROP FUNCTION fixes_test_readonly_dml();
DROP FUNCTION fixes_local_add(int);
DROP FOREIGN TABLE fixes_case;
DROP SERVER fixes_srv CASCADE;
DROP SERVER fixes_ro CASCADE;
