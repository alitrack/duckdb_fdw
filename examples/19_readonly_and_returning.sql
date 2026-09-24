-- ============================================================
-- 19_readonly_and_returning.sql
-- Regression tests for force_readonly server option and
-- INSERT ... RETURNING support.
-- The "expecting rejection" steps toggle ON_ERROR_STOP locally so
-- the script still runs to completion under ON_ERROR_STOP=1.
-- Run: psql -v ON_ERROR_STOP=1 (run_tests.sh pattern)
-- ============================================================

\set ON_ERROR_STOP 1

-- ------------------------------------------------------------
-- Setup: one writable server (RW) and one read-only server (RO)
-- pointing at the same file database.
-- ------------------------------------------------------------
DROP SERVER IF EXISTS fdw_ro CASCADE;
DROP SERVER IF EXISTS fdw_rw CASCADE;

CREATE SERVER fdw_rw FOREIGN DATA WRAPPER duckdb_fdw
    OPTIONS (database '/tmp/fdw_ro_rw_test.db');
CREATE USER MAPPING FOR current_user SERVER fdw_rw;

CREATE SERVER fdw_ro FOREIGN DATA WRAPPER duckdb_fdw
    OPTIONS (database '/tmp/fdw_ro_rw_test.db', force_readonly 'true');
CREATE USER MAPPING FOR current_user SERVER fdw_ro;

-- Seed data through the RW server
SELECT duckdb_execute('fdw_rw',
    'CREATE OR REPLACE TABLE items(id INTEGER, name VARCHAR, price DOUBLE);
     INSERT INTO items VALUES (1, ''apple'', 1.5), (2, ''banana'', 0.9)');

-- Foreign tables on both servers
DROP FOREIGN TABLE IF EXISTS items_rw;
DROP FOREIGN TABLE IF EXISTS items_ro;
CREATE FOREIGN TABLE items_rw(id int, name text, price double precision)
    SERVER fdw_rw OPTIONS (table 'items');
CREATE FOREIGN TABLE items_ro(id int, name text, price double precision)
    SERVER fdw_ro OPTIONS (table 'items');

-- ------------------------------------------------------------
-- 1. Read-only server: reads work
-- ------------------------------------------------------------
SELECT count(*) AS ro_read_ok FROM items_ro;

-- ------------------------------------------------------------
-- 2. Read-only server: INSERT via foreign table is rejected at
--    plan time (relation not updatable).  The rejection below is the
--    EXPECTED result; \set ON_ERROR_STOP 0 captures it and continues.
-- ------------------------------------------------------------
\set ON_ERROR_STOP 0
\echo '--- expecting plan-time rejection of INSERT on read-only server ---'
INSERT INTO items_ro VALUES (99, 'x', 1.0);
\set ON_ERROR_STOP 1

-- ------------------------------------------------------------
-- 3. Read-only server: duckdb_execute with DML is rejected
-- ------------------------------------------------------------
\set ON_ERROR_STOP 0
\echo '--- expecting rejection of duckdb_execute DML on read-only server ---'
SELECT duckdb_execute('fdw_ro', 'INSERT INTO items VALUES (98, ''bad'', 1.0)');
\set ON_ERROR_STOP 1

-- ------------------------------------------------------------
-- 4. Read-only server: duckdb_execute with DDL is rejected
-- ------------------------------------------------------------
\set ON_ERROR_STOP 0
\echo '--- expecting rejection of duckdb_execute DDL on read-only server ---'
SELECT duckdb_execute('fdw_ro', 'CREATE TABLE evil(i INT)');
\set ON_ERROR_STOP 1

-- ------------------------------------------------------------
-- 5. Read-only server: duckdb_execute with a SELECT still works
-- ------------------------------------------------------------
SELECT duckdb_execute('fdw_ro', 'SELECT count(*) FROM items');

-- ------------------------------------------------------------
-- 6. RW server: plain INSERT still works (appender path)
-- ------------------------------------------------------------
INSERT INTO items_rw VALUES (10, 'plum', 2.25);
SELECT count(*) AS rw_after_plain_insert FROM items_rw;

-- ------------------------------------------------------------
-- 7. RW server: INSERT ... RETURNING returns the inserted row.
--    (Historically this used the SELECT * FROM (INSERT ...) subquery
--    form, which PostgreSQL does not accept syntactically; plain
--    top-level INSERT ... RETURNING is the working form.)
-- ------------------------------------------------------------
INSERT INTO items_rw VALUES (20, 'mango', 3.75)
RETURNING id, name, price;

-- ------------------------------------------------------------
-- 8. RW server: INSERT ... RETURNING with a subset of columns
-- ------------------------------------------------------------
INSERT INTO items_rw VALUES (30, 'kiwi', 0.5)
RETURNING name;

-- ------------------------------------------------------------
-- 9. RW server: multi-row INSERT with RETURNING
-- ------------------------------------------------------------
INSERT INTO items_rw VALUES (40, 'a', 1.0), (41, 'b', 2.0)
RETURNING id;

-- ------------------------------------------------------------
-- 10. RW server: default INSERT (no values, no returning) still fine
-- ------------------------------------------------------------
SELECT count(*) AS final_row_count FROM items_rw;

-- ------------------------------------------------------------
-- Cleanup
-- ------------------------------------------------------------
DROP FOREIGN TABLE IF EXISTS items_ro;
DROP FOREIGN TABLE IF EXISTS items_rw;
DROP SERVER IF EXISTS fdw_ro CASCADE;
DROP SERVER IF EXISTS fdw_rw CASCADE;
