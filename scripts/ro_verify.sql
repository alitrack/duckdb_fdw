\set QUIET off
\echo '=== setup: user mapping for ro_srv; data via writable duck_srv (same file) ==='
CREATE USER MAPPING IF NOT EXISTS FOR lhy SERVER ro_srv;
SELECT duckdb_execute('duck_srv', 'CREATE TABLE IF NOT EXISTS ro_t (a int, b text)');
SELECT duckdb_execute('duck_srv', 'DELETE FROM ro_t');
SELECT duckdb_execute('duck_srv', 'INSERT INTO ro_t VALUES (1, chr(120)), (2, chr(121))');

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='f_ro_t') THEN
    EXECUTE 'CREATE FOREIGN TABLE f_ro_t (a int, b text) SERVER ro_srv OPTIONS (table ''ro_t'')';
  END IF;
END $$;

\echo '--- [1] SELECT via FDW on readonly server: expect 2 rows ---'
SELECT * FROM f_ro_t ORDER BY a;

\echo '--- [2] INSERT via FDW: expect planner error (table not updatable) ---'
INSERT INTO f_ro_t VALUES (3, chr(122));

\echo '--- [3] duckdb_execute SELECT: expect OK ---'
SELECT duckdb_execute('ro_srv', 'SELECT count(*) FROM ro_t');

\echo '--- [4] duckdb_execute DML: expect read-only rejection ---'
SELECT duckdb_execute('ro_srv', 'DELETE FROM ro_t');

\echo '--- [5] duckdb_execute comment-prefixed write: expect rejection ---'
SELECT duckdb_execute('ro_srv', '/* c */ UPDATE ro_t SET a=1');

\echo '--- [6] data unchanged, expect 2 rows ---'
SELECT * FROM f_ro_t ORDER BY a;

\echo '--- [7] second FDW query (cache reuse), expect 2 rows, no crash ---'
SELECT count(*) AS c FROM f_ro_t;

\echo '--- [8] crash regression: server WITHOUT user mapping must error cleanly, not segfault ---'
CREATE SERVER IF NOT EXISTS nomap_srv FOREIGN DATA WRAPPER duckdb_fdw
  OPTIONS (database '/home/lhy/g72/t.duckdb', force_readonly 'true');
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='f_nomap') THEN
    EXECUTE 'CREATE FOREIGN TABLE f_nomap (a int, b text) SERVER nomap_srv OPTIONS (table ''ro_t'')';
  END IF;
END $$;
SELECT count(*) FROM f_nomap;
\echo '--- [9] backend still alive? expect 1 ---'
SELECT 1 AS alive;
