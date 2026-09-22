\set QUIET off
\pset footer off
-- Definitive repro: a remote column that is genuinely 16-byte HUGEINT in
-- DuckDB, scanned through the FDW as a PG bigint.  Values are i+1 (never 0)
-- across 2000 rows (spans multiple 2048-vectors/chunks).
--
-- If the chunk reader walks a 16-byte HUGEINT vector with an 8-byte stride,
-- even-indexed rows read the low 64 bits (correct) and odd-indexed rows read
-- the high 64 bits (= 0 for these small values).  Signature: ~half the rows
-- come back 0.

\echo '=== setup remote hugeint table (2000 rows, values i+1) ==='
SELECT duckdb_execute('duck_srv', 'DROP TABLE IF EXISTS hi; CREATE TABLE hi AS SELECT i+1 AS x FROM range(2000) t(i); ALTER TABLE hi ADD COLUMN hi HUGEINT; UPDATE hi SET hi = x::HUGEINT;');

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname='f_hi') THEN
    EXECUTE 'CREATE FOREIGN TABLE f_hi (x bigint, hi bigint) SERVER duck_srv OPTIONS (table ''hi'')';
  END IF;
END $$;

\echo '=== first 12 rows via FDW (expected: 1 2 3 4 5 6 7 8 9 10 11 12) ==='
SELECT x, hi FROM f_hi ORDER BY x LIMIT 12;

\echo '=== how many rows came back 0? (correct answer: 0) ==='
SELECT count(*) AS zeros FROM f_hi WHERE hi = 0;

\echo '=== total rows (expected 2000) ==='
SELECT count(*) AS total FROM f_hi;

\echo '=== differential: chunk(bigint) vs text path, first 16 ==='
SELECT x, hi AS chunk_read,
       (SELECT (f.hi)::text FROM (SELECT hi FROM f_hi f ORDER BY x LIMIT 1 OFFSET 0) s WHERE false) AS ignore
FROM f_hi ORDER BY x LIMIT 16;
