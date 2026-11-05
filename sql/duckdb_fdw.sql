-- Basic Extension Setup
CREATE EXTENSION duckdb_fdw;

-- Create Server
CREATE SERVER duckdb_test FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdb_fdw_regress_main.db');
CREATE USER MAPPING FOR PUBLIC SERVER duckdb_test;

-- Admin helper privilege defaults
CREATE ROLE duckdb_fdw_unprivileged;
SELECT has_function_privilege('duckdb_fdw_unprivileged', 'duckdb_execute(name,text)', 'EXECUTE');
SELECT has_function_privilege('duckdb_fdw_unprivileged', 'duckdb_create_s3_secret(name,text,text,text,text)', 'EXECUTE');

-- Test version function
SELECT duckdb_fdw_version() IS NOT NULL;

-- Create a table in DuckDB via DDL function
SELECT duckdb_execute('duckdb_test', 'DROP TABLE IF EXISTS test_types');
SELECT duckdb_execute('duckdb_test', 'CREATE TABLE test_types (i INTEGER, j BIGINT, d DOUBLE, s VARCHAR)');
SELECT duckdb_execute('duckdb_test', 'INSERT INTO test_types VALUES (1, 100, 3.14, ''hello''), (2, 200, 6.28, ''world'')');

-- Map it to PostgreSQL
CREATE FOREIGN TABLE test_types (
    i INT4,
    j INT8,
    d FLOAT8,
    s TEXT
) SERVER duckdb_test OPTIONS (table 'test_types');

-- Basic Query
SELECT * FROM test_types ORDER BY i;

-- New Types Support Test (UUID, Decimal, Date, Timestamp)
SELECT duckdb_execute('duckdb_test', 'DROP TABLE IF EXISTS test_v2');
SELECT duckdb_execute('duckdb_test', 'CREATE TABLE test_v2 (
    u UUID, 
    d DECIMAL(18,3), 
    dt DATE, 
    ts TIMESTAMP,
    tz TIMESTAMPTZ,
    b BOOLEAN
)');

SELECT duckdb_execute('duckdb_test', 'INSERT INTO test_v2 VALUES (
    ''550e8400-e29b-41d4-a716-446655440000'', 
    12345.678, 
    ''2024-01-01'', 
    ''2024-01-01 12:34:56'',
    NULL,
    true
)');

CREATE FOREIGN TABLE test_v2 (
    u UUID,
    d NUMERIC(18,3),
    dt DATE,
    ts TIMESTAMP,
    tz TIMESTAMPTZ,
    b BOOL
) SERVER duckdb_test OPTIONS (table 'test_v2');

SELECT * FROM test_v2;

-- Appender API Test (INSERT)
INSERT INTO test_types (i, j, d, s) VALUES (3, 300, 9.42, 'appender');
SELECT * FROM test_types WHERE i = 3;

-- Batch Insert API Test (COPY)
CREATE TABLE local_data (i int, j bigint, d double precision, s text);
INSERT INTO local_data SELECT g, g*10, g*1.1, 'str' || g FROM generate_series(4, 10) g;
COPY test_types FROM PROGRAM 'cat' (FORMAT CSV); -- This doesn't work directly like this for FDW
-- Better: INSERT INTO ... SELECT
INSERT INTO test_types SELECT * FROM local_data;
SELECT count(*) FROM test_types;

-- Advanced Pushdown Test
EXPLAIN (VERBOSE, COSTS OFF) 
SELECT stddev(i), variance(j), log(d) FROM test_types GROUP BY s;

-- Filter pushdown test
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM test_types WHERE i = 1;
SELECT * FROM test_types WHERE i = 1;

-- Server option refresh should use a new DuckDB database after ALTER SERVER
CREATE SERVER duckdb_switch FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database 'duckdb_switch_one.db');
CREATE USER MAPPING FOR PUBLIC SERVER duckdb_switch;
SELECT duckdb_execute('duckdb_switch', 'DROP TABLE IF EXISTS switch_test');
SELECT duckdb_execute('duckdb_switch', 'CREATE TABLE switch_test (i INTEGER)');
SELECT duckdb_execute('duckdb_switch', 'INSERT INTO switch_test VALUES (1)');
ALTER SERVER duckdb_switch OPTIONS (SET database 'duckdb_switch_two.db');
SELECT duckdb_execute('duckdb_switch', 'DROP TABLE IF EXISTS switch_test');
SELECT duckdb_execute('duckdb_switch', 'CREATE TABLE switch_test (i INTEGER)');
SELECT duckdb_execute('duckdb_switch', 'INSERT INTO switch_test VALUES (2)');
CREATE FOREIGN TABLE switch_test_ft (
    i INT4
) SERVER duckdb_switch OPTIONS (table 'switch_test');
SELECT * FROM switch_test_ft;
DROP FOREIGN TABLE switch_test_ft;
DROP SERVER duckdb_switch CASCADE;

-- Unsupported write paths remain explicit
UPDATE test_types SET s = 'changed' WHERE i = 1;
DELETE FROM test_types WHERE i = 1;

-- Division pushdown: integer division truncates; real and numeric keep fractions.
SELECT duckdb_execute('duckdb_test',
    'CREATE OR REPLACE TABLE division_inputs (i INTEGER, j SMALLINT, r FLOAT);
     INSERT INTO division_inputs VALUES (5, -5, 1), (-2, 5, 0),
                                       (NULL, NULL, NULL)');
CREATE FOREIGN TABLE division_inputs (i integer, j smallint, r real)
SERVER duckdb_test;

-- bigint / integer: sum(i) = 3, so division by 2 and -2 gives 1 and -1.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(i) / 2 FROM division_inputs;
SELECT sum(i) / 2 AS positive_divisor, sum(i) / (-2) AS negative_divisor
FROM division_inputs;

-- smallint / integer and smallint / smallint: -5 / 2 = -2, 5 / 2 = 2.
SELECT min(j / 2) AS min_div, max(j / 2) AS max_div,
       min(j / 2::smallint) AS smallint_div FROM division_inputs;

-- Floating-point division keeps /: sum(r) = 1, so 1 / 2 = 0.5.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(r) / 2::real FROM division_inputs;
SELECT sum(r) / 2::real AS real_div FROM division_inputs;

-- Nested integer domains resolve to the built-in integer division operator.
CREATE DOMAIN division_integer AS integer;
CREATE DOMAIN nested_division_integer AS division_integer;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(i::nested_division_integer / 2) FROM division_inputs;
SELECT sum(i::nested_division_integer / 2) AS domain_div FROM division_inputs;
DROP DOMAIN nested_division_integer;
DROP DOMAIN division_integer;

-- PG numeric over a DuckDB INTEGER keeps /: 5 / 2 = 2.5.
CREATE FOREIGN TABLE numeric_division(i numeric)
SERVER duckdb_test OPTIONS (table 'division_inputs');
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(i / 2) FROM numeric_division WHERE i = 5;
SELECT sum(i / 2) AS numeric_div FROM numeric_division WHERE i = 5;
DROP FOREIGN TABLE numeric_division;

-- Remote division by zero yields NULL: the aggregate is NULL, the filter matches no rows.
SELECT max(i / (j - j)) AS zero_div FROM division_inputs;
SELECT i FROM division_inputs WHERE i / (j - j) > 0;

-- MATERIALIZED makes PG evaluate the division locally and raise division by zero.
WITH scanned AS MATERIALIZED (SELECT i, j FROM division_inputs)
SELECT i FROM scanned WHERE i / (j - j) > 0;

DROP FOREIGN TABLE division_inputs;
SELECT duckdb_execute('duckdb_test', 'DROP TABLE division_inputs');

-- Cast pushdown preserves PostgreSQL operand types and precision.
SELECT duckdb_execute('duckdb_test',
    'CREATE OR REPLACE TABLE expression_results (i INTEGER, j SMALLINT, r FLOAT);
     INSERT INTO expression_results VALUES (16777217, 5, 0), (NULL, NULL, NULL)');
CREATE FOREIGN TABLE expression_results (i integer, j smallint, r real)
SERVER duckdb_test;

-- Function-call casts: float8(i) = 16777217, int4(j) = 5; count includes NULLs.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(float8(i)), min(int4(j)), count(*) FROM expression_results;
SELECT sum(float8(i)) AS cast_sum, min(int4(j)) AS cast_min, count(*) AS rows
FROM expression_results;

-- PG promotes i to double: 0::real + 16777217 = 16777217 (FLOAT rounds to 16777216).
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(r + i) FROM expression_results;
SELECT sum(r + i) AS implicit_cast_sum FROM expression_results;

-- Unbounded numeric casts execute locally: both spellings return 16777217.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(i::numeric) FROM expression_results;
SELECT sum(i::numeric) AS numeric_cast,
       sum(pg_catalog.numeric(i)) AS numeric_call FROM expression_results;

DROP FOREIGN TABLE expression_results;
SELECT duckdb_execute('duckdb_test', 'DROP TABLE expression_results');

-- Unsupported cast pairs execute locally; timestamp-to-time still pushes down.
SET TIME ZONE 'UTC';
SELECT duckdb_execute('duckdb_test',
    'CREATE OR REPLACE TABLE cast_pairs
         (i INTEGER, tm TIME, iv INTERVAL, ts TIMESTAMP, tz TIMESTAMPTZ);
     INSERT INTO cast_pairs VALUES
       (1, ''02:03:04'', INTERVAL ''02:03:04'',
        ''2024-01-02 02:03:04'', ''2024-01-02 02:03:04+00''),
       (2, NULL, NULL, NULL, NULL)');
CREATE FOREIGN TABLE cast_pairs
    (i integer, tm time, iv interval, ts timestamp, tz timestamptz)
SERVER duckdb_test;

-- TIME -> INTERVAL executes locally and returns 2 hours 3 mins 4 secs.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(tm::interval) FROM cast_pairs;
SELECT min(tm::interval) FROM cast_pairs;

-- Only ts::time pushes down; local iv::time and tz::time also match row 1.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT i FROM cast_pairs
WHERE iv::time = time '02:03:04'
  AND tz::time = time '02:03:04'
  AND ts::time = time '02:03:04';
SELECT i FROM cast_pairs
WHERE iv::time = time '02:03:04'
  AND tz::time = time '02:03:04'
  AND ts::time = time '02:03:04';

DROP FOREIGN TABLE cast_pairs;
SELECT duckdb_execute('duckdb_test', 'DROP TABLE cast_pairs');
RESET TIME ZONE;

-- Regression: text values that look like DuckDB lists must be returned
-- unchanged on read; only array-typed columns get the [..] -> {..} rewrite.
-- (A DuckDB list round-trips as a PG array; a VARCHAR that merely starts
-- with '[' and ends with ']' must NOT be converted.)
SELECT duckdb_execute('duckdb_test', 'DROP TABLE IF EXISTS text_bracket');
SELECT duckdb_execute('duckdb_test', 'CREATE TABLE text_bracket (id INTEGER, a INTEGER[], s VARCHAR)');
SELECT duckdb_execute('duckdb_test', 'INSERT INTO text_bracket VALUES (1, [1,2,3], ''[not an array]''), (2, [7], ''[Description: No-op correction applied to line 42]'')');

CREATE FOREIGN TABLE text_bracket_ft (
    id INT4,
    a INT4[],
    s TEXT
) SERVER duckdb_test OPTIONS (table 'text_bracket');

SELECT * FROM text_bracket_ft ORDER BY id;

DROP FOREIGN TABLE text_bracket_ft;

-- BLOB conversions
SELECT duckdb_execute('duckdb_test',
    'CREATE OR REPLACE TABLE blob_rows(i INTEGER, b BLOB);
     INSERT INTO blob_rows VALUES
       (1, from_hex(''0000275CFF'')), (2, ''''::BLOB), (3, NULL)');
CREATE FOREIGN TABLE blob_rows(i integer, b bytea) SERVER duckdb_test;
SELECT i, encode(b, 'hex'), octet_length(b) FROM blob_rows ORDER BY i;

-- BLOB constants must retain their type and bytes when pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT i FROM blob_rows WHERE b = '\x0000275cff'::bytea;
SELECT i FROM blob_rows WHERE b = '\x0000275cff'::bytea;
SELECT i FROM blob_rows WHERE b = ''::bytea;
SELECT i FROM blob_rows WHERE b = '\xaaaa'::bytea;
SELECT i FROM blob_rows WHERE b <> '\x0000275cff'::bytea ORDER BY i;
SET bytea_output = 'escape';
SELECT i FROM blob_rows WHERE b = '\x0000275cff'::bytea;
SELECT i FROM blob_rows WHERE b = ''::bytea;
RESET bytea_output;

SELECT l.i, l.b FROM blob_rows l JOIN blob_rows r ON l.i = r.i ORDER BY l.i;
WITH grouped AS MATERIALIZED (
    SELECT b, count(*) AS n FROM blob_rows GROUP BY b
)
SELECT encode(b, 'hex'), b IS NULL, n FROM grouped ORDER BY b NULLS LAST;

CREATE DOMAIN binary_value AS bytea;
CREATE DOMAIN nested_binary_value AS binary_value;
CREATE FOREIGN TABLE blob_domains(i integer, b nested_binary_value)
SERVER duckdb_test OPTIONS (table 'blob_rows');
SELECT i, encode(b, 'hex'), b IS NULL FROM blob_domains ORDER BY i;
DROP FOREIGN TABLE blob_domains;
DROP DOMAIN nested_binary_value, binary_value;
DROP FOREIGN TABLE blob_rows;

-- Date conversions
SET datestyle = 'ISO, YMD';

SELECT duckdb_execute('duckdb_test',
    'INSERT INTO test_v2 (dt) VALUES (''infinity''), (''-infinity'')');
SELECT dt FROM test_v2 ORDER BY dt;
SELECT u, dt FROM test_v2 ORDER BY dt;

INSERT INTO test_v2 (dt) VALUES ('infinity');
SELECT dt FROM test_v2 ORDER BY dt;

SET plan_cache_mode = force_generic_plan;
PREPARE date_filter(date) AS
SELECT dt FROM test_v2 WHERE dt = $1 ORDER BY dt;
EXECUTE date_filter('infinity');
EXECUTE date_filter('-infinity');
EXECUTE date_filter(NULL);
DEALLOCATE date_filter;
RESET plan_cache_mode;

-- Timestamp conversions
SET TIME ZONE 'UTC';

SELECT duckdb_execute('duckdb_test',
    'INSERT INTO test_v2 (ts, tz) VALUES
       (''infinity'', ''infinity''),
       (''-infinity'', ''-infinity''),
       (''2026-10-29 12:34:56.123456'',
        TIMESTAMPTZ ''2026-10-29 12:34:56.123456+08'')');
SELECT ts, tz FROM test_v2 WHERE ts IS NOT NULL ORDER BY ts;
SELECT u, ts, tz FROM test_v2 WHERE ts IS NOT NULL ORDER BY ts;

INSERT INTO test_v2 (ts, tz) VALUES ('-infinity', 'infinity');
SELECT ts, tz FROM test_v2 WHERE ts IS NOT NULL ORDER BY ts, tz;

SET plan_cache_mode = force_generic_plan;
PREPARE timestamp_filter(timestamp) AS
SELECT ts FROM test_v2 WHERE ts = $1 ORDER BY ts;
EXECUTE timestamp_filter('infinity');
EXECUTE timestamp_filter('-infinity');
EXECUTE timestamp_filter(NULL);
EXECUTE timestamp_filter('294247-01-10 04:00:54.775807');
DEALLOCATE timestamp_filter;

PREPARE timestamptz_filter(timestamptz) AS
SELECT tz FROM test_v2 WHERE tz = $1 ORDER BY tz;
EXECUTE timestamptz_filter('infinity');
EXECUTE timestamptz_filter('-infinity');
EXECUTE timestamptz_filter('294247-01-10 04:00:54.775807+00');

BEGIN;
SELECT duckdb_execute('duckdb_test', 'SET TimeZone = ''Asia/Shanghai''');
EXECUTE timestamptz_filter('2026-10-29 04:34:56.123456+00');
COMMIT;

DEALLOCATE timestamptz_filter;
RESET plan_cache_mode;
INSERT INTO test_v2 (ts) VALUES ('294247-01-10 04:00:54.775807');
INSERT INTO test_v2 (tz) VALUES ('294247-01-10 04:00:54.775807+00');
RESET TIME ZONE;

-- Cleanup
DROP FOREIGN TABLE test_types;
DROP ROLE duckdb_fdw_unprivileged;
DROP SERVER duckdb_test CASCADE;
DROP EXTENSION duckdb_fdw;
