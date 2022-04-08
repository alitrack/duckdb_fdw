SET datestyle=ISO;
SET timezone='Japan';

--Testcase 1:
CREATE EXTENSION duckdb_fdw;
--Testcase 2:
CREATE SERVER server1 FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test_selectfunc.db');
--CREATE USER MAPPING FOR CURRENT_USER SERVER server1 OPTIONS(user 'user', password 'pass');

--IMPORT FOREIGN SCHEMA public FROM SERVER server1 INTO public OPTIONS(import_time_text 'false');
--Testcase 3:
CREATE FOREIGN TABLE s3(id text OPTIONS (key 'true'), time timestamp, tag1 text, value1 float, value2 int, value3 float, value4 int, str1 text, str2 text) SERVER server1;

-- s3 (value1 as float8, value2 as bigint)
--Testcase 4:
\d s3;
--Testcase 5:
SELECT * FROM s3;

-- select float8() (not pushdown, remove float8, explain)
-- EXPLAIN VERBOSE
-- SELECT float8(value1), float8(value2), float8(value3), float8(value4) FROM s3;
-- duckdb fdw does not support

-- select float8() (not pushdown, remove float8, result)
-- SELECT float8(value1), float8(value2), float8(value3), float8(value4) FROM s3;
-- duckdb fdw does not support

-- select sqrt (builtin function, explain)
-- EXPLAIN VERBOSE
-- SELECT sqrt(value1), sqrt(value2) FROM s3;
-- duckdb fdw does not have sqrt()

-- select sqrt (buitin function, result)
-- SELECT sqrt(value1), sqrt(value2) FROM s3;
-- duckdb fdw does not have sqrt()

-- select sqrt (builtin function,, not pushdown constraints, explain)
-- EXPLAIN VERBOSE
-- SELECT sqrt(value1), sqrt(value2) FROM s3 WHERE to_hex(value2) != '64';
-- duckdb fdw does not have sqrt()

-- select sqrt (builtin function, not pushdown constraints, result)
-- SELECT sqrt(value1), sqrt(value2) FROM s3 WHERE to_hex(value2) != '64';
-- duckdb fdw does not have sqrt()

-- select sqrt (builtin function, pushdown constraints, explain)
-- EXPLAIN VERBOSE
-- SELECT sqrt(value1), sqrt(value2) FROM s3 WHERE value2 != 200;
-- duckdb fdw does not have sqrt()

-- select sqrt (builtin function, pushdown constraints, result)
-- SELECT sqrt(value1), sqrt(value2) FROM s3 WHERE value2 != 200;
-- duckdb fdw does not have sqrt()

-- select abs (builtin function, explain)
--Testcase 6:
EXPLAIN VERBOSE
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM s3;

-- select abs (buitin function, result)
--Testcase 7:
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM s3;

-- select abs (builtin function, not pushdown constraints, explain)
--Testcase 8:
EXPLAIN VERBOSE
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM s3 WHERE to_hex(value2) != '64';

-- select abs (builtin function, not pushdown constraints, result)
--Testcase 9:
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM s3 WHERE to_hex(value2) != '64';

-- select abs (builtin function, pushdown constraints, explain)
--Testcase 10:
EXPLAIN VERBOSE
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM s3 WHERE value2 != 200;

-- select abs (builtin function, pushdown constraints, result)
--Testcase 11:
SELECT abs(value1), abs(value2), abs(value3), abs(value4) FROM s3 WHERE value2 != 200;

-- select log (builtin function, need to swap arguments, numeric cast, explain)
-- log_<base>(v) : postgresql (base, v), duckdb (v, base)
-- EXPLAIN VERBOSE
-- SELECT log(value1::numeric, value2::numeric) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (builtin function, need to swap arguments, numeric cast, result)
-- SELECT log(value1::numeric, value2::numeric) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (stub function, need to swap arguments, float8, explain)
-- EXPLAIN VERBOSE
-- SELECT log(value1, 0.1) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (stub function, need to swap arguments, float8, result)
-- SELECT log(value1, 0.1) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (stub function, need to swap arguments, bigint, explain)
-- EXPLAIN VERBOSE
-- SELECT log(value2, 3) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (stub function, need to swap arguments, bigint, result)
-- SELECT log(value2, 3) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (stub function, need to swap arguments, mix type, explain)
-- EXPLAIN VERBOSE
-- SELECT log(value1, value2) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log (stub function, need to swap arguments, mix type, result)
-- SELECT log(value1, value2) FROM s3 WHERE value1 != 1;
-- duckdb fdw does not have log()

-- select log2 (stub function, explain)
-- EXPLAIN VERBOSE
-- SELECT log2(value1),log2(value2) FROM s3;
-- duckdb fdw does not have log2()

-- select log2 (stub function, result)
-- SELECT log2(value1),log2(value2) FROM s3;
-- duckdb fdw does not have log2()

-- select spread (stub agg function, explain)
-- EXPLAIN VERBOSE
-- SELECT spread(value1),spread(value2),spread(value3),spread(value4) FROM s3;
-- duckdb fdw does not have spread()

-- select spread (stub agg function, result)
-- SELECT spread(value1),spread(value2),spread(value3),spread(value4) FROM s3;
-- duckdb fdw does not have spread()

-- select spread (stub agg function, raise exception if not expected type)
-- SELECT spread(value1::numeric),spread(value2::numeric),spread(value3::numeric),spread(value4::numeric) FROM s3;
-- duckdb fdw does not have spread()

-- select abs as nest function with agg (pushdown, explain)
--Testcase 12:
EXPLAIN VERBOSE
SELECT sum(value3),abs(sum(value3)) FROM s3;

-- select abs as nest function with agg (pushdown, result)
--Testcase 13:
SELECT sum(value3),abs(sum(value3)) FROM s3;

-- select abs as nest with log2 (pushdown, explain)
-- EXPLAIN VERBOSE
-- SELECT abs(log2(value1)),abs(log2(1/value1)) FROM s3;
-- duckdb fdw does not have log2()

-- select abs as nest with log2 (pushdown, result)
-- SELECT abs(log2(value1)),abs(log2(1/value1)) FROM s3;
-- duckdb fdw does not have log2()

-- select abs with non pushdown func and explicit constant (explain)
--Testcase 14:
EXPLAIN VERBOSE
SELECT abs(value3), pi(), 4.1 FROM s3;

-- select abs with non pushdown func and explicit constant (result)
--Testcase 15:
SELECT abs(value3), pi(), 4.1 FROM s3;

-- select sqrt as nest function with agg and explicit constant (pushdown, explain)
-- EXPLAIN VERBOSE
-- SELECT sqrt(count(value1)), pi(), 4.1 FROM s3;
-- duckdb fdw does not have sqrt()

-- select sqrt as nest function with agg and explicit constant (pushdown, result)
-- SELECT sqrt(count(value1)), pi(), 4.1 FROM s3;
-- duckdb fdw does not have sqrt()

-- select sqrt as nest function with agg and explicit constant and tag (error, explain)
-- EXPLAIN VERBOSE
-- SELECT sqrt(count(value1)), pi(), 4.1, tag1 FROM s3;
-- duckdb fdw does not have sqrt()

-- select spread (stub agg function and group by influx_time() and tag) (explain)
-- EXPLAIN VERBOSE
-- SELECT spread("value1"),influx_time(time, interval '1s'),tag1 FROM s3 WHERE time >= to_timestamp(0) and time <= to_timestamp(4) GROUP BY influx_time(time, interval '1s'), tag1;
-- duckdb fdw does not have spread() and influx_time()

-- select spread (stub agg function and group by influx_time() and tag) (result)
-- SELECT spread("value1"),influx_time(time, interval '1s'),tag1 FROM s3 WHERE time >= to_timestamp(0) and time <= to_timestamp(4) GROUP BY influx_time(time, interval '1s'), tag1;
-- duckdb fdw does not have spread() and influx_time()

-- select spread (stub agg function and group by tag only) (result)
-- SELECT tag1,spread("value1") FROM s3 WHERE time >= to_timestamp(0) and time <= to_timestamp(4) GROUP BY tag1;
-- duckdb fdw does not have spread()

-- select spread (stub agg function and other aggs) (result)
-- SELECT sum("value1"),spread("value1"),count("value1") FROM s3;
-- duckdb fdw does not have spread()

-- select abs with order by (explain)
--Testcase 16:
EXPLAIN VERBOSE
SELECT value1, abs(1-value1) FROM s3 order by abs(1-value1);

-- select abs with order by (result)
--Testcase 17:
SELECT value1, abs(1-value1) FROM s3 order by abs(1-value1);

-- select abs with order by index (result)
--Testcase 18:
SELECT value1, abs(1-value1) FROM s3 order by 2,1;

-- select abs with order by index (result)
--Testcase 19:
SELECT value1, abs(1-value1) FROM s3 order by 1,2;

-- select abs and as
--Testcase 20:
SELECT abs(value3) as abs1 FROM s3;

-- select spread over join query (explain)
-- EXPLAIN VERBOSE
-- SELECT spread(t1.value1), spread(t2.value1) FROM s3 t1 INNER JOIN s3 t2 ON (t1.value1 = t2.value1) where t1.value1 = 0.1;
-- duckdb fdw does not have spread()

-- select spread over join query (result, stub call error)
-- SELECT spread(t1.value1), spread(t2.value1) FROM s3 t1 INNER JOIN s3 t2 ON (t1.value1 = t2.value1) where t1.value1 = 0.1;
-- duckdb fdw does not have spread()

-- select spread with having (explain)
-- EXPLAIN VERBOSE
-- SELECT spread(value1) FROM s3 HAVING spread(value1) > 100;
-- duckdb fdw does not have spread()

-- select spread with having (explain, cannot pushdown, stub call error)
-- SELECT spread(value1) FROM s3 HAVING spread(value1) > 100;
-- duckdb fdw does not have spread()

-- select abs with arithmetic and tag in the middle (explain)
--Testcase 21:
EXPLAIN VERBOSE
SELECT abs(value1) + 1, value2, tag1, sqrt(value2) FROM s3;

-- select abs with arithmetic and tag in the middle (result)
--Testcase 22:
SELECT abs(value1) + 1, value2, tag1, sqrt(value2) FROM s3;

-- select with order by limit (explain)
--Testcase 23:
EXPLAIN VERBOSE
SELECT abs(value1), abs(value3), sqrt(value2) FROM s3 ORDER BY abs(value3) LIMIT 1;

-- select with order by limit (explain)
--Testcase 24:
SELECT abs(value1), abs(value3), sqrt(value2) FROM s3 ORDER BY abs(value3) LIMIT 1;

-- select mixing with non pushdown func (all not pushdown, explain)
--Testcase 25:
EXPLAIN VERBOSE
SELECT abs(value1), sqrt(value2), upper(tag1) FROM s3;

-- select mixing with non pushdown func (result)
--Testcase 26:
SELECT abs(value1), sqrt(value2), upper(tag1) FROM s3;

-- duckdb data prep

-- duckdb pushdown supported functions (explain)
--Testcase 27:
EXPLAIN VERBOSE
SELECT abs(value3), length(tag1), lower(str1), ltrim(str2), ltrim(str1, '-'), replace(str1, 'XYZ', 'ABC'), round(value3), rtrim(str1, '-'), rtrim(str2), substr(str1, 4), substr(str1, 4, 3) FROM s3;

-- duckdb pushdown supported functions (result)
--Testcase 28:
SELECT abs(value3), length(tag1), lower(str1), ltrim(str2), ltrim(str1, '-'), replace(str1, 'XYZ', 'ABC'), round(value3), rtrim(str1, '-'), rtrim(str2), substr(str1, 4), substr(str1, 4, 3) FROM s3;

-- duckdb pushdown nest functions (explain)
--Testcase 32:
EXPLAIN VERBOSE
SELECT round(abs(value2), 0) FROM s3;

-- duckdb pushdown nest functions (result)
--Testcase 33:
SELECT round(abs(value2), 0) FROM s3;

--Testcase 29:
DROP FOREIGN TABLE s3;
--Testcase 30:
DROP SERVER server1;
--Testcase 31:
DROP EXTENSION duckdb_fdw;
