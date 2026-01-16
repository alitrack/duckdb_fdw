--SET log_min_messages  TO DEBUG1;
--SET client_min_messages  TO DEBUG1;
--Testcase 16:
CREATE EXTENSION duckdb_fdw;
--Testcase 17:
CREATE SERVER duckdb_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test.db');
--Testcase 18:
CREATE FOREIGN TABLE multiprimary(a int, b int OPTIONS (key 'true'), c int OPTIONS(key 'true')) SERVER duckdb_svr;
-- test for aggregate pushdown
--Testcase 8:
DROP SERVER IF EXISTS duckdb_svr CASCADE;
--Testcase 9:
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;

--Testcase 10:
CREATE EXTENSION duckdb_fdw;
--Testcase 11:
CREATE SERVER duckdb_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test.db');
--Testcase 12:
CREATE FOREIGN TABLE multiprimary(a int, b int OPTIONS (key 'true'), c int OPTIONS(key 'true')) SERVER duckdb_svr;

--Testcase 1:
explain (costs off, verbose) select count(distinct a) from multiprimary;

--Testcase 2:
explain (costs off, verbose) select sum(b),max(b), min(b), avg(b) from multiprimary;

--Testcase 3:
explain (costs off, verbose) select sum(b+5)+2 from multiprimary group by b/2 order by b/2;

--Testcase 4:
explain (costs off, verbose) select sum(a) from multiprimary group by b having sum(a) > 0;

--Testcase 5:
explain (costs off, verbose) select sum(a) from multiprimary group by b having avg(a^2) > 0 and sum(a) > 0;

-- stddev and variance are not pushed down
--Testcase 6:
explain (costs off, verbose) select stddev(a) from multiprimary;
--Testcase 7:
explain (costs off, verbose) select sum(a) from multiprimary group by b having variance(a) > 0;

--Testcase 13:
DROP FOREIGN TABLE multiprimary;

--Testcase 16:
CREATE FOREIGN TABLE limittest(id serial OPTIONS (key 'true'), x int, y text) SERVER duckdb_svr;

--Testcase 17:
INSERT INTO limittest(x, y) VALUES (1, 'x'), (2, 'x'), (3, 'x'), (4, 'x');
--Testcase 18:
INSERT INTO limittest(x, y) VALUES (1, 'y'), (2, 'y'), (3, 'y'), (4, 'y');
--Testcase 19:
INSERT INTO limittest(x, y) VALUES (1, 'z'), (2, 'z'), (3, 'z'), (4, 'z');

--Testcase 20:
EXPLAIN VERBOSE 
SELECT avg(x) FROM limittest GROUP BY y ORDER BY 1 DESC FETCH FIRST 2 ROWS WITH TIES;
--Testcase 21:
SELECT avg(x) FROM limittest GROUP BY y ORDER BY 1 DESC FETCH FIRST 2 ROWS WITH TIES;

--Testcase 22:
EXPLAIN VERBOSE 
SELECT avg(x) FROM limittest WHERE  x >= 0 GROUP BY y ORDER BY 1 DESC FETCH FIRST 2 ROWS WITH TIES;
--Testcase 23:
SELECT avg(x) FROM limittest WHERE  x >= 0 GROUP BY y ORDER BY 1 DESC FETCH FIRST 2 ROWS WITH TIES;

--Testcase 24:
EXPLAIN VERBOSE 
SELECT x FROM limittest WHERE x > 0 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES;
--Testcase 25:
SELECT x FROM limittest WHERE x > 0 ORDER BY 1 FETCH FIRST 2 ROWS WITH TIES;

--Testcase 26:
EXPLAIN VERBOSE 
SELECT x FROM limittest ORDER BY 1 FETCH FIRST 2 ROWS ONLY;
--Testcase 27:
SELECT x FROM limittest ORDER BY 1 FETCH FIRST 2 ROWS ONLY;

--Testcase 28:
DROP FOREIGN TABLE limittest;

--Testcase 14:
DROP SERVER duckdb_svr;
--Testcase 15:
DROP EXTENSION duckdb_fdw CASCADE;

