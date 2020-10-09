--SET log_min_messages  TO DEBUG1;
--SET client_min_messages  TO DEBUG1;
CREATE EXTENSION duckdb_fdw;
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test.db');
CREATE FOREIGN TABLE multiprimary(a int, b int OPTIONS (key 'true'), c int OPTIONS(key 'true')) SERVER sqlite_svr;
-- test for aggregate pushdown
--Testcase 8:
DROP SERVER IF EXISTS sqlite_svr CASCADE;
--Testcase 9:
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;

--Testcase 10:
CREATE EXTENSION duckdb_fdw;
--Testcase 11:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test.db');
--Testcase 12:
CREATE FOREIGN TABLE multiprimary(a int, b int OPTIONS (key 'true'), c int OPTIONS(key 'true')) SERVER sqlite_svr;

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

--Testcase 14:
DROP SERVER sqlite_svr;
--Testcase 15:
DROP EXTENSION duckdb_fdw CASCADE;

