--
-- SELECT_HAVING
--

--Testcase 22:
CREATE EXTENSION duckdb_fdw;
--Testcase 23:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');
--Testcase 24:
CREATE FOREIGN TABLE test_having(a int OPTIONS (key 'true'), b int, c char(8), d char) SERVER sqlite_svr;

-- load test data
--Testcase 1:
INSERT INTO test_having VALUES (0, 1, 'XXXX', 'A');
--Testcase 2:
INSERT INTO test_having VALUES (1, 2, 'AAAA', 'b');
--Testcase 3:
INSERT INTO test_having VALUES (2, 2, 'AAAA', 'c');
--Testcase 4:
INSERT INTO test_having VALUES (3, 3, 'BBBB', 'D');
--Testcase 5:
INSERT INTO test_having VALUES (4, 3, 'BBBB', 'e');
--Testcase 6:
INSERT INTO test_having VALUES (5, 3, 'bbbb', 'F');
--Testcase 7:
INSERT INTO test_having VALUES (6, 4, 'cccc', 'g');
--Testcase 8:
INSERT INTO test_having VALUES (7, 4, 'cccc', 'h');
--Testcase 9:
INSERT INTO test_having VALUES (8, 4, 'CCCC', 'I');
--Testcase 10:
INSERT INTO test_having VALUES (9, 4, 'CCCC', 'j');

--Testcase 11:
SELECT b, c FROM test_having
	GROUP BY b, c HAVING count(*) = 1 ORDER BY b, c;

-- HAVING is effectively equivalent to WHERE in this case
--Testcase 12:
SELECT b, c FROM test_having
	GROUP BY b, c HAVING b = 3 ORDER BY b, c collate "en_US.utf8";

--Testcase 13:
SELECT lower(c), count(c) FROM test_having
	GROUP BY lower(c) HAVING count(*) > 2 OR min(a) = max(a)
	ORDER BY lower(c);

--Testcase 14:
SELECT c, max(a) FROM test_having
	GROUP BY c HAVING count(*) > 2 OR min(a) = max(a)
	ORDER BY c collate "en_US.utf8";

-- test degenerate cases involving HAVING without GROUP BY
-- Per SQL spec, these should generate 0 or 1 row, even without aggregates

--Testcase 15:
SELECT min(a), max(a) FROM test_having HAVING min(a) = max(a);
--Testcase 16:
SELECT min(a), max(a) FROM test_having HAVING min(a) < max(a);

-- errors: ungrouped column references
--Testcase 17:
SELECT a FROM test_having HAVING min(a) < max(a);
--Testcase 18:
SELECT 1 AS one FROM test_having HAVING a > 1;

-- the really degenerate case: need not scan table at all
--Testcase 19:
SELECT 1 AS one FROM test_having HAVING 1 > 2;
--Testcase 20:
SELECT 1 AS one FROM test_having HAVING 1 < 2;

-- and just to prove that we aren't scanning the table:
--Testcase 21:
SELECT 1 AS one FROM test_having WHERE 1/a = 1 HAVING 1 < 2;

--Testcase 25:
DROP FOREIGN TABLE test_having;
--Testcase 26:
DROP SERVER sqlite_svr;
--Testcase 27:
DROP EXTENSION duckdb_fdw CASCADE;
