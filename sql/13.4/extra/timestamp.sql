--
-- TIMESTAMP
--
--Testcase 1:
CREATE EXTENSION duckdb_fdw;
--Testcase 2:
CREATE SERVER duckdb_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdbfdw_test_core.db');
--Testcase 3:
CREATE FOREIGN TABLE dates1 (
	name varchar(20),
	date_as_text timestamp without time zone,
	date_as_number timestamp without time zone OPTIONS (column_type 'INT'))
SERVER duckdb_svr
OPTIONS (table 'dates');

--Testcase 4:
CREATE FOREIGN TABLE dates2 (
	name varchar(20),
	date_as_text timestamp without time zone,
	date_as_number double precision)
SERVER duckdb_svr
OPTIONS (table 'dates');

-- Showing timestamp column from DuckDB value as TEXT and as INTEGER/FLOAT has same value
--Testcase 5:
SELECT name,
	to_char(date_as_text, 	'YYYY-MM-DD HH24:MI:SS.MS') as date_as_text, 
	to_char(date_as_number, 'YYYY-MM-DD HH24:MI:SS.MS') as date_as_number
FROM dates1;
--Testcase 6:
SELECT * FROM dates2;

-- Comparing exact values showing same results even comparing to a text source duckdb column or numerical source duckdb column
--Testcase 7:
SELECT * FROM dates1
WHERE date_as_text = to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 8:
SELECT * FROM dates1
WHERE date_as_number = to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 9:
SELECT * FROM dates1
WHERE date_as_text = to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 10:
SELECT * FROM dates1
WHERE date_as_number = to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

-- Comparing greater values showing same results even comparing to a text source duckdb column or numerical source duckdb column
--Testcase 11:
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 12:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 13:
SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 14:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 15:
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 16:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 17:
SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

--Testcase 18:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

--- Comparing without using to_timestamp
--Testcase 19:
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 20:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 21:
SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 22:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 23:
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29')::timestamp);

--Testcase 24:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29')::timestamp);

--Testcase 25:
SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29')::timestamp);

--Testcase 26:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29')::timestamp);

-- Comparing greater values  without using to_timestamp


--Testcase 27:
SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 28:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 29:
SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 30:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29.000')::timestamp);

--Testcase 31:
SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29')::timestamp);

--Testcase 32:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29')::timestamp);

--Testcase 33:
SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29')::timestamp);

--Testcase 34:
explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29')::timestamp);

--Testcase 35:
DROP FOREIGN TABLE dates1;
--Testcase 36:
DROP FOREIGN TABLE dates2;
--Testcase 37:
DROP SERVER duckdb_svr;
--Testcase 38:
DROP EXTENSION duckdb_fdw CASCADE;
