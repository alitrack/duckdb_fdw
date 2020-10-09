--
-- TIMESTAMP
--
CREATE EXTENSION duckdb_fdw;
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');
CREATE FOREIGN TABLE dates1 (
	name varchar(20),
	date_as_text timestamp without time zone,
	date_as_number timestamp without time zone OPTIONS (column_type 'INT'))
SERVER sqlite_svr
OPTIONS (table 'dates');

CREATE FOREIGN TABLE dates2 (
	name varchar(20),
	date_as_text timestamp without time zone,
	date_as_number double precision)
SERVER sqlite_svr
OPTIONS (table 'dates');

-- Showing timestamp column from SQLite value as TEXT and as INTEGER/FLOAT has same value
SELECT name,
	to_char(date_as_text, 	'YYYY-MM-DD HH24:MI:SS.MS') as date_as_text, 
	to_char(date_as_number, 'YYYY-MM-DD HH24:MI:SS.MS') as date_as_number
FROM dates1;
SELECT * FROM dates2;

-- Comparing exact values showing same results even comparing to a text source sqlite column or numerical source sqlite column
SELECT * FROM dates1
WHERE date_as_text = to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

SELECT * FROM dates1
WHERE date_as_number = to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

SELECT * FROM dates1
WHERE date_as_text = to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

SELECT * FROM dates1
WHERE date_as_number = to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

-- Comparing greater values showing same results even comparing to a text source sqlite column or numerical source sqlite column
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29.000', 'YYYY-MM-DD HH24:MI:SS.MS');

SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > to_timestamp('2020-05-10 10:45:29', 'YYYY-MM-DD HH24:MI:SS.MS');

--- Comparing without using to_timestamp
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29.000')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29.000')::timestamp);

SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29.000')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29.000')::timestamp);

SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text = (('2020-05-10 10:45:29')::timestamp);

SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number = (('2020-05-10 10:45:29')::timestamp);

-- Comparing greater values  without using to_timestamp


SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29.000')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29.000')::timestamp);

SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29.000')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29.000')::timestamp);

SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_text > (('2020-05-10 10:45:29')::timestamp);

SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29')::timestamp);

explain (verbose, costs off)
SELECT * FROM dates1
WHERE date_as_number > (('2020-05-10 10:45:29')::timestamp);

DROP FOREIGN TABLE dates1;
DROP FOREIGN TABLE dates2;
DROP SERVER sqlite_svr;
DROP EXTENSION duckdb_fdw CASCADE;
