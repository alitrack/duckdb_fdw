--SET log_min_messages  TO DEBUG1;
--SET client_min_messages  TO DEBUG1;
--Testcase 44:
CREATE EXTENSION duckdb_fdw;
--Testcase 45:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test.db');

--Testcase 46:
CREATE SERVER sqlite2 FOREIGN DATA WRAPPER duckdb_fdw;

IMPORT FOREIGN SCHEMA public FROM SERVER sqlite_svr INTO public;

--Testcase 1:
INSERT INTO "type_STRING"(col) VALUES ('string');
--Testcase 2:
INSERT INTO "type_BOOLEAN"(col) VALUES (TRUE);
--Testcase 3:
INSERT INTO "type_BOOLEAN"(col) VALUES (FALSE);
--Testcase 4:
INSERT INTO "type_BYTE"(col) VALUES ('c');
--Testcase 5:
INSERT INTO "type_SINT"(col) VALUES (32767);
--Testcase 6:
INSERT INTO "type_SINT"(col) VALUES (-32768);
--Testcase 7:
INSERT INTO "type_BINT"(col) VALUES (9223372036854775807);
--Testcase 8:
INSERT INTO "type_BINT"(col) VALUES (-9223372036854775808);
--Testcase 9:
INSERT INTO "type_INTEGER"(col) VALUES (9223372036854775807);

--Testcase 10:
INSERT INTO "type_FLOAT"(col) VALUES (3.1415);
--Testcase 11:
INSERT INTO "type_DOUBLE"(col) VALUES (3.14159265);
--Testcase 12:
INSERT INTO "type_TIMESTAMP" VALUES ('2017.11.06 12:34:56.789', '2017.11.06');
--Testcase 13:
INSERT INTO "type_TIMESTAMP" VALUES ('2017.11.06 1:3:0', '2017.11.07');
--Testcase 14:
INSERT INTO "type_BLOB"(col) VALUES (bytea('\xDEADBEEF'));
--Testcase 15:
INSERT INTO typetest VALUES(1,'a', 'b', 'c','2017.11.06 12:34:56.789', '2017.11.06 12:34:56.789' ) ;

--Testcase 16:
SELECT * FROM "type_STRING";
--Testcase 17:
SELECT * FROM "type_BOOLEAN";
--Testcase 18:
SELECT * FROM "type_BYTE";
--Testcase 19:
SELECT * FROM "type_SINT";
--Testcase 20:
SELECT * FROM "type_BINT";
--Testcase 21:
SELECT * FROM "type_INTEGER";
--Testcase 22:
SELECT * FROM "type_FLOAT";
--Testcase 23:
SELECT * FROM "type_DOUBLE";
set datestyle=ISO;
--Testcase 24:
SELECT * FROM "type_TIMESTAMP";
--Testcase 25:
SELECT * FROM "type_BLOB";
--Testcase 26:
SELECT * FROM typetest;

--Testcase 27:
insert into "type_STRING" values('TYPE');
--Testcase 28:
insert into "type_STRING" values('type');

-- not pushdown
--Testcase 29:
SELECT  *FROM "type_STRING" WHERE col like 'TYP%';
--Testcase 30:
EXPLAIN SELECT  *FROM "type_STRING" WHERE col like 'TYP%';
-- pushdown
--Testcase 31:
SELECT  *FROM "type_STRING" WHERE col ilike 'typ%';
--Testcase 32:
EXPLAIN SELECT  *FROM "type_STRING" WHERE col ilike 'typ%';

--Testcase 33:
SELECT  *FROM "type_STRING" WHERE col ilike 'typ%' and col like 'TYPE';
--Testcase 34:
EXPLAIN SELECT  *FROM "type_STRING" WHERE col ilike 'typ%' and col like 'TYPE';

--Testcase 35:
SELECT * FROM "type_TIMESTAMP";

--Testcase 36:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM  "type_TIMESTAMP" WHERE col > date ('2017.11.06 12:34:56.789') ;
--Testcase 37:
SELECT * FROM  "type_TIMESTAMP" WHERE col > date ('2017.11.06 12:34:56.789') ;

--Testcase 38:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM  "type_TIMESTAMP" WHERE col::text > date ('2017.11.06 12:34:56.789')::text ;
--Testcase 39:
SELECT * FROM  "type_TIMESTAMP" WHERE col::text > date ('2017.11.06 12:34:56.789')::text ;

--Testcase 40:
EXPLAIN  (VERBOSE, COSTS OFF) SELECT * FROM  "type_TIMESTAMP" WHERE col > b - interval '1 hour'; 
--Testcase 41:
SELECT * FROM  "type_TIMESTAMP" WHERE col > b - interval '1 hour';

--Testcase 42:
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM  "type_TIMESTAMP" WHERE col > b;
--Testcase 43:
SELECT * FROM  "type_TIMESTAMP" WHERE col > b;

DROP EXTENSION duckdb_fdw CASCADE;
