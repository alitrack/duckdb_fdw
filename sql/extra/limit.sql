--
-- LIMIT
-- Check the LIMIT/OFFSET feature of SELECT
--
--Testcase 27:
CREATE EXTENSION duckdb_fdw;
--Testcase 28:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');
--Testcase 29:
CREATE FOREIGN TABLE onek(
	unique1		int4 OPTIONS (key 'true'),
	unique2		int4,
	two 		int4,
	four		int4,
	ten 		int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd     	int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) SERVER sqlite_svr;

--Testcase 30:
CREATE FOREIGN TABLE int8_tbl(q1 int8 OPTIONS (key 'true'), q2 int8) SERVER sqlite_svr;
--Testcase 31:
CREATE FOREIGN TABLE INT8_TMP(
        q1 int8,
        q2 int8,
        q3 int4,
        q4 int2,
        q5 text,
        id int options (key 'true')
) SERVER sqlite_svr;

--Testcase 32:
CREATE FOREIGN TABLE tenk1 (
	unique1		int4 OPTIONS (key 'true'),
	unique2		int4,
	two 		int4,
	four		int4,
	ten 		int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd     	int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
) SERVER sqlite_svr;

CREATE TABLE parent_table (
	unique1		int4 PRIMARY KEY,
	unique2		int4,
	two 		int4,
	four		int4,
	ten 		int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE FOREIGN table inherited_table ()
INHERITS (parent_table)
SERVER sqlite_svr options (table 'tenk1');

--Testcase 1:
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		ORDER BY unique1 LIMIT 2;
--Testcase 2:
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60
		ORDER BY unique1 LIMIT 5;
--Testcase 3:
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 60 AND unique1 < 63
		ORDER BY unique1 LIMIT 5;
--Testcase 4:
SELECT ''::text AS three, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 100
		ORDER BY unique1 LIMIT 3 OFFSET 20;
--Testcase 5:
SELECT ''::text AS zero, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 8 OFFSET 99;
--Testcase 6:
SELECT ''::text AS eleven, unique1, unique2, stringu1
		FROM onek WHERE unique1 < 50
		ORDER BY unique1 DESC LIMIT 20 OFFSET 39;
--Testcase 7:
SELECT ''::text AS ten, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990;
--Testcase 8:
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 OFFSET 990 LIMIT 5;
--Testcase 9:
SELECT ''::text AS five, unique1, unique2, stringu1
		FROM onek
		ORDER BY unique1 LIMIT 5 OFFSET 900;

-- Test null limit and offset.  The planner would discard a simple null
-- constant, so to ensure executor is exercised, do this:
--Testcase 10:
select * from int8_tbl limit (case when random() < 0.5 then null::bigint end);
--Testcase 11:
select * from int8_tbl offset (case when random() < 0.5 then null::bigint end);

-- Test assorted cases involving backwards fetch from a LIMIT plan node
begin;

declare c1 scroll cursor for select * from int8_tbl order by q1 limit 10;
--Testcase 12:
fetch all in c1;
--Testcase 13:
fetch 1 in c1;
--Testcase 14:
fetch backward 1 in c1;
--Testcase 33:
fetch backward all in c1;
--Testcase 34:
fetch backward 1 in c1;
--Testcase 35:
fetch all in c1;

declare c2 scroll cursor for select * from int8_tbl limit 3;
--Testcase 36:
fetch all in c2;
--Testcase 37:
fetch 1 in c2;
--Testcase 38:
fetch backward 1 in c2;
--Testcase 39:
fetch backward all in c2;
--Testcase 40:
fetch backward 1 in c2;
--Testcase 41:
fetch all in c2;

declare c3 scroll cursor for select * from int8_tbl offset 3;
--Testcase 42:
fetch all in c3;
--Testcase 43:
fetch 1 in c3;
--Testcase 44:
fetch backward 1 in c3;
--Testcase 45:
fetch backward all in c3;
--Testcase 46:
fetch backward 1 in c3;
--Testcase 47:
fetch all in c3;

declare c4 scroll cursor for select * from int8_tbl offset 10;
--Testcase 48:
fetch all in c4;
--Testcase 49:
fetch 1 in c4;
--Testcase 50:
fetch backward 1 in c4;
--Testcase 51:
fetch backward all in c4;
--Testcase 52:
fetch backward 1 in c4;
--Testcase 53:
fetch all in c4;

declare c5 scroll cursor for select * from int8_tbl order by q1 fetch first 2 rows with ties;
--Testcase 54:
fetch all in c5;
--Testcase 55:
fetch 1 in c5;
--Testcase 56:
fetch backward 1 in c5;
--Testcase 57:
fetch backward 1 in c5;
--Testcase 58:
fetch all in c5;
--Testcase 59:
fetch backward all in c5;
--Testcase 60:
fetch all in c5;
--Testcase 61:
fetch backward all in c5;

rollback;

-- Stress test for variable LIMIT in conjunction with bounded-heap sorting
--Testcase 62:
DELETE FROM INT8_TMP;
--Testcase 63:
INSERT INTO INT8_TMP SELECT q1 FROM generate_series(1,10) q1;

--Testcase 64:
SELECT
  (SELECT s.q1 
     FROM (VALUES (1)) AS x,
          (SELECT q1 FROM INT8_TMP as n 
             ORDER BY q1 LIMIT 1 OFFSET s.q1-1) AS y) AS z
  FROM INT8_TMP AS s;

--
-- Test behavior of volatile and set-returning functions in conjunction
-- with ORDER BY and LIMIT.
--

--Testcase 65:
create temp sequence testseq;

--Testcase 15:
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

--Testcase 16:
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10 offset 5;

select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10 offset 5;

--Testcase 17:
select currval('testseq');

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 desc limit 10;

select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 desc limit 10;


explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 desc limit 10 offset 5;

select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 desc limit 10 offset 5;

select currval('testseq');

--Testcase 18:
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

--Testcase 19:
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10 offset 5;

select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10 offset 5;

--Testcase 20:
select currval('testseq');

-- test for limit and offset when querying table and foreign table inherited
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from parent_table order by tenthous limit 10;

select unique1, unique2, nextval('testseq')
  from parent_table order by tenthous limit 10;

-- when querying regular tables with inherited tables, only limit is pushed-down when no offset is specified
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from parent_table order by tenthous limit 10 offset 5;

select unique1, unique2, nextval('testseq')
  from parent_table order by tenthous limit 10 offset 5;

select currval('testseq');

--Testcase 21:
explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

--Testcase 22:
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

--Testcase 23:
explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

--Testcase 24:
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

-- use of random() is to keep planner from folding the expressions together
--Testcase 66:
DELETE FROM INT8_TMP;
--Testcase 67:
INSERT INTO INT8_TMP VALUES (generate_series(0,2), generate_series((random()*.1)::int,2));
--Testcase 68:
explain (verbose, costs off)
select q1, q2 from int8_tmp;

--Testcase 69:
select q1, q2 from int8_tmp;

--Testcase 70:
explain (verbose, costs off)
select q1, q2 from int8_tmp order by q2 desc;

--Testcase 71:
select q1, q2 from int8_tmp order by q2 desc;

-- test for failure to set all aggregates' aggtranstype
--Testcase 25:
explain (verbose, costs off)
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

--Testcase 26:
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

--
-- FETCH FIRST
-- Check the WITH TIES clause
--

--Testcase 72:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 2 ROW WITH TIES;

--Testcase 73:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST ROWS WITH TIES;

--Testcase 74:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 1 ROW WITH TIES;

--Testcase 75:
SELECT  thousand
		FROM onek WHERE thousand < 5
		ORDER BY thousand FETCH FIRST 2 ROW ONLY;

-- should fail
--Testcase 76:
SELECT ''::text AS two, unique1, unique2, stringu1
		FROM onek WHERE unique1 > 50
		FETCH FIRST 2 ROW WITH TIES;

-- test ruleutils
--Testcase 77:
CREATE VIEW limit_thousand_v_1 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST 5 ROWS WITH TIES OFFSET 10;
--Testcase 78:
\d+ limit_thousand_v_1
--Testcase 79:
CREATE VIEW limit_thousand_v_2 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand OFFSET 10 FETCH FIRST 5 ROWS ONLY;
--Testcase 80:
\d+ limit_thousand_v_2
--Testcase 81:
CREATE VIEW limit_thousand_v_3 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST NULL ROWS WITH TIES;		-- fails
--Testcase 82:
CREATE VIEW limit_thousand_v_3 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST (NULL+1) ROWS WITH TIES;
--Testcase 83:
\d+ limit_thousand_v_3
--Testcase 84:
CREATE VIEW limit_thousand_v_4 AS SELECT thousand FROM onek WHERE thousand < 995
		ORDER BY thousand FETCH FIRST NULL ROWS ONLY;
--Testcase 85:
\d+ limit_thousand_v_4
-- leave these views

-- Clean up
DO $d$
declare
  l_rec record;
begin
  for l_rec in (select foreign_table_schema, foreign_table_name
                from information_schema.foreign_tables) loop
     execute format('drop foreign table %I.%I cascade;', l_rec.foreign_table_schema, l_rec.foreign_table_name);
  end loop;
end;
$d$;

--Testcase 86:
DROP SERVER sqlite_svr;
--Testcase 87:
DROP EXTENSION duckdb_fdw CASCADE;
