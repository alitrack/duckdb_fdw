--
-- SELECT
--
--Testcase 43:
CREATE EXTENSION duckdb_fdw;
--Testcase 44:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');

--Testcase 45:
CREATE FOREIGN TABLE onek (
  unique1   int4,
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER sqlite_svr;

--Testcase 46:
CREATE FOREIGN TABLE onek2 (
  unique1   int4,
  unique2   int4,
  two     int4,
  four    int4,
  ten     int4,
  twenty    int4,
  hundred   int4,
  thousand  int4,
  twothousand int4,
  fivethous int4,
  tenthous  int4,
  odd     int4,
  even    int4,
  stringu1  name,
  stringu2  name,
  string4   name
) SERVER sqlite_svr;

--Testcase 47:
CREATE FOREIGN TABLE INT8_TBL(
  q1 int8 OPTIONS (key 'true'),
  q2 int8 OPTIONS (key 'true')
) SERVER sqlite_svr;

--Testcase 48:
CREATE FOREIGN TABLE person (
  name    text,
  age     int4,
  location  point
) SERVER sqlite_svr;

-- btree index
-- awk '{if($1<10){print;}else{next;}}' onek.data | sort +0n -1
--
--Testcase 1:
SELECT * FROM onek
   WHERE onek.unique1 < 10
   ORDER BY onek.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
--Testcase 2:
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
--Testcase 3:
SELECT onek.unique1, onek.stringu1 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY stringu1 using <;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1d -2 +0nr -1
--
--Testcase 4:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using <, unique1 using >;

--
-- awk '{if($1>980){print $1,$16;}else{next;}}' onek.data |
-- sort +1dr -2 +0n -1
--
--Testcase 5:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 > 980
   ORDER BY string4 using >, unique1 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0nr -1 +1d -2
--
--Testcase 6:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using >, string4 using <;

--
-- awk '{if($1<20){print $1,$16;}else{next;}}' onek.data |
-- sort +0n -1 +1dr -2
--
--Testcase 7:
SELECT onek.unique1, onek.string4 FROM onek
   WHERE onek.unique1 < 20
   ORDER BY unique1 using <, string4 using >;

--
-- test partial btree indexes
--
-- As of 7.2, planner probably won't pick an indexscan without stats,
-- so ANALYZE first.  Also, we want to prevent it from picking a bitmapscan
-- followed by sort, because that could hide index ordering problems.
--
--ANALYZE onek2;

SET enable_seqscan TO off;
SET enable_bitmapscan TO off;
SET enable_sort TO off;

--
-- awk '{if($1<10){print $0;}else{next;}}' onek.data | sort +0n -1
--
--Testcase 8:
SELECT onek2.* FROM onek2 WHERE onek2.unique1 < 10 order by onek2.unique1;

--
-- awk '{if($1<20){print $1,$14;}else{next;}}' onek.data | sort +0nr -1
--
--Testcase 9:
SELECT onek2.unique1, onek2.stringu1 FROM onek2
    WHERE onek2.unique1 < 20
    ORDER BY unique1 using >;

--
-- awk '{if($1>980){print $1,$14;}else{next;}}' onek.data | sort +1d -2
--
--Testcase 10:
SELECT onek2.unique1, onek2.stringu1 FROM onek2
   WHERE onek2.unique1 > 980 ORDER BY onek2.unique1;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_sort;


--Testcase 11:
SELECT two, stringu1, ten, string4
   INTO TABLE tmp
   FROM onek;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=2){print $4,$5;}else{print;}}' - stud_emp.data
--
-- SELECT name, age FROM person*; ??? check if different
--Testcase 12:
SELECT p.name, p.age FROM person* p;

--
-- awk '{print $1,$2;}' person.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - emp.data |
-- awk '{if(NF!=2){print $3,$2;}else{print;}}' - student.data |
-- awk 'BEGIN{FS="      ";}{if(NF!=1){print $4,$5;}else{print;}}' - stud_emp.data |
-- sort +1nr -2
--
--Testcase 13:
SELECT p.name, p.age FROM person* p ORDER BY age using >, name;

--
-- Test some cases involving whole-row Var referencing a subquery
--
--Testcase 49:
select foo from (select 1 offset 0) as foo;
--Testcase 50:
select foo from (select null offset 0) as foo;
--Testcase 51:
select foo from (select 'xyzzy',1,null offset 0) as foo;

--
-- Test VALUES lists
--
--Testcase 14:
select * from onek, (values(147, 'RFAAAA'), (931, 'VJAAAA')) as v (i, j)
    WHERE onek.unique1 = v.i and onek.stringu1 = v.j;

-- a more complex case
-- looks like we're coding lisp :-)
--Testcase 15:
select * from onek,
  (values ((select i from
    (values(10000), (2), (389), (1000), (2000), ((select 10029))) as foo(i)
    order by i asc limit 1))) bar (i)
  where onek.unique1 = bar.i;

-- try VALUES in a subquery
--Testcase 16:
select * from onek
    where (unique1,ten) in (values (1,1), (20,0), (99,9), (17,99))
    order by unique1;

-- VALUES is also legal as a standalone query or a set-operation member
--Testcase 17:
VALUES (1,2), (3,4+4), (7,77.7);

--Testcase 52:
VALUES (1,2), (3,4+4), (7,77.7)
UNION ALL
SELECT 2+2, 57
UNION ALL
TABLE int8_tbl;

--
-- Test ORDER BY options
--

--Testcase 53:
CREATE FOREIGN TABLE foo (f1 int options (key 'true')) SERVER sqlite_svr;

--Testcase 18:
INSERT INTO foo VALUES (42),(3),(10),(7),(null),(null),(1);

--Testcase 19:
SELECT * FROM foo ORDER BY f1;
--Testcase 20:
SELECT * FROM foo ORDER BY f1 ASC;	-- same thing
--Testcase 21:
SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--Testcase 22:
SELECT * FROM foo ORDER BY f1 DESC;
--Testcase 23:
SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

-- Skip, cannot create index on foreign table
-- check if indexscans do the right things
--CREATE INDEX fooi ON foo (f1);
--SET enable_sort = false;

--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--SELECT * FROM foo ORDER BY f1 DESC;
--SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--DROP INDEX fooi;
--CREATE INDEX fooi ON foo (f1 DESC);

--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--SELECT * FROM foo ORDER BY f1 DESC;
--SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--DROP INDEX fooi;
--CREATE INDEX fooi ON foo (f1 DESC NULLS LAST);

--SELECT * FROM foo ORDER BY f1;
--SELECT * FROM foo ORDER BY f1 NULLS FIRST;
--SELECT * FROM foo ORDER BY f1 DESC;
--SELECT * FROM foo ORDER BY f1 DESC NULLS LAST;

--
-- Test planning of some cases with partial indexes
--

-- partial index is usable
--Testcase 24:
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 25:
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- actually run the query with an analyze to use the partial index
--Testcase 54:
explain (costs off, analyze on, timing off, summary off)
select * from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 27:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
--Testcase 28:
select unique2 from onek2 where unique2 = 11 and stringu1 = 'ATAAAA';
-- partial index predicate implies clause, so no need for retest
--Testcase 29:
explain (costs off)
select * from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 30:
select * from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 31:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 32:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
-- but if it's an update target, must retest anyway
--Testcase 33:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
--Testcase 34:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B' for update;
-- partial index is not applicable
--Testcase 35:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
--Testcase 36:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'C';
-- partial index implies clause, but bitmap scan must recheck predicate anyway
SET enable_indexscan TO off;
--Testcase 37:
explain (costs off)
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
--Testcase 38:
select unique2 from onek2 where unique2 = 11 and stringu1 < 'B';
RESET enable_indexscan;
-- check multi-index cases too
--Testcase 39:
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
--Testcase 40:
select unique1, unique2 from onek2
  where (unique2 = 11 or unique1 = 0) and stringu1 < 'B';
--Testcase 41:
explain (costs off)
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;
--Testcase 42:
select unique1, unique2 from onek2
  where (unique2 = 11 and stringu1 < 'B') or unique1 = 0;

--
-- Test some corner cases that have been known to confuse the planner
--

-- ORDER BY on a constant doesn't really need any sorting
--Testcase 55:
SELECT 1 AS x ORDER BY x;

-- But ORDER BY on a set-valued expression does
--Testcase 56:
create function sillysrf(int) returns setof int as
  'values (1),(10),(2),($1)' language sql immutable;

--Testcase 57:
select sillysrf(42);
--Testcase 58:
select sillysrf(-1) order by 1;

--Testcase 59:
drop function sillysrf(int);

-- X = X isn't a no-op, it's effectively X IS NOT NULL assuming = is strict
-- (see bug #5084)
--Testcase 60:
select * from (values (2),(null),(1)) v(k) where k = k order by k;
--Testcase 61:
select * from (values (2),(null),(1)) v(k) where k = k;

-- Test partitioned tables with no partitions, which should be handled the
-- same as the non-inheritance case when expanding its RTE.
--Testcase 62:
create table list_parted_tbl (a int,b int) partition by list (a);
--Testcase 63:
create table list_parted_tbl1 partition of list_parted_tbl
  for values in (1) partition by list(b);
--Testcase 64:
explain (costs off) select * from list_parted_tbl;
--Testcase 65:
drop table list_parted_tbl;

--Testcase 66:
DROP FOREIGN TABLE onek;
--Testcase 67:
DROP FOREIGN TABLE onek2;
--Testcase 68:
DROP FOREIGN TABLE int8_tbl;
--Testcase 69:
DROP FOREIGN TABLE person;
--Testcase 70:
DROP FOREIGN TABLE foo;
--Testcase 71:
DROP SERVER sqlite_svr;
--Testcase 72:
DROP EXTENSION duckdb_fdw CASCADE;
