--
-- JOIN
-- Test JOIN clauses
--
--Testcase 360:
CREATE EXTENSION duckdb_fdw;
--Testcase 361:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');

--Testcase 362:
CREATE FOREIGN TABLE J1_TBL (
  i integer,
  j integer,
  t text
) SERVER sqlite_svr; 

--Testcase 363:
CREATE FOREIGN TABLE J2_TBL (
  i integer,
  k integer
) SERVER sqlite_svr; 

--Testcase 364:
CREATE FOREIGN TABLE tenk1 (
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

--Testcase 365:
CREATE FOREIGN TABLE tenk2 (
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

--Testcase 366:
CREATE FOREIGN TABLE INT4_TBL(f1 int4 OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 367:
CREATE FOREIGN TABLE FLOAT8_TBL(f1 float8 OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 368:
CREATE FOREIGN TABLE INT8_TBL(
  q1 int8 OPTIONS (key 'true'),
  q2 int8 OPTIONS (key 'true')
) SERVER sqlite_svr;
--Testcase 369:
CREATE FOREIGN TABLE INT2_TBL(f1 int2 OPTIONS (key 'true')) SERVER sqlite_svr;

--Testcase 1:
INSERT INTO J1_TBL VALUES (1, 4, 'one');
--Testcase 2:
INSERT INTO J1_TBL VALUES (2, 3, 'two');
--Testcase 3:
INSERT INTO J1_TBL VALUES (3, 2, 'three');
--Testcase 4:
INSERT INTO J1_TBL VALUES (4, 1, 'four');
--Testcase 5:
INSERT INTO J1_TBL VALUES (5, 0, 'five');
--Testcase 6:
INSERT INTO J1_TBL VALUES (6, 6, 'six');
--Testcase 7:
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
--Testcase 8:
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
--Testcase 9:
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
--Testcase 10:
INSERT INTO J1_TBL VALUES (NULL, NULL, 'null');
--Testcase 11:
INSERT INTO J1_TBL VALUES (NULL, 0, 'zero');

--Testcase 12:
INSERT INTO J2_TBL VALUES (1, -1);
--Testcase 13:
INSERT INTO J2_TBL VALUES (2, 2);
--Testcase 14:
INSERT INTO J2_TBL VALUES (3, -3);
--Testcase 15:
INSERT INTO J2_TBL VALUES (2, 4);
--Testcase 16:
INSERT INTO J2_TBL VALUES (5, -5);
--Testcase 17:
INSERT INTO J2_TBL VALUES (5, -5);
--Testcase 18:
INSERT INTO J2_TBL VALUES (0, NULL);
--Testcase 19:
INSERT INTO J2_TBL VALUES (NULL, NULL);
--Testcase 20:
INSERT INTO J2_TBL VALUES (NULL, 0);

-- useful in some tests below
--Testcase 370:
create temp table onerow();
--Testcase 371:
insert into onerow default values;
analyze onerow;


--
-- CORRELATION NAMES
-- Make sure that table/column aliases are supported
-- before diving into more complex join syntax.
--

--Testcase 21:
SELECT '' AS "xxx", *
  FROM J1_TBL AS tx;

--Testcase 22:
SELECT '' AS "xxx", *
  FROM J1_TBL tx;

--Testcase 23:
SELECT '' AS "xxx", *
  FROM J1_TBL AS t1 (a, b, c);

--Testcase 24:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c);

--Testcase 25:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e);

--Testcase 26:
SELECT '' AS "xxx", t1.a, t2.e
  FROM J1_TBL t1 (a, b, c), J2_TBL t2 (d, e)
  WHERE t1.a = t2.d;


--
-- CROSS JOIN
-- Qualifications are not allowed on cross joins,
-- which degenerate into a standard unqualified inner join.
--

--Testcase 27:
SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL;

-- ambiguous column
--Testcase 28:
SELECT '' AS "xxx", i, k, t
  FROM J1_TBL CROSS JOIN J2_TBL;

-- resolve previous ambiguity by specifying the table name
--Testcase 29:
SELECT '' AS "xxx", t1.i, k, t
  FROM J1_TBL t1 CROSS JOIN J2_TBL t2;

--Testcase 30:
SELECT '' AS "xxx", ii, tt, kk
  FROM (J1_TBL CROSS JOIN J2_TBL)
    AS tx (ii, jj, tt, ii2, kk);

--Testcase 31:
SELECT '' AS "xxx", tx.ii, tx.jj, tx.kk
  FROM (J1_TBL t1 (a, b, c) CROSS JOIN J2_TBL t2 (d, e))
    AS tx (ii, jj, tt, ii2, kk);

--Testcase 32:
SELECT '' AS "xxx", *
  FROM J1_TBL CROSS JOIN J2_TBL a CROSS JOIN J2_TBL b;


--
--
-- Inner joins (equi-joins)
--
--

--
-- Inner joins (equi-joins) with USING clause
-- The USING syntax changes the shape of the resulting table
-- by including a column in the USING clause only once in the result.
--

-- Inner equi-join on specified column
--Testcase 33:
SELECT '' AS "xxx", *
  FROM J1_TBL INNER JOIN J2_TBL USING (i);

-- Same as above, slightly different syntax
--Testcase 34:
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL USING (i);

--Testcase 35:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, d) USING (a)
  ORDER BY a, d;

--Testcase 36:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) JOIN J2_TBL t2 (a, b) USING (b)
  ORDER BY b, t1.a;


--
-- NATURAL JOIN
-- Inner equi-join on all columns with the same name
--

--Testcase 37:
SELECT '' AS "xxx", *
  FROM J1_TBL NATURAL JOIN J2_TBL;

--Testcase 38:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (a, d);

--Testcase 39:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b, c) NATURAL JOIN J2_TBL t2 (d, a);

-- mismatch number of columns
-- currently, Postgres will fill in with underlying names
--Testcase 40:
SELECT '' AS "xxx", *
  FROM J1_TBL t1 (a, b) NATURAL JOIN J2_TBL t2 (a);


--
-- Inner joins (equi-joins)
--

--Testcase 41:
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.i);

--Testcase 42:
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i = J2_TBL.k);


--
-- Non-equi-joins
--

--Testcase 43:
SELECT '' AS "xxx", *
  FROM J1_TBL JOIN J2_TBL ON (J1_TBL.i <= J2_TBL.k);


--
-- Outer joins
-- Note that OUTER is a noise word
--

--Testcase 44:
SELECT '' AS "xxx", *
  FROM J1_TBL LEFT OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 45:
SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 46:
SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT OUTER JOIN J2_TBL USING (i);

--Testcase 47:
SELECT '' AS "xxx", *
  FROM J1_TBL RIGHT JOIN J2_TBL USING (i);

--Testcase 48:
SELECT '' AS "xxx", *
  FROM J1_TBL FULL OUTER JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 49:
SELECT '' AS "xxx", *
  FROM J1_TBL FULL JOIN J2_TBL USING (i)
  ORDER BY i, k, t;

--Testcase 50:
SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (k = 1);

--Testcase 51:
SELECT '' AS "xxx", *
  FROM J1_TBL LEFT JOIN J2_TBL USING (i) WHERE (i = 1);

--
-- semijoin selectivity for <>
--
--Testcase 52:
explain (costs off)
select * from int4_tbl i4, tenk1 a
where exists(select * from tenk1 b
             where a.twothousand = b.twothousand and a.fivethous <> b.fivethous)
      and i4.f1 = a.tenthous;


--
-- More complicated constructs
--

--
-- Multiway full join
--

--Testcase 372:
CREATE FOREIGN TABLE t11 (name TEXT, n INTEGER) SERVER sqlite_svr;
--Testcase 373:
CREATE FOREIGN TABLE t21 (name TEXT, n INTEGER) SERVER sqlite_svr;
--Testcase 374:
CREATE FOREIGN TABLE t31 (name TEXT, n INTEGER) SERVER sqlite_svr;

--Testcase 53:
INSERT INTO t11 VALUES ( 'bb', 11 );
--Testcase 54:
INSERT INTO t21 VALUES ( 'bb', 12 );
--Testcase 55:
INSERT INTO t21 VALUES ( 'cc', 22 );
--Testcase 56:
INSERT INTO t21 VALUES ( 'ee', 42 );
--Testcase 57:
INSERT INTO t31 VALUES ( 'bb', 13 );
--Testcase 58:
INSERT INTO t31 VALUES ( 'cc', 23 );
--Testcase 59:
INSERT INTO t31 VALUES ( 'dd', 33 );

--Testcase 60:
SELECT * FROM t11 FULL JOIN t21 USING (name) FULL JOIN t31 USING (name);

--
-- Test interactions of join syntax and subqueries
--

-- Basic cases (we expect planner to pull up the subquery here)
--Testcase 61:
SELECT * FROM
(SELECT * FROM t21) as s2
INNER JOIN
(SELECT * FROM t31) s3
USING (name);

--Testcase 62:
SELECT * FROM
(SELECT * FROM t21) as s2
LEFT JOIN
(SELECT * FROM t31) s3
USING (name);

--Testcase 63:
SELECT * FROM
(SELECT * FROM t21) as s2
FULL JOIN
(SELECT * FROM t31) s3
USING (name);

-- Cases with non-nullable expressions in subquery results;
-- make sure these go to null as expected
--Testcase 64:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 65:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL LEFT JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 66:
SELECT * FROM
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 67:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t11) as s1
NATURAL INNER JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL INNER JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 68:
SELECT * FROM
(SELECT name, n as s1_n, 1 as s1_1 FROM t11) as s1
NATURAL FULL JOIN
(SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
NATURAL FULL JOIN
(SELECT name, n as s3_n, 3 as s3_2 FROM t31) s3;

--Testcase 69:
SELECT * FROM
(SELECT name, n as s1_n FROM t11) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n FROM t21) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t31) as s3
  ) ss2;

--Testcase 70:
SELECT * FROM
(SELECT name, n as s1_n FROM t11) as s1
NATURAL FULL JOIN
  (SELECT * FROM
    (SELECT name, n as s2_n, 2 as s2_2 FROM t21) as s2
    NATURAL FULL JOIN
    (SELECT name, n as s3_n FROM t31) as s3
  ) ss2;

-- Constants as join keys can also be problematic
--Testcase 375:
SELECT * FROM
  (SELECT name, n as s1_n FROM t11) as s1
FULL JOIN
  (SELECT name, 2 as s2_n FROM t21) as s2
ON (s1_n = s2_n);

-- Test for propagation of nullability constraints into sub-joins

--Testcase 376:
create foreign table x (x1 int, x2 int) server sqlite_svr;
--Testcase 71:
insert into x values (1,11);
--Testcase 72:
insert into x values (2,22);
--Testcase 73:
insert into x values (3,null);
--Testcase 74:
insert into x values (4,44);
--Testcase 75:
insert into x values (5,null);

--Testcase 377:
create foreign table y (y1 int, y2 int) server sqlite_svr;
--Testcase 76:
insert into y values (1,111);
--Testcase 77:
insert into y values (2,222);
--Testcase 78:
insert into y values (3,333);
--Testcase 79:
insert into y values (4,null);

--Testcase 80:
select * from x;
--Testcase 81:
select * from y;

--Testcase 82:
select * from x left join y on (x1 = y1 and x2 is not null);
--Testcase 83:
select * from x left join y on (x1 = y1 and y2 is not null);

--Testcase 84:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1);
--Testcase 85:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and x2 is not null);
--Testcase 86:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and y2 is not null);
--Testcase 87:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1 and xx2 is not null);
-- these should NOT give the same answers as above
--Testcase 88:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (x2 is not null);
--Testcase 89:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (y2 is not null);
--Testcase 90:
select * from (x left join y on (x1 = y1)) left join x xx(xx1,xx2)
on (x1 = xx1) where (xx2 is not null);

--
-- regression test: check for bug with propagation of implied equality
-- to outside an IN
--
--Testcase 91:
select count(*) from tenk1 a where unique1 in
  (select unique1 from tenk1 b join tenk1 c using (unique1)
   where b.unique2 = 42);

--
-- regression test: check for failure to generate a plan with multiple
-- degenerate IN clauses
--
--Testcase 92:
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);

-- try that with GEQO too
begin;
set geqo = on;
set geqo_threshold = 2;
--Testcase 93:
select count(*) from tenk1 x where
  x.unique1 in (select a.f1 from int4_tbl a,float8_tbl b where a.f1=b.f1) and
  x.unique1 = 0 and
  x.unique1 in (select aa.f1 from int4_tbl aa,float8_tbl bb where aa.f1=bb.f1);
rollback;

--
-- regression test: be sure we cope with proven-dummy append rels
--
--Testcase 378:
create table b (aa int, bb int);
--Testcase 379:
explain (costs off)
select aa, bb, unique1, unique1
  from tenk1 right join b on aa = unique1
  where bb < bb and bb is null;

--Testcase 380:
select aa, bb, unique1, unique1
  from tenk1 right join b on aa = unique1
  where bb < bb and bb is null;

--
-- regression test: check handling of empty-FROM subquery underneath outer join
--
--Testcase 94:
explain (costs off)
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--Testcase 95:
select * from int8_tbl i1 left join (int8_tbl i2 join
  (select 123 as x) ss on i2.q1 = x) on i1.q2 = i2.q2
order by 1, 2;

--
-- regression test: check a case where join_clause_is_movable_into() gives
-- an imprecise result, causing an assertion failure
--
--Testcase 96:
select count(*)
from
  (select t31.tenthous as x1, coalesce(t11.stringu1, t21.stringu1) as x2
   from tenk1 t11
   left join tenk1 t21 on t11.unique1 = t21.unique1
   join tenk1 t31 on t11.unique2 = t31.unique2) ss,
  tenk1 t4,
  tenk1 t5
where t4.thousand = t5.unique1 and ss.x1 = t4.tenthous and ss.x2 = t5.stringu1;

--
-- regression test: check a case where we formerly missed including an EC
-- enforcement clause because it was expected to be handled at scan level
--
--Testcase 97:
explain (costs off)
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--Testcase 98:
select a.f1, b.f1, t.thousand, t.tenthous from
  tenk1 t,
  (select sum(f1)+1 as f1 from int4_tbl i4a) a,
  (select sum(f1) as f1 from int4_tbl i4b) b
where b.f1 = t.thousand and a.f1 = b.f1 and (a.f1+b.f1+999) = t.tenthous;

--
-- check a case where we formerly got confused by conflicting sort orders
-- in redundant merge join path keys
-- PS: Used ORDER BY to force SQLite and PG12 always order in the same way (NULLS FIRST/LAST default value for PG and Sqlite are different)
--
--Testcase 99:
explain (costs off)
select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k ORDER BY j1_tbl.i, j2_tbl.k;

--Testcase 100:
select * from
  j1_tbl full join
  (select * from j2_tbl order by j2_tbl.i desc, j2_tbl.k asc) j2_tbl
  on j1_tbl.i = j2_tbl.i and j1_tbl.i = j2_tbl.k ORDER BY j1_tbl.i, j2_tbl.k;

--
-- a different check for handling of redundant sort keys in merge joins
--
--Testcase 101:
explain (costs off)
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;

--Testcase 102:
select count(*) from
  (select * from tenk1 x order by x.thousand, x.twothousand, x.fivethous) x
  left join
  (select * from tenk1 y order by y.unique2) y
  on x.thousand = y.unique2 and x.twothousand = y.hundred and x.fivethous = y.unique2;


--
-- Clean up
--

--Testcase 381:
DROP FOREIGN TABLE t11;
--Testcase 382:
DROP FOREIGN TABLE t21;
--Testcase 383:
DROP FOREIGN TABLE t31;

--Testcase 384:
DROP FOREIGN TABLE J1_TBL;
--Testcase 385:
DROP FOREIGN TABLE J2_TBL;

-- Both DELETE and UPDATE allow the specification of additional tables
-- to "join" against to determine which rows should be modified.

--Testcase 386:
CREATE FOREIGN TABLE t12 (a int OPTIONS (key 'true'), b int) SERVER sqlite_svr;
--Testcase 387:
CREATE FOREIGN TABLE t22 (a int OPTIONS (key 'true'), b int) SERVER sqlite_svr;
--Testcase 388:
CREATE FOREIGN TABLE t32 (x int OPTIONS (key 'true'), y int) SERVER sqlite_svr;

--Testcase 103:
INSERT INTO t12 VALUES (5, 10);
--Testcase 104:
INSERT INTO t12 VALUES (15, 20);
--Testcase 105:
INSERT INTO t12 VALUES (100, 100);
--Testcase 106:
INSERT INTO t12 VALUES (200, 1000);
--Testcase 107:
INSERT INTO t22 VALUES (200, 2000);
--Testcase 108:
INSERT INTO t32 VALUES (5, 20);
--Testcase 109:
INSERT INTO t32 VALUES (6, 7);
--Testcase 110:
INSERT INTO t32 VALUES (7, 8);
--Testcase 111:
INSERT INTO t32 VALUES (500, 100);

--Testcase 112:
DELETE FROM t32 USING t12 table1 WHERE t32.x = table1.a;
--Testcase 113:
SELECT * FROM t32;
--Testcase 114:
DELETE FROM t32 USING t12 JOIN t22 USING (a) WHERE t32.x > t12.a;
--Testcase 115:
SELECT * FROM t32;
--Testcase 116:
DELETE FROM t32 USING t32 t3_other WHERE t32.x = t3_other.x AND t32.y = t3_other.y;
--Testcase 117:
SELECT * FROM t32;

-- Test join against inheritance tree

--Testcase 389:
create temp table t2a () inherits (t22);

--Testcase 118:
insert into t2a values (200, 2001);

--Testcase 119:
select * from t12 left join t22 on (t12.a = t22.a);

-- Test matching of column name with wrong alias

--Testcase 120:
select t12.x from t12 join t32 on (t12.a = t32.x);

--
-- regression test for 8.1 merge right join bug
--

--Testcase 390:
CREATE FOREIGN TABLE tt1 ( tt1_id int4, joincol int4 ) SERVER sqlite_svr;
--Testcase 121:
INSERT INTO tt1 VALUES (1, 11);
--Testcase 122:
INSERT INTO tt1 VALUES (2, NULL);

--Testcase 391:
CREATE FOREIGN TABLE tt2 ( tt2_id int4, joincol int4 ) SERVER sqlite_svr;
--Testcase 123:
INSERT INTO tt2 VALUES (21, 11);
--Testcase 124:
INSERT INTO tt2 VALUES (22, 11);

set enable_hashjoin to off;
set enable_nestloop to off;

-- these should give the same results

--Testcase 125:
select tt1.*, tt2.* from tt1 left join tt2 on tt1.joincol = tt2.joincol;

--Testcase 126:
select tt1.*, tt2.* from tt2 right join tt1 on tt1.joincol = tt2.joincol;

reset enable_hashjoin;
reset enable_nestloop;

--
-- regression test for bug #13908 (hash join with skew tuples & nbatch increase)
--

set work_mem to '64kB';
set enable_mergejoin to off;

--Testcase 127:
explain (costs off)
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;
--Testcase 128:
select count(*) from tenk1 a, tenk1 b
  where a.hundred = b.thousand and (b.fivethous % 10) < 10;

reset work_mem;
reset enable_mergejoin;

--
-- regression test for 8.2 bug with improper re-ordering of left joins
--

--Testcase 392:
create foreign table tt3(f1 int, f2 text) server sqlite_svr;
--Testcase 129:
insert into tt3 select x, repeat('xyzzy', 100) from generate_series(1,10000) x;

--Testcase 393:
create foreign table tt4(f1 int) server sqlite_svr;
--Testcase 130:
insert into tt4 values (0),(1),(9999);

--Testcase 131:
SELECT a.f1
FROM tt4 a
LEFT JOIN (
        SELECT b.f1
        FROM tt3 b LEFT JOIN tt3 c ON (b.f1 = c.f1)
        WHERE c.f1 IS NULL
) AS d ON (a.f1 = d.f1)
WHERE d.f1 IS NULL;

--
-- regression test for proper handling of outer joins within antijoins
--

--Testcase 394:
create foreign table tt4x(c1 int, c2 int, c3 int) server sqlite_svr;

--Testcase 132:
explain (costs off)
select * from tt4x t1
where not exists (
  select 1 from tt4x t2
    left join tt4x t3 on t2.c3 = t3.c1
    left join ( select t5.c1 as c1
                from tt4x t4 left join tt4x t5 on t4.c2 = t5.c1
              ) a1 on t3.c2 = a1.c1
  where t1.c1 = t2.c2
);

--
-- regression test for problems of the sort depicted in bug #3494
--

--Testcase 395:
create foreign table tt5(f1 int, f2 int) server sqlite_svr;
--Testcase 396:
create foreign table tt6(f1 int, f2 int) server sqlite_svr;

--Testcase 133:
insert into tt5 values(1, 10);
--Testcase 134:
insert into tt5 values(1, 11);

--Testcase 135:
insert into tt6 values(1, 9);
--Testcase 136:
insert into tt6 values(1, 2);
--Testcase 137:
insert into tt6 values(2, 9);

--Testcase 138:
select * from tt5,tt6 where tt5.f1 = tt6.f1 and tt5.f1 = tt5.f2 - tt6.f2;

--
-- regression test for problems of the sort depicted in bug #3588
--

--Testcase 397:
create foreign table xx (pkxx int) server sqlite_svr;
--Testcase 398:
create foreign table yy (pkyy int, pkxx int) server sqlite_svr;

--Testcase 139:
insert into xx values (1);
--Testcase 140:
insert into xx values (2);
--Testcase 141:
insert into xx values (3);

--Testcase 142:
insert into yy values (101, 1);
--Testcase 143:
insert into yy values (201, 2);
--Testcase 144:
insert into yy values (301, NULL);

--Testcase 145:
select yy.pkyy as yy_pkyy, yy.pkxx as yy_pkxx, yya.pkyy as yya_pkyy,
       xxa.pkxx as xxa_pkxx, xxb.pkxx as xxb_pkxx
from yy
     left join (SELECT * FROM yy where pkyy = 101) as yya ON yy.pkyy = yya.pkyy
     left join xx xxa on yya.pkxx = xxa.pkxx
     left join xx xxb on coalesce (xxa.pkxx, 1) = xxb.pkxx;

--
-- regression test for improper pushing of constants across outer-join clauses
-- (as seen in early 8.2.x releases)
--

--Testcase 399:
create foreign table zt1 (f1 int OPTIONS(key 'true')) server sqlite_svr;
--Testcase 400:
create foreign table zt2 (f2 int OPTIONS(key 'true')) server sqlite_svr;
--Testcase 401:
create foreign table zt3 (f3 int OPTIONS(key 'true')) server sqlite_svr;
--Testcase 146:
insert into zt1 values(53);
--Testcase 147:
insert into zt2 values(53);

--Testcase 148:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zt1 on (f3 = f1)
where f2 = 53;

--Testcase 402:
create temp view zv1 as select *,'dummy'::text AS junk from zt1;

--Testcase 149:
select * from
  zt2 left join zt3 on (f2 = f3)
      left join zv1 on (f3 = f1)
where f2 = 53;

--
-- regression test for improper extraction of OR indexqual conditions
-- (as seen in early 8.3.x releases)
--

--Testcase 150:
select a.unique2, a.ten, b.tenthous, b.unique2, b.hundred
from tenk1 a left join tenk1 b on a.unique2 = b.tenthous
where a.unique1 = 42 and
      ((b.unique2 is null and a.ten = 2) or b.hundred = 3);

--
-- test proper positioning of one-time quals in EXISTS (8.4devel bug)
--
--Testcase 151:
prepare foo(bool) as
  select count(*) from tenk1 a left join tenk1 b
    on (a.unique2 = b.unique1 and exists
        (select 1 from tenk1 c where c.thousand = b.unique2 and $1));
--Testcase 152:
execute foo(true);
--Testcase 153:
execute foo(false);

--
-- test for sane behavior with noncanonical merge clauses, per bug #4926
--

begin;

set enable_mergejoin = 1;
set enable_hashjoin = 0;
set enable_nestloop = 0;

--Testcase 403:
create foreign table a1 (i integer) server sqlite_svr;
--Testcase 404:
create foreign table b1 (x integer, y integer) server sqlite_svr;

--Testcase 154:
select * from a1 left join b1 on i = x and i = y and x = i;

rollback;

-- skip this test, sqlite fdw does not support customized type
-- test handling of merge clauses using record_ops
--
--begin;

--create type mycomptype as (id int, v bigint);

--create foreign table tidv (idv mycomptype) server sqlite_svr;
--create index on tidv (idv);

--explain (costs off)
--select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

--set enable_mergejoin = 0;

--explain (costs off)
--select a.idv, b.idv from tidv a, tidv b where a.idv = b.idv;

--rollback;

--
-- test NULL behavior of whole-row Vars, per bug #5025
--
--Testcase 155:
select t1.q2, count(t2.*)
from int8_tbl t1 left join int8_tbl t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 156:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 157:
select t1.q2, count(t2.*)
from int8_tbl t1 left join (select * from int8_tbl offset 0) t2 on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--Testcase 158:
select t1.q2, count(t2.*)
from int8_tbl t1 left join
  (select q1, case when q2=1 then 1 else q2 end as q2 from int8_tbl) t2
  on (t1.q2 = t2.q1)
group by t1.q2 order by 1;

--
-- test incorrect failure to NULL pulled-up subexpressions
--
begin;

--Testcase 405:
create foreign table a2 (
     code char OPTIONS (key 'true')
) server sqlite_svr;
--Testcase 406:
create foreign table b2 (
     a char OPTIONS (key 'true'),
     num integer OPTIONS (key 'true')
) server sqlite_svr;
--Testcase 407:
create foreign table c2 (
     name char OPTIONS (key 'true'),
     a char
) server sqlite_svr;

--Testcase 159:
insert into a2 (code) values ('p');
--Testcase 160:
insert into a2 (code) values ('q');
--Testcase 161:
insert into b2 (a, num) values ('p', 1);
--Testcase 162:
insert into b2 (a, num) values ('p', 2);
--Testcase 163:
insert into c2 (name, a) values ('A', 'p');
--Testcase 164:
insert into c2 (name, a) values ('B', 'q');
--Testcase 165:
insert into c2 (name, a) values ('C', null);

--Testcase 166:
select c2.name, ss.code, ss.b_cnt, ss.const
from c2 left join
  (select a2.code, coalesce(b_grp.cnt, 0) as b_cnt, -1 as const
   from a2 left join
     (select count(1) as cnt, b2.a from b2 group by b2.a) as b_grp
     on a2.code = b_grp.a
  ) as ss
  on (c2.a = ss.code)
order by c2.name;

rollback;

--
-- test incorrect handling of placeholders that only appear in targetlists,
-- per bug #6154
--
--Testcase 408:
create foreign table sub_tbl (key1 int, key3 int, key5 int, key6 int, value1 int, id int options (key 'true')) server sqlite_svr;
--Testcase 409:
insert into sub_tbl values (1, 1, 1, 2, 42);

--Testcase 410:
SELECT * FROM
( SELECT key1 from sub_tbl) sub1
LEFT JOIN
( SELECT sub3.key3, sub4.value2, COALESCE(sub4.value2, 66) as value3 FROM
    ( SELECT key3 from sub_tbl) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT key5 from sub_tbl) sub5
        LEFT JOIN
        ( SELECT key6, value1 from sub_tbl ) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

-- test the path using join aliases, too
--Testcase 411:
SELECT * FROM
( SELECT key1 from sub_tbl ) sub1
LEFT JOIN
( SELECT sub3.key3, value2, COALESCE(value2, 66) as value3 FROM
    ( SELECT key3 from sub_tbl ) sub3
    LEFT JOIN
    ( SELECT sub5.key5, COALESCE(sub6.value1, 1) as value2 FROM
        ( SELECT key5 from sub_tbl ) sub5
        LEFT JOIN
        ( SELECT key6, value1 from sub_tbl) sub6
        ON sub5.key5 = sub6.key6
    ) sub4
    ON sub4.key5 = sub3.key3
) sub2
ON sub1.key1 = sub2.key3;

--
-- test case where a PlaceHolderVar is used as a nestloop parameter
--

--Testcase 167:
EXPLAIN (COSTS OFF)
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--Testcase 168:
SELECT qq, unique1
  FROM
  ( SELECT COALESCE(q1, 0) AS qq FROM int8_tbl a ) AS ss1
  FULL OUTER JOIN
  ( SELECT COALESCE(q2, -1) AS qq FROM int8_tbl b ) AS ss2
  USING (qq)
  INNER JOIN tenk1 c ON qq = unique2;

--
-- nested nestloops can require nested PlaceHolderVars
--

--Testcase 412:
create foreign table nt1 (
  id int OPTIONS (key 'true'),
  a1 boolean,
  a2 boolean
) server sqlite_svr;
--Testcase 413:
create foreign table nt2 (
  id int OPTIONS (key 'true'),
  nt1_id int,
  b1 boolean,
  b2 boolean
) server sqlite_svr;
--Testcase 414:
create foreign table nt3 (
  id int OPTIONS (key 'true'),
  nt2_id int,
  c1 boolean
) server sqlite_svr;

--Testcase 169:
insert into nt1 values (1,true,true);
--Testcase 170:
insert into nt1 values (2,true,false);
--Testcase 171:
insert into nt1 values (3,false,false);
--Testcase 172:
insert into nt2 values (1,1,true,true);
--Testcase 173:
insert into nt2 values (2,2,true,false);
--Testcase 174:
insert into nt2 values (3,3,false,false);
--Testcase 175:
insert into nt3 values (1,1,true);
--Testcase 176:
insert into nt3 values (2,2,false);
--Testcase 177:
insert into nt3 values (3,3,true);

--Testcase 178:
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--Testcase 179:
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 and ss1.a3) AS b3
     from nt2 as nt2
       left join
         (select nt1.*, (nt1.id is not null) as a3 from nt1) as ss1
         on ss1.id = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--
-- test case where a PlaceHolderVar is propagated into a subquery
--

--Testcase 180:
explain (costs off)
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--Testcase 181:
select * from
  int8_tbl t1 left join
  (select q1 as x, 42 as y from int8_tbl t2) ss
  on t1.q2 = ss.x
where
  1 = (select 1 from int8_tbl t3 where ss.y is not null limit 1)
order by 1,2;

--
-- test the corner cases FULL JOIN ON TRUE and FULL JOIN ON FALSE
--
--Testcase 182:
select * from int4_tbl a full join int4_tbl b on true;
--Testcase 183:
select * from int4_tbl a full join int4_tbl b on false;

--
-- test for ability to use a cartesian join when necessary
--
--Testcase 415:
create foreign table q1(i int) server sqlite_svr;
--Testcase 416:
insert into q1 values (1);
--Testcase 417:
create foreign table q2(i int) server sqlite_svr;
--Testcase 418:
insert into q2 values (0);
--Testcase 184:
explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where q1.i = thousand or q2.i = thousand;

--Testcase 185:
explain (costs off)
select * from
  tenk1 join int4_tbl on f1 = twothousand,
  q1, q2
where thousand = (q1.i + q2.i);

--
-- test ability to generate a suitable plan for a star-schema query
--

--Testcase 186:
explain (costs off)
select * from
  tenk1, int8_tbl a, int8_tbl b
where thousand = a.q1 and tenthous = b.q1 and a.q2 = 1 and b.q2 = 2;

--
-- test a corner case in which we shouldn't apply the star-schema optimization
--

--Testcase 187:
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--Testcase 188:
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (select 1,0 from onerow) v1(x1,x2)
               left join (select 3,1 from onerow) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- variant that isn't quite a star-schema case

--Testcase 189:
select ss1.d1 from
  tenk1 as t1
  inner join tenk1 as t2
  on t1.tenthous = t2.ten
  inner join
    int8_tbl as i8
    left join int4_tbl as i4
      inner join (select 64::information_schema.cardinal_number as d1
                  from tenk1 t3,
                       lateral (select abs(t3.unique1) + random()) ss0(x)
                  where t3.fivethous < 0) as ss1
      on i4.f1 = ss1.d1
    on i8.q1 = i4.f1
  on t1.tenthous = ss1.d1
where t1.unique1 < i4.f1;

-- this variant is foldable by the remove-useless-RESULT-RTEs code

--Testcase 419:
explain (costs off)
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

--Testcase 420:
select t1.unique2, t1.stringu1, t2.unique1, t2.stringu2 from
  tenk1 t1
  inner join int4_tbl i1
    left join (select v1.x2, v2.y1, 11 AS d1
               from (values(1,0)) v1(x1,x2)
               left join (values(3,1)) v2(y1,y2)
               on v1.x1 = v2.y2) subq1
    on (i1.f1 = subq1.x2)
  on (t1.unique2 = subq1.d1)
  left join tenk1 t2
  on (subq1.y1 = t2.unique1)
where t1.unique2 < 42 and t1.stringu1 > t2.stringu2;

-- Here's a variant that we can't fold too aggressively, though,
-- or we end up with noplace to evaluate the lateral PHV

--Testcase 421:
explain (verbose, costs off)
select * from
  (select key1 as x from sub_tbl) ss1 left join (select key6 as y from sub_tbl) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;
--Testcase 422:
select * from
  (select key1 as x from sub_tbl as x) ss1 left join (select key6 as y from sub_tbl) ss2 on (true),
  lateral (select ss2.y as z limit 1) ss3;
  
--
-- test inlining of immutable functions
--
--Testcase 423:
create function f_immutable_int4(i integer) returns integer as
$$ begin return i; end; $$ language plpgsql immutable;

-- check optimization of function scan with join
--Testcase 424:
explain (costs off)
select unique1 from tenk1, (select * from f_immutable_int4(1) x) x
where x = unique1;

--Testcase 425:
explain (verbose, costs off)
select unique1, x.*
from tenk1, (select *, random() from f_immutable_int4(1) x) x
where x = unique1;

--Testcase 426:
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = unique1;

--Testcase 427:
explain (costs off)
select unique1 from tenk1, lateral f_immutable_int4(1) x where x = unique1;

--Testcase 428:
explain (costs off)
select unique1, x from tenk1 join f_immutable_int4(1) x on unique1 = x;

--Testcase 429:
explain (costs off)
select unique1, x from tenk1 left join f_immutable_int4(1) x on unique1 = x;

--Testcase 430:
explain (costs off)
select unique1, x from tenk1 right join f_immutable_int4(1) x on unique1 = x;

--Testcase 431:
explain (costs off)
select unique1, x from tenk1 full join f_immutable_int4(1) x on unique1 = x;

-- check that pullup of a const function allows further const-folding
--Testcase 432:
explain (costs off)
select unique1 from tenk1, f_immutable_int4(1) x where x = 42;

-- test inlining of immutable functions with PlaceHolderVars
--Testcase 433:
explain (costs off)
select nt3.id
from nt3 as nt3
  left join
    (select nt2.*, (nt2.b1 or i4 = 42) AS b3
     from nt2 as nt2
       left join
         f_immutable_int4(0) i4
         on i4 = nt2.nt1_id
    ) as ss2
    on ss2.id = nt3.nt2_id
where nt3.id = 1 and ss2.b3;

--Testcase 434:
drop function f_immutable_int4(int);

-- test inlining when function returns composite

--Testcase 435:
create function mki8(bigint, bigint) returns int8_tbl as
$$select row($1,$2)::int8_tbl$$ language sql;

--Testcase 436:
create function mki4(int) returns int4_tbl as
$$select row($1)::int4_tbl$$ language sql;

--Testcase 437:
explain (verbose, costs off)
select * from mki8(1,2);
--Testcase 438:
select * from mki8(1,2);

--Testcase 439:
explain (verbose, costs off)
select * from mki4(42);
--Testcase 440:
select * from mki4(42);

--Testcase 441:
drop function mki8(bigint, bigint);
--Testcase 442:
drop function mki4(int);

--
-- test extraction of restriction OR clauses from join OR clause
-- (we used to only do this for indexable clauses)
--

--Testcase 190:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.hundred = 4);
--Testcase 191:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or (a.unique2 = 3 and b.ten = 4);
--Testcase 192:
explain (costs off)
select * from tenk1 a join tenk1 b on
  (a.unique1 = 1 and b.unique1 = 2) or
  ((a.unique2 = 3 or a.unique2 = 7) and b.hundred = 4);

--
-- test placement of movable quals in a parameterized join tree
--

--Testcase 193:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten = t3.ten
where t1.unique1 = 1;

--Testcase 194:
explain (costs off)
select * from tenk1 t1 left join
  (tenk1 t2 join tenk1 t3 on t2.thousand = t3.unique2)
  on t1.hundred = t2.hundred and t1.ten + t2.ten = t3.ten
where t1.unique1 = 1;

--Testcase 195:
explain (costs off)
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 196:
select count(*) from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on a.unique2 = b.unique1 and c.thousand = a.thousand
  join int4_tbl on b.thousand = f1;

--Testcase 197:
explain (costs off)
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 198:
select b.unique1 from
  tenk1 a join tenk1 b on a.unique1 = b.unique2
  left join tenk1 c on b.unique1 = 42 and c.thousand = a.thousand
  join int4_tbl i1 on b.thousand = f1
  right join int4_tbl i2 on i2.f1 = b.tenthous
  order by 1;

--Testcase 199:
explain (costs off)
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 200:
select * from
(
  select unique1, q1, coalesce(unique1, -1) + q1 as fault
  from int8_tbl left join tenk1 on (q2 = unique2)
) ss
where fault = 122
order by fault;

--Testcase 201:
explain (costs off)
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--Testcase 202:
select * from
(values (1, array[10,20]), (2, array[20,30])) as v1(v1x,v1ys)
left join (values (1, 10), (2, 20)) as v2(v2x,v2y) on v2x = v1x
left join unnest(v1ys) as u1(u1y) on u1y = v2y;

--
-- test handling of potential equivalence clauses above outer joins
--

--Testcase 203:
explain (costs off)
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 204:
select q1, unique2, thousand, hundred
  from int8_tbl a left join tenk1 b on q1 = unique2
  where coalesce(thousand,123) = q1 and q1 = coalesce(hundred,123);

--Testcase 205:
explain (costs off)
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--Testcase 206:
select f1, unique2, case when unique2 is null then f1 else 0 end
  from int4_tbl a left join tenk1 b on f1 = unique2
  where (case when unique2 is null then f1 else 0 end) = 0;

--
-- another case with equivalence clauses above outer joins (bug #8591)
--

--Testcase 207:
explain (costs off)
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--Testcase 208:
select a.unique1, b.unique1, c.unique1, coalesce(b.twothousand, a.twothousand)
  from tenk1 a left join tenk1 b on b.thousand = a.unique1                        left join tenk1 c on c.unique2 = coalesce(b.twothousand, a.twothousand)
  where a.unique2 < 10 and coalesce(b.twothousand, a.twothousand) = 44;

--
-- check handling of join aliases when flattening multiple levels of subquery
--

--Testcase 209:
explain (verbose, costs off)
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--Testcase 210:
select foo1.join_key as foo1_id, foo3.join_key AS foo3_id, bug_field from
  (values (0),(1)) foo1(join_key)
left join
  (select join_key, bug_field from
    (select ss1.join_key, ss1.bug_field from
      (select f1 as join_key, 666 as bug_field from int4_tbl i1) ss1
    ) foo2
   left join
    (select unique2 as join_key from tenk1 i2) ss2
   using (join_key)
  ) foo3
using (join_key);

--
-- test successful handling of nested outer joins with degenerate join quals
--
--Testcase 443:
create foreign table text_tbl(f1 text) server sqlite_svr;

--Testcase 211:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 212:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 213:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 214:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 215:
explain (verbose, costs off)
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 216:
select t1.* from
  text_tbl t1
  left join (select *, '***'::text as d1 from int8_tbl i8b1) b1
    left join int8_tbl i8
      left join (select *, null::int as d2 from int8_tbl i8b2, int4_tbl i4b2
                 where q1 = f1) b2
      on (i8.q1 = b2.q1)
    on (b2.d2 = b1.q2)
  on (t1.f1 = b1.d1)
  left join int4_tbl i4
  on (i8.q2 = i4.f1);

--Testcase 217:
explain (verbose, costs off)
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--Testcase 218:
select * from
  text_tbl t1
  inner join int8_tbl i8
  on i8.q2 = 456
  right join text_tbl t2
  on t1.f1 = 'doh!'
  left join int4_tbl i4
  on i8.q1 = i4.f1;

--
-- test for appropriate join order in the presence of lateral references
--

--Testcase 219:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 220:
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss
where t1.f1 = ss.f1;

--Testcase 221:
explain (verbose, costs off)
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 222:
select * from
  text_tbl t1
  left join int8_tbl i8
  on i8.q2 = 123,
  lateral (select i8.q1, t2.f1 from text_tbl t2 limit 1) as ss1,
  lateral (select ss1.* from text_tbl t3 limit 1) as ss2
where t1.f1 = ss2.f1;

--Testcase 223:
explain (verbose, costs off)
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--Testcase 224:
select 1 from
  text_tbl as tt1
  inner join text_tbl as tt2 on (tt1.f1 = 'foo')
  left join text_tbl as tt3 on (tt3.f1 = 'foo')
  left join text_tbl as tt4 on (tt3.f1 = tt4.f1),
  lateral (select tt4.f1 as c0 from text_tbl as tt5 limit 1) as ss1
where tt1.f1 = ss1.c0;

--
-- check a case in which a PlaceHolderVar forces join order
--

--Testcase 225:
explain (verbose, costs off)
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--Testcase 226:
select ss2.* from
  int4_tbl i41
  left join int8_tbl i8
    join (select i42.f1 as c1, i43.f1 as c2, 42 as c3
          from int4_tbl i42, int4_tbl i43) ss1
    on i8.q1 = ss1.c2
  on i41.f1 = ss1.c1,
  lateral (select i41.*, i8.*, ss1.* from text_tbl limit 1) ss2
where ss1.c2 = 0;

--
-- test successful handling of full join underneath left join (bug #14105)
--

--Testcase 227:
explain (costs off)
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--Testcase 228:
select * from
  (select 1 as id) as xx
  left join
    (tenk1 as a1 full join (select 1 as id) as yy on (a1.unique1 = yy.id))
  on (xx.id = coalesce(yy.id));

--
-- test ability to push constants through outer join clauses
--

--Testcase 229:
explain (costs off)
  select * from int4_tbl a left join tenk1 b on f1 = unique2 where f1 = 0;

--Testcase 230:
explain (costs off)
  select * from tenk1 a full join tenk1 b using(unique2) where unique2 = 42;

--
-- test that quals attached to an outer join have correct semantics,
-- specifically that they don't re-use expressions computed below the join;
-- we force a mergejoin so that coalesce(b.q1, 1) appears as a join input
--

set enable_hashjoin to off;
set enable_nestloop to off;

--Testcase 231:
explain (verbose, costs off)
  select a.q2, b.q1
    from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
    where coalesce(b.q1, 1) > 0;
--Testcase 232:
select a.q2, b.q1
  from int8_tbl a left join int8_tbl b on a.q2 = coalesce(b.q1, 1)
  where coalesce(b.q1, 1) > 0;

reset enable_hashjoin;
reset enable_nestloop;

--
-- test join removal
--

begin;

--Testcase 444:
CREATE FOREIGN TABLE a3 (id int OPTIONS (key 'true'), b_id int) SERVER sqlite_svr;
--Testcase 445:
CREATE FOREIGN TABLE b3 (id int OPTIONS (key 'true'), c_id int) SERVER sqlite_svr;
--Testcase 446:
CREATE FOREIGN TABLE c3 (id int OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 447:
CREATE FOREIGN TABLE d3 (a int, b int) SERVER sqlite_svr;
--Testcase 233:
INSERT INTO a3 VALUES (0, 0), (1, NULL);
--Testcase 234:
INSERT INTO b3 VALUES (0, 0), (1, NULL);
--Testcase 235:
INSERT INTO c3 VALUES (0), (1);
--Testcase 236:
INSERT INTO d3 VALUES (1,3), (2,2), (3,1);

-- all three cases should be optimizable into a3 simple seqscan
--Testcase 237:
explain (costs off) SELECT a3.* FROM a3 LEFT JOIN b3 ON a3.b_id = b3.id;
--Testcase 238:
explain (costs off) SELECT b3.* FROM b3 LEFT JOIN c3 ON b3.c_id = c3.id;
--Testcase 239:
explain (costs off)
  SELECT a3.* FROM a3 LEFT JOIN (b3 left join c3 on b3.c_id = c3.id)
  ON (a3.b_id = b3.id);

-- check optimization of outer join within another special join
--Testcase 240:
explain (costs off)
select id from a3 where id in (
	select b3.id from b3 left join c3 on b3.id = c3.id
);

-- check that join removal works for a left join when joining a subquery
-- that is guaranteed to be unique by its GROUP BY clause
--Testcase 241:
explain (costs off)
select d3.* from d3 left join (select * from b3 group by b3.id, b3.c_id) s
  on d3.a = s.id and d3.b = s.c_id;

-- similarly, but keying off a DISTINCT clause
--Testcase 242:
explain (costs off)
select d3.* from d3 left join (select distinct * from b3) s
  on d3.a = s.id and d3.b = s.c_id;

-- join removal is not possible when the GROUP BY contains a column that is
-- not in the join condition.  (Note: as of 9.6, we notice that b3.id is a
-- primary key and so drop b3.c_id from the GROUP BY of the resulting plan;
-- but this happens too late for join removal in the outer plan level.)
--Testcase 243:
explain (costs off)
select d3.* from d3 left join (select * from b3 group by b3.id, b3.c_id) s
  on d3.a = s.id;

-- similarly, but keying off a DISTINCT clause
--Testcase 244:
explain (costs off)
select d3.* from d3 left join (select distinct * from b3) s
  on d3.a = s.id;

-- check join removal works when uniqueness of the join condition is enforced
-- by a UNION
--Testcase 245:
explain (costs off)
select d3.* from d3 left join (select id from a3 union select id from b3) s
  on d3.a = s.id;

-- check join removal with a cross-type comparison operator
--Testcase 246:
explain (costs off)
select i8.* from int8_tbl i8 left join (select f1 from int4_tbl group by f1) i4
  on i8.q1 = i4.f1;

-- check join removal with lateral references
--Testcase 247:
explain (costs off)
select 1 from (select a3.id FROM a3 left join b3 on a3.b_id = b3.id) q,
			  lateral generate_series(1, q.id) gs(i) where q.id = gs.i;

rollback;

--Testcase 448:
create foreign table parent (k int options (key 'true'), pd int) server sqlite_svr;
--Testcase 449:
create foreign table child (k int options (key 'true'), cd int) server sqlite_svr;
--Testcase 248:
insert into parent values (1, 10), (2, 20), (3, 30);
--Testcase 249:
insert into child values (1, 100), (4, 400);

-- this case is optimizable
--Testcase 250:
select p.* from parent p left join child c on (p.k = c.k);
--Testcase 251:
explain (costs off)
  select p.* from parent p left join child c on (p.k = c.k);

-- this case is not
--Testcase 252:
select p.*, linked from parent p
  left join (select c.*, true as linked from child c) as ss
  on (p.k = ss.k);
--Testcase 253:
explain (costs off)
  select p.*, linked from parent p
    left join (select c.*, true as linked from child c) as ss
    on (p.k = ss.k);

-- check for a 9.0rc1 bug: join removal breaks pseudoconstant qual handling
--Testcase 254:
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;
--Testcase 255:
explain (costs off)
select p.* from
  parent p left join child c on (p.k = c.k)
  where p.k = 1 and p.k = 2;

--Testcase 256:
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;
--Testcase 257:
explain (costs off)
select p.* from
  (parent p left join child c on (p.k = c.k)) join parent x on p.k = x.k
  where p.k = 1 and p.k = 2;

-- bug 5255: this is not optimizable by join removal
begin;

--Testcase 450:
CREATE FOREIGN TABLE a4 (id int OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 451:
CREATE FOREIGN TABLE b4 (id int OPTIONS (key 'true'), a_id int) SERVER sqlite_svr;
--Testcase 258:
INSERT INTO a4 VALUES (0), (1);
--Testcase 259:
INSERT INTO b4 VALUES (0, 0), (1, NULL);

--Testcase 260:
SELECT * FROM b4 LEFT JOIN a4 ON (b4.a_id = a4.id) WHERE (a4.id IS NULL OR a4.id > 0);
--Testcase 261:
SELECT b4.* FROM b4 LEFT JOIN a4 ON (b4.a_id = a4.id) WHERE (a4.id IS NULL OR a4.id > 0);

rollback;

-- another join removal bug: this is not optimizable, either
begin;

--Testcase 452:
create foreign table innertab (id int8 options (key 'true'), dat1 int8) server sqlite_svr;
--Testcase 262:
insert into innertab values(123, 42);

--Testcase 263:
SELECT * FROM
    (SELECT 1 AS x) ss1
  LEFT JOIN
    (SELECT q1, q2, COALESCE(dat1, q1) AS y
     FROM int8_tbl LEFT JOIN innertab ON q2 = id) ss2
  ON true;

rollback;

-- another join removal bug: we must clean up correctly when removing a PHV
begin;

--Testcase 453:
create foreign table uniquetbl (f1 text) server sqlite_svr;

--Testcase 264:
explain (costs off)
select t1.* from
  uniquetbl as t1
  left join (select *, '***'::text as d1 from uniquetbl) t2
  on t1.f1 = t2.f1
  left join uniquetbl t3
  on t2.d1 = t3.f1;

--Testcase 265:
explain (costs off)
select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

--Testcase 266:
select t0.*
from
 text_tbl t0
 left join
   (select case t1.ten when 0 then 'doh!'::text else null::text end as case1,
           t1.stringu2
     from tenk1 t1
     join int4_tbl i4 ON i4.f1 = t1.unique2
     left join uniquetbl u1 ON u1.f1 = t1.string4) ss
  on t0.f1 = ss.case1
where ss.stringu2 !~* ss.case1;

rollback;

-- bug #8444: we've historically allowed duplicate aliases within aliased JOINs

--Testcase 267:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = f1; -- error
--Testcase 268:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y) j on q1 = y.f1; -- error
--Testcase 269:
select * from
  int8_tbl x join (int4_tbl x cross join int4_tbl y(ff)) j on q1 = f1; -- ok

--
-- Test hints given on incorrect column references are useful
--

--Testcase 270:
select t1.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t1" suggestion
--Testcase 271:
select t2.uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, prefer "t2" suggestion
--Testcase 272:
select uunique1 from
  tenk1 t1 join tenk2 t2 on t1.two = t2.two; -- error, suggest both at once

--
-- Take care to reference the correct RTE
--

--Testcase 454:
select atts.relid::regclass, s.* from pg_stats s join
    pg_attribute a on s.attname = a.attname and s.tablename =
    a.attrelid::regclass::text join (select unnest(indkey) attnum,
    indexrelid from pg_index i) atts on atts.attnum = a.attnum where
    schemaname != 'pg_catalog';

--
-- Test LATERAL
--

--Testcase 273:
select unique2, x.*
from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
--Testcase 274:
explain (costs off)
  select unique2, x.*
  from tenk1 a, lateral (select * from int4_tbl b where f1 = a.unique1) x;
--Testcase 275:
select unique2, x.*
from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
--Testcase 276:
explain (costs off)
  select unique2, x.*
  from int4_tbl x, lateral (select unique2 from tenk1 where f1 = unique1) ss;
--Testcase 277:
explain (costs off)
  select unique2, x.*
  from int4_tbl x cross join lateral (select unique2 from tenk1 where f1 = unique1) ss;
--Testcase 278:
select unique2, x.*
from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;
--Testcase 279:
explain (costs off)
  select unique2, x.*
  from int4_tbl x left join lateral (select unique1, unique2 from tenk1 where f1 = unique1) ss on true;

-- check scoping of lateral versus parent references
-- the first of these should return int8_tbl.q2, the second int8_tbl.q1
--Testcase 280:
select *, (select r from (select q1 as q2) x, (select q2 as r) y) from int8_tbl;
--Testcase 281:
select *, (select r from (select q1 as q2) x, lateral (select q2 as r) y) from int8_tbl;

-- lateral with function in FROM
--Testcase 282:
select count(*) from tenk1 a, lateral generate_series(1,two) g;
--Testcase 283:
explain (costs off)
  select count(*) from tenk1 a, lateral generate_series(1,two) g;
--Testcase 284:
explain (costs off)
  select count(*) from tenk1 a cross join lateral generate_series(1,two) g;
-- don't need the explicit LATERAL keyword for functions
--Testcase 285:
explain (costs off)
  select count(*) from tenk1 a, generate_series(1,two) g;

-- lateral with UNION ALL subselect
--Testcase 286:
explain (costs off)
  select * from generate_series(100,200) g,
    lateral (select * from int8_tbl a where g = q1 union all
             select * from int8_tbl b where g = q2) ss;
--Testcase 287:
select * from generate_series(100,200) g,
  lateral (select * from int8_tbl a where g = q1 union all
           select * from int8_tbl b where g = q2) ss;

-- lateral with VALUES
--Testcase 288:
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;
--Testcase 289:
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1)) ss(x) on b.unique2 = ss.x;

-- lateral with VALUES, no flattening possible
--Testcase 290:
explain (costs off)
  select count(*) from tenk1 a,
    tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;
--Testcase 291:
select count(*) from tenk1 a,
  tenk1 b join lateral (values(a.unique1),(-1)) ss(x) on b.unique2 = ss.x;

-- lateral injecting a strange outer join condition
--Testcase 292:
explain (costs off)
  select * from int8_tbl a,
    int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
      on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;
--Testcase 293:
select * from int8_tbl a,
  int8_tbl x left join lateral (select a.q1 from int4_tbl y) ss(z)
    on x.q2 = ss.z
  order by a.q1, a.q2, x.q1, x.q2, ss.z;

-- lateral reference to a join alias variable
--Testcase 294:
select * from (select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (select x) ss2(y);
--Testcase 295:
select * from (select f1 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1,
  lateral (values(x)) ss2(y);
--Testcase 296:
select * from ((select f1/2 as x from int4_tbl) ss1 join int4_tbl i4 on x = f1) j,
  lateral (select x) ss2(y);

-- lateral references requiring pullup
--Testcase 297:
select * from (values(1)) x(lb),
  lateral generate_series(lb,4) x4;
--Testcase 298:
select * from (select f1/1000000000 from int4_tbl) x(lb),
  lateral generate_series(lb,4) x4;
--Testcase 299:
select * from (values(1)) x(lb),
  lateral (values(lb)) y(lbcopy);
--Testcase 300:
select * from (values(1)) x(lb),
  lateral (select lb from int4_tbl) y(lbcopy);
--Testcase 301:
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (values(x.q1,y.q1,y.q2)) v(xq1,yq1,yq2);
--Testcase 302:
select * from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
--Testcase 303:
select x.* from
  int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1,
  lateral (select x.q1,y.q1,y.q2) v(xq1,yq1,yq2);
--Testcase 304:
select v.* from
  (int8_tbl x left join (select q1,coalesce(q2,0) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
--Testcase 305:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 union all select x.q2,y.q2) v(vx,vy);
--Testcase 307:
select v.* from
  (int8_tbl x left join (select q1,(select coalesce(q2,0)) q2 from int8_tbl) y on x.q2 = y.q1)
  left join int4_tbl z on z.f1 = x.q2,
  lateral (select x.q1,y.q1 from onerow union all select x.q2,y.q2 from onerow) v(vx,vy);

-- Error when using sub-query with multi instances of table, this issue is fixed on PostgreSQL-12
--Testcase 455:
explain (verbose, costs off)
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 456:
select * from
  int8_tbl a left join
  lateral (select *, a.q2 as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 457:
explain (verbose, costs off)
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;
--Testcase 458:
select * from
  int8_tbl a left join
  lateral (select *, coalesce(a.q2, 42) as x from int8_tbl b) ss on a.q2 = ss.q1;

-- lateral can result in join conditions appearing below their
-- real semantic level
--Testcase 308:
explain (verbose, costs off)
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 309:
select * from int4_tbl i left join
  lateral (select * from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 310:
explain (verbose, costs off)
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 311:
select * from int4_tbl i left join
  lateral (select coalesce(i) from int2_tbl j where i.f1 = j.f1) k on true;
--Testcase 312:
explain (verbose, costs off)
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;
--Testcase 313:
select * from int4_tbl a,
  lateral (
    select * from int4_tbl b left join int8_tbl c on (b.f1 = q1 and a.f1 = q2)
  ) ss;

-- lateral reference in a PlaceHolderVar evaluated at join level
-- Error when using sub-query with multi instances of table, this issue is fixed on PostgreSQL-12
--Testcase 459:
explain (verbose, costs off)
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;
--Testcase 460:
select * from
  int8_tbl a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   int8_tbl b cross join int8_tbl c) ss
  on a.q2 = ss.bq1;

-- case requiring nested PlaceHolderVars
--Testcase 461:
explain (verbose, costs off)
select * from
  int8_tbl c left join (
    int8_tbl a left join (select q1, coalesce(q2,42) as x from int8_tbl b) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select ss2.y offset 0) ss3;

-- case that breaks the old ph_may_need optimization
--Testcase 462:
explain (verbose, costs off)
select c.*,a.*,ss1.q1,ss2.q1,ss3.* from
  int8_tbl c left join (
    int8_tbl a left join
      (select q1, coalesce(q2,f1) as x from int8_tbl b, int4_tbl b2
       where q1 < f1) ss1
      on a.q2 = ss1.q1
    cross join
    lateral (select q1, coalesce(ss1.x,q2) as y from int8_tbl d) ss2
  ) on c.q2 = ss2.q1,
  lateral (select * from int4_tbl i where ss2.y > f1) ss3;

-- check processing of postponed quals (bug #9041)
--Testcase 463:
delete from sub_tbl;
--Testcase 464:
insert into sub_tbl values (1, 2, 3, 4, 5);
--Testcase 465:
explain (verbose, costs off)
select * from
  (select key1 as x  from sub_tbl offset 0) x cross join (select key3 as y from sub_tbl offset 0) y
  left join lateral (
    select * from (select key5 as z from sub_tbl offset 0) z where z.z = x.x
  ) zz on zz.z = y.y;

-- check dummy rels with lateral references (bug #15694)
--Testcase 466:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl where false) ss on true;
--Testcase 467:
explain (verbose, costs off)
select * from int8_tbl i8 left join lateral
  (select *, i8.q2 from int4_tbl i1, int4_tbl i2 where false) ss on true;

-- check handling of nested appendrels inside LATERAL
--Testcase 468:
select * from
  ((select key3 as v from sub_tbl) union all (select key5 as v from sub_tbl)) as q1
  cross join lateral
  ((select * from
      ((select key6 as v from sub_tbl) union all (select value1 as v from sub_tbl)) as q3)
   union all
   (select q1.v)
  ) as q2;

-- check we don't try to do a unique-ified semijoin with LATERAL
--Testcase 314:
explain (verbose, costs off)
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;
--Testcase 315:
select * from
  (values (0,9998), (1,1000)) v(id,x),
  lateral (select f1 from int4_tbl
           where f1 = any (select unique1 from tenk1
                           where unique2 = v.x offset 0)) ss;

-- check proper extParam/allParam handling (this isn't exactly a LATERAL issue,
-- but we can make the test case much more compact with LATERAL)
--Testcase 316:
explain (verbose, costs off)
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

--Testcase 317:
select * from (values (0), (1)) v(id),
lateral (select * from int8_tbl t1,
         lateral (select * from
                    (select * from int8_tbl t2
                     where q1 = any (select q2 from int8_tbl t3
                                     where q2 = (select greatest(t1.q1,t2.q2))
                                       and (select v.id=0)) offset 0) ss2) ss
         where t1.q1 = ss.q2) ss0;

-- test some error cases where LATERAL should have been used but wasn't
--Testcase 318:
select f1,g from int4_tbl a, (select f1 as g) ss;
--Testcase 319:
select f1,g from int4_tbl a, (select a.f1 as g) ss;
--Testcase 320:
select f1,g from int4_tbl a cross join (select f1 as g) ss;
--Testcase 321:
select f1,g from int4_tbl a cross join (select a.f1 as g) ss;
-- SQL:2008 says the left table is in scope but illegal to access here
--Testcase 322:
select f1,g from int4_tbl a right join lateral generate_series(0, a.f1) g on true;
--Testcase 323:
select f1,g from int4_tbl a full join lateral generate_series(0, a.f1) g on true;
-- check we complain about ambiguous table references
--Testcase 324:
select * from
  int8_tbl x cross join (int4_tbl x cross join lateral (select x.f1) ss);
-- LATERAL can be used to put an aggregate into the FROM clause of its query
--Testcase 325:
select 1 from tenk1 a, lateral (select max(a.unique1) from int4_tbl b) ss;

-- check behavior of LATERAL in UPDATE/DELETE

--Testcase 469:
create temp table xx1 as select f1 as x1, -f1 as x2 from int4_tbl;

-- error, can't do this:
--Testcase 326:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = x1) ss;
--Testcase 327:
update xx1 set x2 = f1 from (select * from int4_tbl where f1 = xx1.x1) ss;
-- can't do it even with LATERAL:
--Testcase 328:
update xx1 set x2 = f1 from lateral (select * from int4_tbl where f1 = x1) ss;
-- we might in future allow something like this, but for now it's an error:
--Testcase 329:
update xx1 set x2 = f1 from xx1, lateral (select * from int4_tbl where f1 = x1) ss;

-- also errors:
--Testcase 330:
delete from xx1 using (select * from int4_tbl where f1 = x1) ss;
--Testcase 331:
delete from xx1 using (select * from int4_tbl where f1 = xx1.x1) ss;
--Testcase 332:
delete from xx1 using lateral (select * from int4_tbl where f1 = x1) ss;

-- Skip this test, sqlite fdw does not support to create partition table
-- test LATERAL reference propagation down a multi-level inheritance hierarchy
-- produced for a multi-level partitioned table hierarchy.
--
--create table join_pt1 (a int, b int, c varchar) partition by range(a);
--create table join_pt1p1 partition of join_pt1 for values from (0) to (100) partition by range(b);
--create table join_pt1p2 partition of join_pt1 for values from (100) to (200);
--create table join_pt1p1p1 partition of join_pt1p1 for values from (0) to (100);
--insert into join_pt1 values (1, 1, 'x'), (101, 101, 'y');
--create table join_ut1 (a int, b int, c varchar);
--insert into join_ut1 values (101, 101, 'y'), (2, 2, 'z');
--explain (verbose, costs off)
--select t1.b, ss.phv from join_ut1 t1 left join lateral
--              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
--                                          from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
--              on t1.a = ss.t2a order by t1.a;
--select t1.b, ss.phv from join_ut1 t1 left join lateral
--              (select t2.a as t2a, t3.a t3a, least(t1.a, t2.a, t3.a) phv
--                                          from join_pt1 t2 join join_ut1 t3 on t2.a = t3.b) ss
--              on t1.a = ss.t2a order by t1.a;

--drop table join_pt1;
--drop table join_ut1;

--
-- test that foreign key join estimation performs sanely for outer joins
--

begin;

--Testcase 470:
create foreign table fkest (a int options (key 'true'), b int options (key 'true'), c int) server sqlite_svr;
--Testcase 471:
create foreign table fkest1 (a int options (key 'true'), b int options (key 'true')) server sqlite_svr;

--Testcase 333:
insert into fkest select x/10, x%10, x from generate_series(1,1000) x;
--Testcase 334:
insert into fkest1 select x/10, x%10 from generate_series(1,1000) x;

--alter table fkest1
--  add constraint fkest1_a_b_fkey foreign key (a,b) references fkest;

--analyze fkest;
--analyze fkest1;

--Testcase 335:
explain (costs off)
select *
from fkest f
  left join fkest1 f1 on f.a = f1.a and f.b = f1.b
  left join fkest1 f2 on f.a = f2.a and f.b = f2.b
  left join fkest1 f3 on f.a = f3.a and f.b = f3.b
where f.c = 1;

rollback;

--
-- test planner's ability to mark joins as unique
--

--Testcase 472:
create foreign table j11 (id int options (key 'true')) server sqlite_svr;
--Testcase 473:
create foreign table j21 (id int options (key 'true')) server sqlite_svr;
--Testcase 474:
create foreign table j31 (id int) server sqlite_svr;

--Testcase 336:
insert into j11 values(1),(2),(3);
--Testcase 337:
insert into j21 values(1),(2),(3);
--Testcase 338:
insert into j31 values(1),(1);

-- ensure join is properly marked as unique
--Testcase 339:
explain (verbose, costs off)
select * from j11 inner join j21 on j11.id = j21.id;

-- ensure join is not unique when not an equi-join
--Testcase 340:
explain (verbose, costs off)
select * from j11 inner join j21 on j11.id > j21.id;

-- ensure non-unique rel is not chosen as inner
--Testcase 341:
explain (verbose, costs off)
select * from j11 inner join j31 on j11.id = j31.id;

-- ensure left join is marked as unique
--Testcase 342:
explain (verbose, costs off)
select * from j11 left join j21 on j11.id = j21.id;

-- ensure right join is marked as unique
--Testcase 343:
explain (verbose, costs off)
select * from j11 right join j21 on j11.id = j21.id;

-- ensure full join is marked as unique
--Testcase 344:
explain (verbose, costs off)
select * from j11 full join j21 on j11.id = j21.id;

-- a clauseless (cross) join can't be unique
--Testcase 345:
explain (verbose, costs off)
select * from j11 cross join j21;

-- ensure a natural join is marked as unique
--Testcase 346:
explain (verbose, costs off)
select * from j11 natural join j21;

-- ensure a distinct clause allows the inner to become unique
--Testcase 347:
explain (verbose, costs off)
select * from j11
inner join (select distinct id from j31) j31 on j11.id = j31.id;

-- ensure group by clause allows the inner to become unique
--Testcase 348:
explain (verbose, costs off)
select * from j11
inner join (select id from j31 group by id) j31 on j11.id = j31.id;

--drop table j1;
--drop table j2;
--drop table j3;

-- test more complex permutations of unique joins

--Testcase 475:
create foreign table j12 (id1 int options (key 'true'), id2 int options (key 'true')) server sqlite_svr;
--Testcase 476:
create foreign table j22 (id1 int options (key 'true'), id2 int options (key 'true')) server sqlite_svr;
--Testcase 477:
create foreign table j32 (id1 int options (key 'true'), id2 int options (key 'true')) server sqlite_svr;

--Testcase 349:
insert into j12 values(1,1),(1,2);
--Testcase 350:
insert into j22 values(1,1);
--Testcase 351:
insert into j32 values(1,1);

--analyze j1;
--analyze j2;
--analyze j3;

-- ensure there's no unique join when not all columns which are part of the
-- unique index are seen in the join clause
--Testcase 352:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1;

-- ensure proper unique detection with multiple join quals
--Testcase 353:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2;

-- ensure we don't detect the join to be unique when quals are not part of the
-- join condition
--Testcase 354:
explain (verbose, costs off)
select * from j12
inner join j22 on j12.id1 = j22.id1 where j12.id2 = 1;

-- as above, but for left joins.
--Testcase 355:
explain (verbose, costs off)
select * from j12
left join j22 on j12.id1 = j22.id1 where j12.id2 = 1;

-- validate logic in merge joins which skips mark and restore.
-- it should only do this if all quals which were used to detect the unique
-- are present as join quals, and not plain quals.
set enable_nestloop to 0;
set enable_hashjoin to 0;
set enable_sort to 0;
-- skip, cannot create index on foreign table
-- create indexes that will be preferred over the PKs to perform the join
--create index j1_id1_idx on j1 (id1) where id1 % 1000 = 1;
--create index j2_id1_idx on j2 (id1) where id1 % 1000 = 1;
-- need an additional row in j2, if we want j2_id1_idx to be preferred
--Testcase 478:
insert into j22 values(1,2);
--analyze j2;

--Testcase 356:
explain (costs off) select * from j12 j12
inner join j12 j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1;

--Testcase 357:
select * from j12 j12
inner join j12 j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1;

-- Exercise array keys mark/restore B-Tree code
--Testcase 479:
explain (costs off) select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 = any (array[1]);

--Testcase 480:
select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 = any (array[1]);

-- Exercise array keys "find extreme element" B-Tree code
--Testcase 481:
explain (costs off) select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 >= any (array[1,5]);

--Testcase 482:
select * from j12
inner join j22 on j12.id1 = j22.id1 and j12.id2 = j22.id2
where j12.id1 % 1000 = 1 and j22.id1 % 1000 = 1 and j22.id1 >= any (array[1,5]);

reset enable_nestloop;
reset enable_hashjoin;
reset enable_sort;

--drop table j1;
--drop table j2;
--drop table j3;

-- check that semijoin inner is not seen as unique for a portion of the outerrel
--Testcase 483:
CREATE FOREIGN TABLE onek (
  unique1   int4 OPTIONS (key 'true'),
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

--Testcase 358:
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from tenk1 t3
              where t3.thousand = t1.unique1 and t3.tenthous = t2.hundred)
      and t1.unique1 < 1;

-- ... unless it actually is unique
--Testcase 484:
create table j3 as select unique1, tenthous from onek;
vacuum analyze j3;
--Testcase 485:
create unique index on j3(unique1, tenthous);

--Testcase 359:
explain (verbose, costs off)
select t1.unique1, t2.hundred
from onek t1, tenk1 t2
where exists (select 1 from j3
              where j3.unique1 = t1.unique1 and j3.tenthous = t2.hundred)
      and t1.unique1 < 1;

--Testcase 486:
drop table j3;

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
--Testcase 487:
DROP SERVER sqlite_svr;
--Testcase 488:
DROP EXTENSION duckdb_fdw CASCADE;
