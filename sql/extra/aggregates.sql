--
-- AGGREGATES
--
--Testcase 266:
CREATE EXTENSION duckdb_fdw;
--Testcase 267:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test_core.db');
--Testcase 268:
CREATE FOREIGN TABLE onek(
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

--Testcase 269:
CREATE FOREIGN TABLE aggtest (
  a       int2,
  b     float4
) SERVER sqlite_svr;

--Testcase 270:
CREATE FOREIGN TABLE student (
  name    text,
  age     int4,
  location  point,
  gpa     float8
) SERVER sqlite_svr;

--Testcase 271:
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

--Testcase 272:
CREATE FOREIGN TABLE INT8_TBL(
  q1 int8 OPTIONS (key 'true'),
  q2 int8 OPTIONS (key 'true')
) SERVER sqlite_svr;

--Testcase 273:
CREATE FOREIGN TABLE INT4_TBL(f1 int4 OPTIONS (key 'true')) SERVER sqlite_svr; 

--Testcase 274:
CREATE FOREIGN TABLE multi_arg_agg (a int OPTIONS (key 'true'), b int, c text) SERVER sqlite_svr;

--Testcase 275:
CREATE FOREIGN TABLE VARCHAR_TBL(f1 varchar(4) OPTIONS (key 'true')) SERVER sqlite_svr;

--Testcase 276:
CREATE FOREIGN TABLE FLOAT8_TBL(f1 float8 OPTIONS (key 'true')) SERVER sqlite_svr;

-- avoid bit-exact output here because operations may not be bit-exact.
SET extra_float_digits = 0;
--Testcase 1:
SELECT avg(four) AS avg_1 FROM onek;

--Testcase 2:
SELECT avg(a) AS avg_32 FROM aggtest WHERE a < 100;

-- In 7.1, avg(float4) is computed using float8 arithmetic.
--Testcase 3:
-- Round the result to limited digits to avoid platform-specific results.
SELECT avg(b)::numeric(10,3) AS avg_107_943 FROM aggtest;

--Testcase 4:
-- Round the result to limited digits to avoid platform-specific results.
SELECT avg(gpa)::numeric(10,3) AS avg_3_4 FROM ONLY student;


--Testcase 5:
SELECT sum(four) AS sum_1500 FROM onek;
--Testcase 6:
SELECT sum(a) AS sum_198 FROM aggtest;
--Testcase 7:
-- Round the result to limited digits to avoid platform-specific results.
SELECT sum(b)::numeric(10,3) AS avg_431_773 FROM aggtest;
--Testcase 8:
-- Round the result to limited digits to avoid platform-specific results.
SELECT sum(gpa)::numeric(10,3) AS avg_6_8 FROM ONLY student;

--Testcase 9:
SELECT max(four) AS max_3 FROM onek;
--Testcase 10:
SELECT max(a) AS max_100 FROM aggtest;
--Testcase 11:
SELECT max(aggtest.b) AS max_324_78 FROM aggtest;
--Testcase 12:
SELECT max(student.gpa) AS max_3_7 FROM student;

--Testcase 13:
-- Round the result to limited digits to avoid platform-specific results.
SELECT stddev_pop(b)::numeric(20,10) FROM aggtest;
--Testcase 14:
-- Round the result to limited digits to avoid platform-specific results.
SELECT stddev_samp(b)::numeric(20,10) FROM aggtest;
--Testcase 15:
-- Round the result to limited digits to avoid platform-specific results.
SELECT var_pop(b)::numeric(20,10) FROM aggtest;
--Testcase 16:
-- Round the result to limited digits to avoid platform-specific results.
SELECT var_samp(b)::numeric(20,10) FROM aggtest;

--Testcase 17:
SELECT stddev_pop(b::numeric) FROM aggtest;
--Testcase 18:
SELECT stddev_samp(b::numeric) FROM aggtest;
--Testcase 19:
SELECT var_pop(b::numeric) FROM aggtest;
--Testcase 20:
SELECT var_samp(b::numeric) FROM aggtest;

-- population variance is defined for a single tuple, sample variance
-- is not
--Testcase 277:
CREATE FOREIGN TABLE agg_t3(a float8, b float8, id integer OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 278:
DELETE FROM agg_t3;
--Testcase 279:
INSERT INTO agg_t3 values (1.0::float8, 2.0::float8);
--Testcase 280:
SELECT var_pop(a), var_samp(b) FROM agg_t3;

--Testcase 281:
DELETE FROM agg_t3;
--Testcase 282:
INSERT INTO agg_t3 values (3.0::float8, 4.0::float8);
--Testcase 283:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t3;

--Testcase 284:
DELETE FROM agg_t3;
--Testcase 285:
INSERT INTO agg_t3 values ('inf'::float8, 'inf'::float8);
--Testcase 286:
SELECT var_pop(a), var_samp(b) FROM agg_t3;
--Testcase 287:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t3;

--Testcase 288:
DELETE FROM agg_t3;
--Testcase 289:
INSERT INTO agg_t3 values ('nan'::float8, 'nan'::float8);
--Testcase 290:
SELECT var_pop(a), var_samp(b) FROM agg_t3;
--Testcase 291:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t3;

--Testcase 292:
CREATE FOREIGN TABLE agg_t4(a float4, b float4, id integer OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 293:
DELETE FROM agg_t4;
--Testcase 294:
INSERT INTO agg_t4 values (1.0::float4, 2.0::float4);
--Testcase 295:
SELECT var_pop(a), var_samp(b) FROM agg_t4;

--Testcase 296:
DELETE FROM agg_t4;
--Testcase 297:
INSERT INTO agg_t4 values (3.0::float4, 4.0::float4);
--Testcase 298:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t4;

--Testcase 299:
DELETE FROM agg_t4;
--Testcase 300:
INSERT INTO agg_t4 values ('inf'::float4, 'inf'::float4);
--Testcase 301:
SELECT var_pop(a), var_samp(b) FROM agg_t4;
--Testcase 302:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t4;

--Testcase 303:
DELETE FROM agg_t4;
--Testcase 304:
INSERT INTO agg_t4 values ('nan'::float4, 'nan'::float4);
--Testcase 305:
SELECT var_pop(a), var_samp(b) FROM agg_t4;
--Testcase 306:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t4;

--Testcase 307:
CREATE FOREIGN TABLE agg_t5(a numeric, b numeric, id integer OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 308:
DELETE FROM agg_t5;
--Testcase 309:
INSERT INTO agg_t5 values (1.0::numeric, 2.0::numeric);
--Testcase 310:
SELECT var_pop(a), var_samp(b) FROM agg_t5;

--Testcase 311:
DELETE FROM agg_t5;
--Testcase 312:
INSERT INTO agg_t5 values (3.0::numeric, 4.0::numeric);
--Testcase 313:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t5;

--Testcase 314:
DELETE FROM agg_t5;
--Testcase 315:
INSERT INTO agg_t5 values ('nan'::numeric, 'nan'::numeric);
--Testcase 316:
SELECT var_pop(a), var_samp(b) FROM agg_t5;
--Testcase 317:
SELECT stddev_pop(a), stddev_samp(b) FROM agg_t5;

-- verify correct results for null and NaN inputs
--Testcase 318:
CREATE FOREIGN TABLE agg_t8(a text OPTIONS (key 'true'), b text) SERVER sqlite_svr;
--Testcase 319:
DELETE FROM agg_t8;
--Testcase 320:
INSERT INTO agg_t8 select * from generate_series(1,3);
--Testcase 321:
select sum(null::int4) from agg_t8;
--Testcase 322:
select sum(null::int8) from agg_t8;
--Testcase 323:
select sum(null::numeric) from agg_t8;
--Testcase 324:
select sum(null::float8) from agg_t8;
--Testcase 325:
select avg(null::int4) from agg_t8;
--Testcase 326:
select avg(null::int8) from agg_t8;
--Testcase 327:
select avg(null::numeric) from agg_t8;
--Testcase 328:
select avg(null::float8) from agg_t8;
--Testcase 329:
select sum('NaN'::numeric) from agg_t8;
--Testcase 330:
select avg('NaN'::numeric) from agg_t8;

-- verify correct results for infinite inputs
--Testcase 331:
DELETE FROM agg_t3;
--Testcase 332:
INSERT INTO agg_t3 VALUES ('1'::float8), ('infinity'::float8);
--Testcase 333:
SELECT avg(a), var_pop(a) FROM agg_t3;

--Testcase 334:
DELETE FROM agg_t3;
--Testcase 335:
INSERT INTO agg_t3 VALUES ('infinity'::float8), ('1'::float8);
--Testcase 336:
SELECT avg(a), var_pop(a) FROM agg_t3;

--Testcase 337:
DELETE FROM agg_t3;
--Testcase 338:
INSERT INTO agg_t3 VALUES ('infinity'::float8), ('infinity'::float8);
--Testcase 339:
SELECT avg(a), var_pop(a) FROM agg_t3;

--Testcase 340:
DELETE FROM agg_t3;
--Testcase 341:
INSERT INTO agg_t3 VALUES ('-infinity'::float8), ('infinity'::float8);
--Testcase 342:
SELECT avg(a), var_pop(a) FROM agg_t3;

-- test accuracy with a large input offset
--Testcase 343:
CREATE FOREIGN TABLE agg_t6(a float8, id integer OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 344:
DELETE FROM agg_t6;
--Testcase 345:
INSERT INTO agg_t6 VALUES (100000003), (100000004), (100000006), (100000007);
--Testcase 346:
SELECT avg(a), var_pop(a) FROM agg_t6;

--Testcase 347:
DELETE FROM agg_t6;
--Testcase 348:
INSERT INTO agg_t6 VALUES (7000000000005), (7000000000007);
--Testcase 349:
SELECT avg(a), var_pop(a) FROM agg_t6;

-- SQL2003 binary aggregates
--Testcase 21:
SELECT regr_count(b, a) FROM aggtest;
--Testcase 22:
SELECT regr_sxx(b, a) FROM aggtest;
--Testcase 23:
-- Round the result to limited digits to avoid platform-specific results.
SELECT regr_syy(b, a)::numeric(20,10) FROM aggtest;
--Testcase 24:
-- Round the result to limited digits to avoid platform-specific results.
SELECT regr_sxy(b, a)::numeric(20,10) FROM aggtest;
--Testcase 25:
-- Round the result to limited digits to avoid platform-specific results.
SELECT regr_avgx(b, a), regr_avgy(b, a)::numeric(20,10) FROM aggtest;
--Testcase 26:
-- Round the result to limited digits to avoid platform-specific results.
SELECT regr_r2(b, a)::numeric(20,10) FROM aggtest;
--Testcase 27:
-- Round the result to limited digits to avoid platform-specific results.
SELECT regr_slope(b, a)::numeric(20,10), regr_intercept(b, a)::numeric(20,10) FROM aggtest;
--Testcase 28:
-- Round the result to limited digits to avoid platform-specific results.
SELECT covar_pop(b, a)::numeric(20,10), covar_samp(b, a)::numeric(20,10) FROM aggtest;
--Testcase 29:
-- Round the result to limited digits to avoid platform-specific results.
SELECT corr(b, a)::numeric(20,10) FROM aggtest;

-- check single-tuple behavior
--Testcase 350:
CREATE FOREIGN TABLE agg_t7(a float8, b float8, c float8, d float8, id integer OPTIONS (key 'true')) SERVER sqlite_svr;
--Testcase 351:
DELETE FROM agg_t7;
--Testcase 352:
INSERT INTO agg_t7 VALUES (1, 2, 3, 4);
--Testcase 353:
SELECT covar_pop(a,b), covar_samp(c,d) FROM agg_t7;

--Testcase 354:
DELETE FROM agg_t7;
--Testcase 355:
INSERT INTO agg_t7 VALUES (1, 'inf', 3, 'inf');
--Testcase 356:
SELECT covar_pop(a,b), covar_samp(c,d) FROM agg_t7;

--Testcase 357:
DELETE FROM agg_t7;
--Testcase 358:
INSERT INTO agg_t7 VALUES (1, 'nan', 3, 'nan');
--Testcase 359:
SELECT covar_pop(a,b), covar_samp(c,d) FROM agg_t7;

-- test accum and combine functions directly
--Testcase 360:
CREATE FOREIGN TABLE regr_test (x float8, y float8, id int options (key 'true')) SERVER sqlite_svr;
--Testcase 361:
DELETE FROM regr_test;
--Testcase 362:
INSERT INTO regr_test VALUES (10,150),(20,250),(30,350),(80,540),(100,200);
--Testcase 363:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30,80);
--Testcase 364:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test;

--Testcase 365:
CREATE FOREIGN TABLE agg_t15 (a text, b int, c int, id int options (key 'true')) SERVER sqlite_svr;
--Testcase 366:
delete from agg_t15;
--Testcase 367:
insert into agg_t15 values ('{4,140,2900}', 100);
--Testcase 368:
SELECT float8_accum(a::float8[], b) from agg_t15;

--Testcase 369:
delete from agg_t15;
--Testcase 370:
insert into agg_t15 values ('{4,140,2900,1290,83075,15050}', 200, 100);
--Testcase 371:
SELECT float8_regr_accum(a::float8[], b, c) from agg_t15;

--Testcase 372:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (10,20,30);

--Testcase 373:
SELECT count(*), sum(x), regr_sxx(y,x), sum(y),regr_syy(y,x), regr_sxy(y,x)
FROM regr_test WHERE x IN (80,100);

--Testcase 374:
CREATE FOREIGN TABLE agg_t16 (a text, b text, id int options (key 'true')) SERVER sqlite_svr;
--Testcase 375:
delete from agg_t16;
--Testcase 376:
insert into agg_t16 values ('{3,60,200}', '{0,0,0}');
--Testcase 377:
insert into agg_t16 values ('{0,0,0}', '{2,180,200}');
--Testcase 378:
insert into agg_t16 values ('{3,60,200}', '{2,180,200}');
--Testcase 379:
SELECT float8_combine(a::float8[], b::float8[]) FROM agg_t16;

--Testcase 380:
delete from agg_t16;
--Testcase 381:
insert into agg_t16 values ('{3,60,200,750,20000,2000}', '{0,0,0,0,0,0}');
--Testcase 382:
insert into agg_t16 values ('{0,0,0,0,0,0}', '{2,180,200,740,57800,-3400}');
--Testcase 383:
insert into agg_t16 values ('{3,60,200,750,20000,2000}', '{2,180,200,740,57800,-3400}');
--Testcase 384:
SELECT float8_regr_combine(a::float8[], b::float8[]) FROM agg_t16;

--Testcase 385:
DROP FOREIGN TABLE regr_test;

-- test count, distinct
--Testcase 30:
SELECT count(four) AS cnt_1000 FROM onek;
--Testcase 31:
SELECT count(DISTINCT four) AS cnt_4 FROM onek;

--Testcase 32:
select ten, count(*), sum(four) from onek
group by ten order by ten;

--Testcase 33:
select ten, count(four), sum(DISTINCT four) from onek
group by ten order by ten;

-- user-defined aggregates
--Testcase 386:
CREATE AGGREGATE newavg (
   sfunc = int4_avg_accum, basetype = int4, stype = _int8,
   finalfunc = int8_avg,
   initcond1 = '{0,0}'
);

--Testcase 387:
CREATE AGGREGATE newsum (
   sfunc1 = int4pl, basetype = int4, stype1 = int4,
   initcond1 = '0'
);

--Testcase 388:
CREATE AGGREGATE newcnt (*) (
   sfunc = int8inc, stype = int8,
   initcond = '0', parallel = safe
);

--Testcase 389:
CREATE AGGREGATE newcnt ("any") (
   sfunc = int8inc_any, stype = int8,
   initcond = '0'
);

--Testcase 390:
CREATE AGGREGATE oldcnt (
   sfunc = int8inc, basetype = 'ANY', stype = int8,
   initcond = '0'
);

--Testcase 391:
create function sum3(int8,int8,int8) returns int8 as
'select $1 + $2 + $3' language sql strict immutable;

--Testcase 392:
create aggregate sum2(int8,int8) (
   sfunc = sum3, stype = int8,
   initcond = '0'
);

--Testcase 34:
SELECT newavg(four) AS avg_1 FROM onek;
--Testcase 35:
SELECT newsum(four) AS sum_1500 FROM onek;
--Testcase 36:
SELECT newcnt(four) AS cnt_1000 FROM onek;
--Testcase 37:
SELECT newcnt(*) AS cnt_1000 FROM onek;
--Testcase 38:
SELECT oldcnt(*) AS cnt_1000 FROM onek;
--Testcase 39:
SELECT sum2(q1,q2) FROM int8_tbl;

-- test for outer-level aggregates

-- this should work
--Testcase 40:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

-- this should fail because subquery has an agg of its own in WHERE
--Testcase 41:
select ten, sum(distinct four) from onek a
group by ten
having exists (select 1 from onek b
               where sum(distinct a.four + b.four) = b.four);

-- Test handling of sublinks within outer-level aggregates.
-- Per bug report from Daniel Grace.
--Testcase 42:
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1)))
from tenk1 o;

-- Test handling of Params within aggregate arguments in hashed aggregation.
-- Per bug report from Jeevan Chalke.
--Testcase 393:
explain (verbose, costs off)
select s1, s2, sm
from generate_series(1, 3) s1,
     lateral (select s2, sum(s1 + s2) sm
              from generate_series(1, 3) s2 group by s2) ss
order by 1, 2;

--Testcase 394:
select s1, s2, sm
from generate_series(1, 3) s1,
     lateral (select s2, sum(s1 + s2) sm
              from generate_series(1, 3) s2 group by s2) ss
order by 1, 2;

--Testcase 395:
explain (verbose, costs off)
select array(select sum(x+y) s
            from generate_series(1,3) y group by y order by s)
  from generate_series(1,3) x;

--Testcase 396:
select array(select sum(x+y) s
            from generate_series(1,3) y group by y order by s)
  from generate_series(1,3) x;

--
-- test for bitwise integer aggregates
--
--Testcase 397:
CREATE FOREIGN TABLE bitwise_test(
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2
) SERVER sqlite_svr;

-- empty case
--Testcase 43:
SELECT
  BIT_AND(i2) AS "?",
  BIT_OR(i4)  AS "?"
FROM bitwise_test;

--Testcase 44:
INSERT INTO bitwise_test VALUES
  (1, 1, 1, 1, 1),
  (3, 3, 3, null, 2),
  (7, 7, 7, 3, 4);

--Testcase 45:
SELECT
  BIT_AND(i2) AS "1",
  BIT_AND(i4) AS "1",
  BIT_AND(i8) AS "1",
  BIT_AND(i)  AS "?",
  BIT_AND(x)  AS "0",

  BIT_OR(i2)  AS "7",
  BIT_OR(i4)  AS "7",
  BIT_OR(i8)  AS "7",
  BIT_OR(i)   AS "?",
  BIT_OR(x)   AS "7"
FROM bitwise_test;

--
-- test boolean aggregates
--
-- first test all possible transition and final states

--Testcase 398:
CREATE FOREIGN TABLE bool_test_tmp(
  b1 BOOL OPTIONS (key 'true'),
  b2 BOOL OPTIONS (key 'true')
) SERVER sqlite_svr;

-- boolean and transitions
-- null because strict
BEGIN;
--Testcase 399:
INSERT INTO bool_test_tmp VALUES
  (NULL, NULL),
  (TRUE, NULL),
  (FALSE, NULL),
  (NULL, TRUE),
  (NULL, FALSE);
--Testcase 400:
SELECT booland_statefunc(b1, b2) IS NULL as "t" FROM bool_test_tmp;
ROLLBACK;

-- and actual computations
BEGIN;
--Testcase 401:
INSERT INTO bool_test_tmp VALUES
  (TRUE, TRUE);
--Testcase 402:
SELECT booland_statefunc(b1, b2) as "t" FROM bool_test_tmp;
ROLLBACK;

BEGIN;
--Testcase 403:
INSERT INTO bool_test_tmp VALUES
  (TRUE, FALSE),
  (FALSE, TRUE),
  (FALSE, FALSE);
--Testcase 404:
SELECT NOT booland_statefunc(b1, b2) as "t" FROM bool_test_tmp;
ROLLBACK;

-- boolean or transitions
-- null because strict
BEGIN;
--Testcase 405:
INSERT INTO bool_test_tmp VALUES
  (NULL, NULL),
  (TRUE, NULL),
  (FALSE, NULL),
  (NULL, TRUE),
  (NULL, FALSE);
--Testcase 406:
SELECT boolor_statefunc(b1, b2) IS NULL as "t" FROM bool_test_tmp;
ROLLBACK;

-- actual computations
BEGIN;
--Testcase 407:
INSERT INTO bool_test_tmp VALUES
  (TRUE, TRUE),
  (TRUE, FALSE),
  (FALSE, TRUE);
--Testcase 408:
SELECT boolor_statefunc(b1, b2) as "t" FROM bool_test_tmp;
ROLLBACK;

BEGIN;
--Testcase 409:
INSERT INTO bool_test_tmp VALUES
  (FALSE, FALSE);
--Testcase 410:
SELECT NOT boolor_statefunc(b1, b2) as "t" FROM bool_test_tmp;
ROLLBACK;

--Testcase 411:
CREATE FOREIGN TABLE bool_test(
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL
) SERVER sqlite_svr;

-- empty case
--Testcase 46:
SELECT
  BOOL_AND(b1)   AS "n",
  BOOL_OR(b3)    AS "n"
FROM bool_test;

--Testcase 47:
INSERT INTO bool_test VALUES
  (TRUE, null, FALSE, null),
  (FALSE, TRUE, null, null),
  (null, TRUE, FALSE, null);

--Testcase 48:
SELECT
  BOOL_AND(b1)     AS "f",
  BOOL_AND(b2)     AS "t",
  BOOL_AND(b3)     AS "f",
  BOOL_AND(b4)     AS "n",
  BOOL_AND(NOT b2) AS "f",
  BOOL_AND(NOT b3) AS "t"
FROM bool_test;

--Testcase 49:
SELECT
  EVERY(b1)     AS "f",
  EVERY(b2)     AS "t",
  EVERY(b3)     AS "f",
  EVERY(b4)     AS "n",
  EVERY(NOT b2) AS "f",
  EVERY(NOT b3) AS "t"
FROM bool_test;

--Testcase 50:
SELECT
  BOOL_OR(b1)      AS "t",
  BOOL_OR(b2)      AS "t",
  BOOL_OR(b3)      AS "f",
  BOOL_OR(b4)      AS "n",
  BOOL_OR(NOT b2)  AS "f",
  BOOL_OR(NOT b3)  AS "t"
FROM bool_test;

--
-- Test cases that should be optimized into indexscans instead of
-- the generic aggregate implementation.
--

-- Basic cases
--Testcase 51:
explain (costs off)
  select min(unique1) from tenk1;
--Testcase 52:
select min(unique1) from tenk1;
--Testcase 53:
explain (costs off)
  select max(unique1) from tenk1;
--Testcase 54:
select max(unique1) from tenk1;
--Testcase 55:
explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;
--Testcase 56:
select max(unique1) from tenk1 where unique1 < 42;
--Testcase 57:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;
--Testcase 58:
select max(unique1) from tenk1 where unique1 > 42;

-- the planner may choose a generic aggregate here if parallel query is
-- enabled, since that plan will be parallel safe and the "optimized"
-- plan, which has almost identical cost, will not be.  we want to test
-- the optimized plan, so temporarily disable parallel query.
begin;
set local max_parallel_workers_per_gather = 0;
--Testcase 59:
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;
--Testcase 60:
select max(unique1) from tenk1 where unique1 > 42000;
rollback;

-- multi-column index (uses tenk1_thous_tenthous)
--Testcase 61:
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;
--Testcase 62:
select max(tenthous) from tenk1 where thousand = 33;
--Testcase 63:
explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;
--Testcase 64:
select min(tenthous) from tenk1 where thousand = 33;

-- check parameter propagation into an indexscan subquery
--Testcase 65:
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;
--Testcase 66:
select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
  from int4_tbl;

-- check some cases that were handled incorrectly in 8.3.0
--Testcase 67:
explain (costs off)
  select distinct max(unique2) from tenk1;
--Testcase 68:
select distinct max(unique2) from tenk1;
--Testcase 69:
explain (costs off)
  select max(unique2) from tenk1 order by 1;
--Testcase 70:
select max(unique2) from tenk1 order by 1;
--Testcase 71:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);
--Testcase 72:
select max(unique2) from tenk1 order by max(unique2);
--Testcase 73:
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;
--Testcase 74:
select max(unique2) from tenk1 order by max(unique2)+1;
--Testcase 75:
explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;
--Testcase 76:
select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

-- interesting corner case: constant gets optimized into a seqscan
--Testcase 77:
explain (costs off)
  select max(100) from tenk1;
--Testcase 78:
select max(100) from tenk1;

-- try it on an inheritance tree
--Testcase 412:
create foreign table minmaxtest(f1 int) server sqlite_svr;
--Testcase 413:
create table minmaxtest1() inherits (minmaxtest);
--Testcase 414:
create table minmaxtest2() inherits (minmaxtest);
--Testcase 415:
create table minmaxtest3() inherits (minmaxtest);
--Testcase 416:
create index minmaxtest1i on minmaxtest1(f1);
--Testcase 417:
create index minmaxtest2i on minmaxtest2(f1 desc);
--Testcase 418:
create index minmaxtest3i on minmaxtest3(f1) where f1 is not null;

--Testcase 79:
insert into minmaxtest values(11), (12);
--Testcase 80:
insert into minmaxtest1 values(13), (14);
--Testcase 81:
insert into minmaxtest2 values(15), (16);
--Testcase 82:
insert into minmaxtest3 values(17), (18);

--Testcase 83:
explain (costs off)
  select min(f1), max(f1) from minmaxtest;
--Testcase 84:
select min(f1), max(f1) from minmaxtest;

-- DISTINCT doesn't do anything useful here, but it shouldn't fail
--Testcase 85:
explain (costs off)
  select distinct min(f1), max(f1) from minmaxtest;
--Testcase 86:
select distinct min(f1), max(f1) from minmaxtest;

-- check for correct detection of nested-aggregate errors
--Testcase 87:
select max(min(unique1)) from tenk1;
--Testcase 88:
select (select max(min(unique1)) from int8_tbl) from tenk1;

--
-- Test removal of redundant GROUP BY columns
--

--Testcase 419:
create foreign table agg_t1 (a int OPTIONS (key 'true'), b int OPTIONS (key 'true'), c int, d int) server sqlite_svr;
--Testcase 420:
create foreign table agg_t2 (x int OPTIONS (key 'true'), y int OPTIONS (key 'true'), z int) server sqlite_svr;
--Testcase 421:
create foreign table agg_t9 (a int OPTIONS (key 'true'), b int OPTIONS (key 'true'), c int) server sqlite_svr;

-- Non-primary-key columns can be removed from GROUP BY
--Testcase 89:
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- No removal can happen if the complete PK is not present in GROUP BY
--Testcase 90:
explain (costs off) select a,c from agg_t1 group by a,c,d;

-- Test removal across multiple relations
--Testcase 91:
explain (costs off) select *
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.y,agg_t2.z;

-- Test case where agg_t1 can be optimized but not agg_t2
--Testcase 92:
explain (costs off) select agg_t1.*,agg_t2.x,agg_t2.z
from agg_t1 inner join agg_t2 on agg_t1.a = agg_t2.x and agg_t1.b = agg_t2.y
group by agg_t1.a,agg_t1.b,agg_t1.c,agg_t1.d,agg_t2.x,agg_t2.z;

-- Cannot optimize when PK is deferrable
--Testcase 422:
explain (costs off) select * from agg_t9 group by a,b,c;

--Testcase 423:
create temp table t1c () inherits (agg_t1);

-- Ensure we don't remove any columns when t1 has a child table
--Testcase 424:
explain (costs off) select * from agg_t1 group by a,b,c,d;

-- Okay to remove columns if we're only querying the parent.
--Testcase 425:
explain (costs off) select * from only agg_t1 group by a,b,c,d;

-- Skip this test, duckdb_fdw does not support partition table
--create foreign table p_t1 (
--  a int options (key 'true'),
--  b int options (key 'true'),
--  c int,
--  d int,
--) partition by list(a) server sqlite_svr;
--create temp table p_t1_1 partition of p_t1 for values in(1);
--create temp table p_t1_2 partition of p_t1 for values in(2);

-- Ensure we can remove non-PK columns for partitioned tables.
--explain (costs off) select * from p_t1 group by a,b,c,d;

--drop table t1 cascade;
--drop table t2;
--drop table t3;
--drop table p_t1;

--
-- Test GROUP BY matching of join columns that are type-coerced due to USING
--

--Testcase 426:
create foreign table t1(f1 int, f2 bigint) server sqlite_svr;
--Testcase 427:
create foreign table t2(f1 bigint, f22 bigint) server sqlite_svr;

--Testcase 428:
select f1 from t1 left join t2 using (f1) group by f1;
--Testcase 429:
select f1 from t1 left join t2 using (f1) group by t1.f1;
--Testcase 430:
select t1.f1 from t1 left join t2 using (f1) group by t1.f1;
-- only this one should fail:
--Testcase 431:
select t1.f1 from t1 left join t2 using (f1) group by f1;

--Testcase 432:
drop foreign table t1, t2;

--
-- Test combinations of DISTINCT and/or ORDER BY
--
begin;
--Testcase 93:
delete from INT8_TBL;
--Testcase 94:
insert into INT8_TBL values (1,4),(2,3),(3,1),(4,2);
--Testcase 95:
select array_agg(q1 order by q2)
  from INT8_TBL;
--Testcase 96:
select array_agg(q1 order by q1)
  from INT8_TBL;
--Testcase 97:
select array_agg(q1 order by q1 desc)
  from INT8_TBL;
--Testcase 98:
select array_agg(q2 order by q1 desc)
  from INT8_TBL;

--Testcase 99:
delete from INT4_TBL;
--Testcase 100:
insert into INT4_TBL values (1),(2),(1),(3),(null),(2);
--Testcase 101:
select array_agg(distinct f1)
  from INT4_TBL;
--Testcase 102:
select array_agg(distinct f1 order by f1)
  from INT4_TBL;
--Testcase 103:
select array_agg(distinct f1 order by f1 desc)
  from INT4_TBL;
--Testcase 104:
select array_agg(distinct f1 order by f1 desc nulls last)
  from INT4_TBL;
rollback;

-- multi-arg aggs, strict/nonstrict, distinct/order by
--Testcase 433:
create type aggtype as (a integer, b integer, c text);

--Testcase 434:
create function aggf_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql strict immutable;

--Testcase 435:
create function aggfns_trans(aggtype[],integer,integer,text) returns aggtype[]
as 'select array_append($1,ROW($2,$3,$4)::aggtype)'
language sql immutable;

--Testcase 436:
create aggregate aggfstr(integer,integer,text) (
   sfunc = aggf_trans, stype = aggtype[],
   initcond = '{}'
);

--Testcase 437:
create aggregate aggfns(integer,integer,text) (
   sfunc = aggfns_trans, stype = aggtype[], sspace = 10000,
   initcond = '{}'
);

begin;
--Testcase 105:
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');
--Testcase 106:
select aggfstr(a,b,c) from multi_arg_agg;
--Testcase 107:
select aggfns(a,b,c) from multi_arg_agg;

--Testcase 108:
select aggfstr(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;
--Testcase 109:
select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 110:
select aggfstr(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;
--Testcase 111:
select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

-- test specific code paths

--Testcase 112:
select aggfns(distinct a,a,c order by c using ~<~,a) from multi_arg_agg, generate_series(1,2) i;
--Testcase 113:
select aggfns(distinct a,a,c order by c using ~<~) from multi_arg_agg, generate_series(1,2) i;
--Testcase 114:
select aggfns(distinct a,a,c order by a) from multi_arg_agg, generate_series(1,2) i;
--Testcase 115:
select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

-- check node I/O via view creation and usage, also deparsing logic

--Testcase 438:
create view agg_view1 as
  select aggfns(a,b,c) from multi_arg_agg;

--Testcase 116:
select * from agg_view1;
--Testcase 117:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 439:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c) from multi_arg_agg, generate_series(1,3) i;

--Testcase 118:
select * from agg_view1;
--Testcase 119:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 440:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by b) from multi_arg_agg, generate_series(1,3) i;

--Testcase 120:
select * from agg_view1;
--Testcase 121:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 441:
create or replace view agg_view1 as
  select aggfns(a,b,c order by b+1) from multi_arg_agg;

--Testcase 122:
select * from agg_view1;
--Testcase 123:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 442:
create or replace view agg_view1 as
  select aggfns(a,a,c order by b) from multi_arg_agg;

--Testcase 124:
select * from agg_view1;
--Testcase 125:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 443:
create or replace view agg_view1 as
  select aggfns(a,b,c order by c using ~<~) from multi_arg_agg;

--Testcase 126:
select * from agg_view1;
--Testcase 127:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 444:
create or replace view agg_view1 as
  select aggfns(distinct a,b,c order by a,c using ~<~,b) from multi_arg_agg, generate_series(1,2) i;

--Testcase 128:
select * from agg_view1;
--Testcase 129:
select pg_get_viewdef('agg_view1'::regclass);

--Testcase 445:
drop view agg_view1;
rollback;

-- incorrect DISTINCT usage errors
--Testcase 130:
insert into multi_arg_agg values (1,1,'foo');
--Testcase 131:
select aggfns(distinct a,b,c order by i) from multi_arg_agg, generate_series(1,2) i;
--Testcase 132:
select aggfns(distinct a,b,c order by a,b+1) from multi_arg_agg, generate_series(1,2) i;
--Testcase 133:
select aggfns(distinct a,b,c order by a,b,i,c) from multi_arg_agg, generate_series(1,2) i;
--Testcase 134:
select aggfns(distinct a,a,c order by a,b) from multi_arg_agg, generate_series(1,2) i;

-- string_agg tests
begin;
--Testcase 135:
delete from varchar_tbl;
--Testcase 136:
insert into varchar_tbl values ('aaaa'),('bbbb'),('cccc');
--Testcase 137:
select string_agg(f1,',') from varchar_tbl;

--Testcase 138:
delete from varchar_tbl;
--Testcase 139:
insert into varchar_tbl values ('aaaa'),(null),('bbbb'),('cccc');
--Testcase 140:
select string_agg(f1,',') from varchar_tbl;

--Testcase 141:
delete from varchar_tbl;
--Testcase 142:
insert into varchar_tbl values (null),(null),('bbbb'),('cccc');
--Testcase 143:
select string_agg(f1,'AB') from varchar_tbl;

--Testcase 144:
delete from varchar_tbl;
--Testcase 145:
insert into varchar_tbl values (null),(null);
--Testcase 146:
select string_agg(f1,',') from varchar_tbl;
rollback;

-- check some implicit casting cases, as per bug #5564

--Testcase 147:
select string_agg(distinct f1, ',' order by f1) from varchar_tbl;  -- ok
--Testcase 148:
select string_agg(distinct f1::text, ',' order by f1) from varchar_tbl;  -- not ok
--Testcase 149:
select string_agg(distinct f1, ',' order by f1::text) from varchar_tbl;  -- not ok
--Testcase 150:
select string_agg(distinct f1::text, ',' order by f1::text) from varchar_tbl;  -- ok

-- string_agg bytea tests
--Testcase 446:
create foreign table bytea_test_table(v bytea) server sqlite_svr;

--Testcase 151:
select string_agg(v, '') from bytea_test_table;

--Testcase 152:
insert into bytea_test_table values(decode('ff','hex'));

--Testcase 153:
select string_agg(v, '') from bytea_test_table;

--Testcase 154:
insert into bytea_test_table values(decode('aa','hex'));

--Testcase 155:
select string_agg(v, '') from bytea_test_table;
--Testcase 156:
select string_agg(v, NULL) from bytea_test_table;
--Testcase 157:
select string_agg(v, decode('ee', 'hex')) from bytea_test_table;

--Testcase 447:
drop foreign table bytea_test_table;

-- FILTER tests

--Testcase 158:
select min(unique1) filter (where unique1 > 100) from tenk1;

--Testcase 159:
select sum(1/ten) filter (where ten > 0) from tenk1;

--Testcase 160:
select ten, sum(distinct four) filter (where four::text ~ '123') from onek a
group by ten;

--Testcase 161:
select ten, sum(distinct four) filter (where four > 10) from onek a
group by ten
having exists (select 1 from onek b where sum(distinct a.four) = b.four);

--Testcase 448:
create foreign table agg_t17(foo text, bar text) server sqlite_svr;
--Testcase 449:
insert into agg_t17 values ('a', 'b');

--Testcase 450:
select max(foo COLLATE "C") filter (where (bar collate "POSIX") > '0')
from agg_t17;

-- outer reference in FILTER (PostgreSQL extension)
--Testcase 451:
create foreign table agg_t18 (inner_c int) server sqlite_svr;
--Testcase 452:
create foreign table agg_t19 (outer_c int) server sqlite_svr;
--Testcase 453:
insert into agg_t18 values (1);
--Testcase 454:
insert into agg_t19 values (2), (3);

--Testcase 455:
select (select count(*)
        from agg_t18) from agg_t19; -- inner query is aggregation query
--Testcase 456:
select (select count(*) filter (where outer_c <> 0)
        from agg_t18) from agg_t19; -- outer query is aggregation query
--Testcase 457:
select (select count(inner_c) filter (where outer_c <> 0)
        from agg_t18) from agg_t19; -- inner query is aggregation query

--Testcase 162:
select
  (select max((select i.unique2 from tenk1 i where i.unique1 = o.unique1))
     filter (where o.unique1 < 10))
from tenk1 o;					-- outer query is aggregation query

-- subquery in FILTER clause (PostgreSQL extension)
--Testcase 163:
select sum(unique1) FILTER (WHERE
  unique1 IN (SELECT unique1 FROM onek where unique1 < 100)) FROM tenk1;

-- exercise lots of aggregate parts with FILTER
begin;
--Testcase 164:
delete from multi_arg_agg;
--Testcase 165:
insert into multi_arg_agg values (1,3,'foo'),(0,null,null),(2,2,'bar'),(3,1,'baz');
--Testcase 166:
select aggfns(distinct a,b,c order by a,c using ~<~,b) filter (where a > 1) from multi_arg_agg, generate_series(1,2) i;
rollback;

-- ordered-set aggregates

begin;
--Testcase 167:
delete from FLOAT8_TBL;
--Testcase 168:
insert into FLOAT8_TBL values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 169:
select f1, percentile_cont(f1) within group (order by x::float8)
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
--Testcase 170:
delete from FLOAT8_TBL;
--Testcase 171:
insert into FLOAT8_TBL values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 172:
select f1, percentile_cont(f1 order by f1) within group (order by x)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
--Testcase 173:
delete from FLOAT8_TBL;
--Testcase 174:
insert into FLOAT8_TBL values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 175:
select f1, sum() within group (order by x::float8)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

begin;
--Testcase 176:
delete from FLOAT8_TBL;
--Testcase 177:
insert into FLOAT8_TBL values (0::float8),(0.1),(0.25),(0.4),(0.5),(0.6),(0.75),(0.9),(1);
--Testcase 178:
select f1, percentile_cont(f1,f1)  -- error
from generate_series(1,5) x,
     FLOAT8_TBL
group by f1 order by f1;
rollback;

--Testcase 179:
-- Round the result to limited digits to avoid platform-specific results.
select (percentile_cont(0.5) within group (order by b))::numeric(20,10) from aggtest;
--Testcase 180:
-- Round the result to limited digits to avoid platform-specific results.
select (percentile_cont(0.5) within group (order by b))::numeric(20,10), sum(b)::numeric(10,3) from aggtest;
--Testcase 181:
-- Round the result to limited digits to avoid platform-specific results.
select percentile_cont(0.5) within group (order by thousand) from tenk1;
--Testcase 182:
select percentile_disc(0.5) within group (order by thousand) from tenk1;

begin;
--Testcase 183:
delete from INT4_TBL;
--Testcase 184:
insert into INT4_TBL values (1),(1),(2),(2),(3),(3),(4);
--Testcase 185:
select rank(3) within group (order by f1) from INT4_TBL;
--Testcase 186:
select cume_dist(3) within group (order by f1) from INT4_TBL;
--Testcase 187:
insert into INT4_TBL values (5);
--Testcase 458:
-- Round the result to limited digits to avoid platform-specific results.
select (percent_rank(3) within group (order by f1))::numeric(20,10) from INT4_TBL;
--Testcase 459:
delete from INT4_TBL where f1 = 5;
--Testcase 188:
select dense_rank(3) within group (order by f1) from INT4_TBL;
rollback;

--Testcase 189:
select percentile_disc(array[0,0.1,0.25,0.5,0.75,0.9,1]) within group (order by thousand)
from tenk1;
--Testcase 190:
select percentile_cont(array[0,0.25,0.5,0.75,1]) within group (order by thousand)
from tenk1;
--Testcase 191:
select percentile_disc(array[[null,1,0.5],[0.75,0.25,null]]) within group (order by thousand)
from tenk1;

--Testcase 460:
create foreign table agg_t21 (x int) server sqlite_svr;
begin;
--Testcase 248:
insert into agg_t21 select * from generate_series(1,6);
--Testcase 249:
select percentile_cont(array[0,1,0.25,0.75,0.5,1,0.3,0.32,0.35,0.38,0.4]) within group (order by x)
from agg_t21;
rollback;

--Testcase 192:
select ten, mode() within group (order by string4) from tenk1 group by ten;

--Testcase 461:
create foreign table agg_t20 (x text) server sqlite_svr;
begin;
--Testcase 462:
insert into agg_t20 values (unnest('{fred,jim,fred,jack,jill,fred,jill,jim,jim,sheila,jim,sheila}'::text[]));
--Testcase 463:
select percentile_disc(array[0.25,0.5,0.75]) within group (order by x) from agg_t20;
rollback;

-- check collation propagates up in suitable cases:
begin;
--Testcase 464:
insert into agg_t20 values ('fred'), ('jim');
--Testcase 465:
select pg_collation_for(percentile_disc(1) within group (order by x collate "POSIX")) from agg_t20;
rollback;

-- ordered-set aggs created with CREATE AGGREGATE
--Testcase 466:
create aggregate my_percentile_disc(float8 ORDER BY anyelement) (
  stype = internal,
  sfunc = ordered_set_transition,
  finalfunc = percentile_disc_final,
  finalfunc_extra = true,
  finalfunc_modify = read_write
);

--Testcase 467:
create aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any") (
  stype = internal,
  sfunc = ordered_set_transition_multi,
  finalfunc = rank_final,
  finalfunc_extra = true,
  hypothetical
);

alter aggregate my_percentile_disc(float8 ORDER BY anyelement)
  rename to test_percentile_disc;
  
alter aggregate my_rank(VARIADIC "any" ORDER BY VARIADIC "any")
  rename to test_rank;

begin;
--Testcase 468:
insert into agg_t21 values (1),(1),(2),(2),(3),(3),(4);
--Testcase 469:
select test_rank(3) within group (order by x) from agg_t21;
rollback;
 
--Testcase 193:
select test_percentile_disc(0.5) within group (order by thousand) from tenk1;

-- ordered-set aggs can't use ungrouped vars in direct args:
begin;
--Testcase 470:
insert into agg_t21 select * from generate_series(1,5);
--Testcase 471:
select rank(x) within group (order by x) from agg_t21;
rollback;

-- outer-level agg can't use a grouped arg of a lower level, either:
begin;
--Testcase 472:
insert into agg_t21 select * from generate_series(1,5);
--Testcase 473:
select array(select percentile_disc(a) within group (order by x)
               from (values (0.3),(0.7)) v(a) group by a)
  from agg_t21;
rollback;

-- agg in the direct args is a grouping violation, too:
begin;
--Testcase 474:
insert into agg_t21 select * from generate_series(1,5);
--Testcase 475:
select rank(sum(x)) within group (order by x) from agg_t21;
rollback;

-- hypothetical-set type unification and argument-count failures:
begin;
--Testcase 264:
insert into agg_t20 values ('fred'), ('jim');
--Testcase 265:
select rank(3) within group (order by x) from agg_t20;
rollback;

--Testcase 194:
select rank(3) within group (order by stringu1,stringu2) from tenk1;

begin;
--Testcase 476:
insert into agg_t21 select * from generate_series(1,5);
--Testcase 477:
select rank('fred') within group (order by x) from agg_t21;
rollback;

begin;
--Testcase 478:
insert into agg_t20 values ('fred'), ('jim');
--Testcase 479:
select rank('adam'::text collate "C") within group (order by x collate "POSIX")
  from agg_t20;
rollback;

-- hypothetical-set type unification successes:
begin;
--Testcase 480:
insert into agg_t20 values ('fred'), ('jim');
--Testcase 481:
select rank('adam'::varchar) within group (order by x) from agg_t20;
rollback;

begin;
--Testcase 482:
insert into agg_t21 select * from generate_series(1,5);
--Testcase 483:
select rank('3') within group (order by x) from agg_t21;
rollback;

-- divide by zero check
begin;
--Testcase 484:
insert into agg_t21 select * from generate_series(1,0);
--Testcase 485:
select percent_rank(0) within group (order by x) from agg_t21;
rollback;

-- deparse and multiple features:
--Testcase 486:
create view aggordview1 as
select ten,
       percentile_disc(0.5) within group (order by thousand) as p50,
       percentile_disc(0.5) within group (order by thousand) filter (where hundred=1) as px,
       rank(5,'AZZZZ',50) within group (order by hundred, string4 desc, hundred)
  from tenk1
 group by ten order by ten;

--Testcase 196:
select pg_get_viewdef('aggordview1');
--Testcase 197:
select * from aggordview1 order by ten;
--Testcase 487:
drop view aggordview1;

-- variadic aggregates
--Testcase 488:
create function least_accum(anyelement, variadic anyarray)
returns anyelement language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 489:
create aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);

--Testcase 490:
create function cleast_accum(anycompatible, variadic anycompatiblearray)
returns anycompatible language sql as
  'select least($1, min($2[i])) from generate_subscripts($2,1) g(i)';

--Testcase 491:
create aggregate cleast_agg(variadic items anycompatiblearray) (
  stype = anycompatible, sfunc = cleast_accum);

--Testcase 198:
select least_agg(q1,q2) from int8_tbl;
--Testcase 199:
select least_agg(variadic array[q1,q2]) from int8_tbl;

--Testcase 492:
select cleast_agg(q1,q2) from int8_tbl;
--Testcase 493:
select cleast_agg(4.5,f1) from int4_tbl;
--Testcase 494:
select cleast_agg(variadic array[4.5,f1]) from int4_tbl;
--Testcase 495:
select pg_typeof(cleast_agg(variadic array[4.5,f1])) from int4_tbl;

-- test aggregates with common transition functions share the same states
--Testcase 496:
create foreign table agg_t10(one int, id int options (key 'true')) server sqlite_svr;
--Testcase 497:
create foreign table agg_t11(one int, two int, id int options (key 'true')) server sqlite_svr;
--Testcase 498:
create foreign table agg_t12(a int, id int options (key 'true')) server sqlite_svr;
begin work;

--Testcase 499:
create type avg_state as (total bigint, count bigint);

--Testcase 500:
create or replace function avg_transfn(state avg_state, n int) returns avg_state as
$$
declare new_state avg_state;
begin
	raise notice 'avg_transfn called with %', n;
	if state is null then
		if n is not null then
			new_state.total := n;
			new_state.count := 1;
			return new_state;
		end if;
		return null;
	elsif n is not null then
		state.total := state.total + n;
		state.count := state.count + 1;
		return state;
	end if;

	return null;
end
$$ language plpgsql;

--Testcase 501:
create function avg_finalfn(state avg_state) returns int4 as
$$
begin
	if state is null then
		return NULL;
	else
		return state.total / state.count;
	end if;
end
$$ language plpgsql;

--Testcase 502:
create function sum_finalfn(state avg_state) returns int4 as
$$
begin
	if state is null then
		return NULL;
	else
		return state.total;
	end if;
end
$$ language plpgsql;

--Testcase 503:
create aggregate my_avg(int4)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn
);

--Testcase 504:
create aggregate my_sum(int4)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn
);

-- aggregate state should be shared as aggs are the same.
--Testcase 505:
delete from agg_t10;
--Testcase 506:
insert into agg_t10 values (1), (3);
--Testcase 507:
select my_avg(one),my_avg(one) from agg_t10;

-- aggregate state should be shared as transfn is the same for both aggs.
--Testcase 508:
select my_avg(one),my_sum(one) from agg_t10;

-- same as previous one, but with DISTINCT, which requires sorting the input.
--Testcase 509:
delete from agg_t10;
--Testcase 510:
insert into agg_t10 values (1), (3), (1);
--Testcase 511:
select my_avg(distinct one),my_sum(distinct one) from agg_t10;

-- shouldn't share states due to the distinctness not matching.
--Testcase 512:
delete from agg_t10;
--Testcase 513:
insert into agg_t10 values (1), (3);
--Testcase 514:
select my_avg(distinct one),my_sum(one) from agg_t10;

-- shouldn't share states due to the filter clause not matching.
--Testcase 515:
select my_avg(one) filter (where one > 1),my_sum(one) from agg_t10;

-- this should not share the state due to different input columns.
--Testcase 516:
delete from agg_t11;
--Testcase 517:
insert into agg_t11 values (1,2),(3,4);
--Testcase 518:
select my_avg(one),my_sum(two) from agg_t11;

-- exercise cases where OSAs share state
--Testcase 519:
delete from agg_t12;
--Testcase 520:
insert into agg_t12 values (1), (3), (5), (7);
--Testcase 521:
select
  percentile_cont(0.5) within group (order by a),
  percentile_disc(0.5) within group (order by a)
from agg_t12;

--Testcase 522:
select
  percentile_cont(0.25) within group (order by a),
  percentile_disc(0.5) within group (order by a)
from agg_t12;

-- these can't share state currently
--Testcase 523:
select
  rank(4) within group (order by a),
  dense_rank(4) within group (order by a)
from agg_t12;

-- test that aggs with the same sfunc and initcond share the same agg state
--Testcase 524:
create aggregate my_sum_init(int4)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = sum_finalfn,
   initcond = '(10,0)'
);

--Testcase 525:
create aggregate my_avg_init(int4)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(10,0)'
);

--Testcase 526:
create aggregate my_avg_init2(int4)
(
   stype = avg_state,
   sfunc = avg_transfn,
   finalfunc = avg_finalfn,
   initcond = '(4,0)'
);

-- state should be shared if INITCONDs are matching
--Testcase 527:
delete from agg_t10;
--Testcase 528:
insert into agg_t10 values (1), (3);
--Testcase 529:
select my_sum_init(one),my_avg_init(one) from agg_t10;


-- Varying INITCONDs should cause the states not to be shared.
--Testcase 530:
select my_sum_init(one),my_avg_init2(one) from agg_t10;

rollback;

-- test aggregate state sharing to ensure it works if one aggregate has a
-- finalfn and the other one has none.
begin work;

--Testcase 531:
create or replace function sum_transfn(state int4, n int4) returns int4 as
$$
declare new_state int4;
begin
	raise notice 'sum_transfn called with %', n;
	if state is null then
		if n is not null then
			new_state := n;
			return new_state;
		end if;
		return null;
	elsif n is not null then
		state := state + n;
		return state;
	end if;

	return null;
end
$$ language plpgsql;

--Testcase 532:
create function halfsum_finalfn(state int4) returns int4 as
$$
begin
	if state is null then
		return NULL;
	else
		return state / 2;
	end if;
end
$$ language plpgsql;

--Testcase 533:
create aggregate my_sum(int4)
(
   stype = int4,
   sfunc = sum_transfn
);

--Testcase 534:
create aggregate my_half_sum(int4)
(
   stype = int4,
   sfunc = sum_transfn,
   finalfunc = halfsum_finalfn
);

-- Agg state should be shared even though my_sum has no finalfn
--Testcase 535:
delete from agg_t10;
--Testcase 536:
insert into agg_t10 values (1), (2), (3), (4);
--Testcase 537:
select my_sum(one),my_half_sum(one) from agg_t10;

rollback;


-- test that the aggregate transition logic correctly handles
-- transition / combine functions returning NULL

-- First test the case of a normal transition function returning NULL
BEGIN;
--Testcase 538:
CREATE FUNCTION balkifnull(int8, int4)
RETURNS int8
STRICT
LANGUAGE plpgsql AS $$
BEGIN
    IF $1 IS NULL THEN
       RAISE 'erroneously called with NULL argument';
    END IF;
    RETURN NULL;
END$$;

--Testcase 539:
CREATE AGGREGATE balk(int4)
(
    SFUNC = balkifnull(int8, int4),
    STYPE = int8,
    PARALLEL = SAFE,
    INITCOND = '0'
);

--Testcase 200:
SELECT balk(hundred) FROM tenk1;

ROLLBACK;

-- Secondly test the case of a parallel aggregate combiner function
-- returning NULL. For that use normal transition function, but a
-- combiner function returning NULL.
BEGIN ISOLATION LEVEL REPEATABLE READ;
--Testcase 540:
CREATE FUNCTION balkifnull(int8, int8)
RETURNS int8
PARALLEL SAFE
STRICT
LANGUAGE plpgsql AS $$
BEGIN
    IF $1 IS NULL THEN
       RAISE 'erroneously called with NULL argument';
    END IF;
    RETURN NULL;
END$$;

--Testcase 541:
CREATE AGGREGATE balk(int4)
(
    SFUNC = int4_sum(int8, int4),
    STYPE = int8,
    COMBINEFUNC = balkifnull(int8, int8),
    PARALLEL = SAFE,
    INITCOND = '0'
);

-- force use of parallelism
-- Skip this test, cannot alter foreign table tenk1
-- ALTER FOREIGN TABLE tenk1 set (parallel_workers = 4);
-- SET LOCAL parallel_setup_cost=0;
-- SET LOCAL max_parallel_workers_per_gather=4;

-- EXPLAIN (COSTS OFF) SELECT balk(hundred) FROM tenk1;
-- SELECT balk(hundred) FROM tenk1;

ROLLBACK;

-- test coverage for aggregate combine/serial/deserial functions
BEGIN ISOLATION LEVEL REPEATABLE READ;

SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET max_parallel_workers_per_gather = 4;
SET parallel_leader_participation = off;
SET enable_indexonlyscan = off;

-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg
--Testcase 542:
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 543:
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

-- variance(int8) covers numeric_combine
-- avg(numeric) covers numeric_avg_combine
--Testcase 544:
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

--Testcase 545:
SELECT variance(unique1::int8), avg(unique1::numeric)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

ROLLBACK;

-- test coverage for dense_rank
--Testcase 546:
create foreign table agg_t13(x int, id int options (key 'true')) server sqlite_svr;
--Testcase 547:
insert into agg_t13 values (1),(1),(2),(2),(3),(3);
--Testcase 548:
SELECT dense_rank(x) WITHIN GROUP (ORDER BY x) FROM agg_t13 GROUP BY (x) ORDER BY 1;
--Testcase 549:
delete from agg_t13;


-- Ensure that the STRICT checks for aggregates does not take NULLness
-- of ORDER BY columns into account. See bug report around
-- 2a505161-2727-2473-7c46-591ed108ac52@email.cz
--Testcase 550:
create foreign table agg_t14(x int, y int, id int options (key 'true')) server sqlite_svr;
--Testcase 551:
insert into agg_t14 values (1, NULL), (1, 2);
--Testcase 552:
SELECT min(x ORDER BY y) FROM agg_t14;
--Testcase 553:
SELECT min(x ORDER BY y) FROM agg_t14;

-- check collation-sensitive matching between grouping expressions
begin;
--Testcase 554:
insert into agg_t20 values (unnest(array['a','b']));
--Testcase 555:
select x||'a', case x||'a' when 'aa' then 1 else 0 end, count(*)
  from agg_t20 group by x||'a' order by 1;
rollback;

begin;
--Testcase 556:
insert into agg_t20 values (unnest(array['a','b']));
--Testcase 557:
select x||'a', case when x||'a' = 'aa' then 1 else 0 end, count(*)
  from agg_t20 group by x||'a' order by 1;
rollback;

-- Make sure that generation of HashAggregate for uniqification purposes
-- does not lead to array overflow due to unexpected duplicate hash keys
-- see CAFeeJoKKu0u+A_A9R9316djW-YW3-+Gtgvy3ju655qRHR3jtdA@mail.gmail.com
--Testcase 558:
explain (costs off)
  select 1 from tenk1
   where (hundred, thousand) in (select twothousand, twothousand from onek);

--
-- Hash Aggregation Spill tests
--

set enable_sort=false;
set work_mem='64kB';

--Testcase 559:
select unique1, count(*), sum(twothousand) from tenk1
group by unique1
having sum(fivethous) > 4975
order by sum(twothousand);

set work_mem to default;
set enable_sort to default;

--
-- Compare results between plans using sorting and plans using hash
-- aggregation. Force spilling in both cases by setting work_mem low.
--

set work_mem='64kB';

--Testcase 560:
create foreign table agg_data_2k(g int, id int options (key 'true')) server sqlite_svr;
--Testcase 561:
create foreign table agg_data_20k(g int, id int options (key 'true')) server sqlite_svr;

--Testcase 562:
create foreign table agg_group_1(c1 int, c2 numeric, c3 int) server sqlite_svr;
--Testcase 563:
create foreign table agg_group_2(a int, c1 numeric, c2 text, c3 int) server sqlite_svr;
--Testcase 564:
create foreign table agg_group_3(c1 numeric, c2 int4, c3 int) server sqlite_svr;
--Testcase 565:
create foreign table agg_group_4(c1 numeric, c2 text, c3 int) server sqlite_svr;

--Testcase 566:
create foreign table agg_hash_1(c1 int, c2 numeric, c3 int) server sqlite_svr;
--Testcase 567:
create foreign table agg_hash_2(a int, c1 numeric, c2 text, c3 int) server sqlite_svr;
--Testcase 568:
create foreign table agg_hash_3(c1 numeric, c2 int4, c3 int) server sqlite_svr;
--Testcase 569:
create foreign table agg_hash_4(c1 numeric, c2 text, c3 int) server sqlite_svr;


--Testcase 570:
insert into agg_data_2k select g from generate_series(0, 1999) g;
--analyze agg_data_2k;

--Testcase 571:
insert into agg_data_20k select g from generate_series(0, 19999) g;
--analyze agg_data_20k;

-- Produce results with sorting.

set enable_hashagg = false;

set jit_above_cost = 0;

--Testcase 572:
explain (costs off)
select g%10000 as c1, sum(g::numeric) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 573:
insert into agg_group_1
select g%10000 as c1, sum(g::numeric) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 574:
insert into agg_group_2
select * from
  (values (100), (300), (500)) as r(a),
  lateral (
    select (g/2)::numeric as c1,
           array_agg(g::numeric) as c2,
	   count(*) as c3
    from agg_data_2k
    where g < r.a
    group by g/2) as s;

set jit_above_cost to default;

--Testcase 575:
insert into agg_group_3
select (g/2)::numeric as c1, sum(7::int4) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 576:
insert into agg_group_4
select (g/2)::numeric as c1, array_agg(g::numeric) as c2, count(*) as c3
  from agg_data_2k group by g/2;

-- Produce results with hash aggregation

set enable_hashagg = true;
set enable_sort = false;

set jit_above_cost = 0;

--Testcase 577:
explain (costs off)
select g%10000 as c1, sum(g::numeric) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 578:
insert into agg_hash_1
select g%10000 as c1, sum(g::numeric) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

--Testcase 579:
insert into agg_hash_2
select * from
  (values (100), (300), (500)) as r(a),
  lateral (
    select (g/2)::numeric as c1,
           array_agg(g::numeric) as c2,
	   count(*) as c3
    from agg_data_2k
    where g < r.a
    group by g/2) as s;

set jit_above_cost to default;

--Testcase 580:
insert into agg_hash_3
select (g/2)::numeric as c1, sum(7::int4) as c2, count(*) as c3
  from agg_data_2k group by g/2;

--Testcase 581:
insert into agg_hash_4
select (g/2)::numeric as c1, array_agg(g::numeric) as c2, count(*) as c3
  from agg_data_2k group by g/2;

set enable_sort = true;
set work_mem to default;

-- Compare group aggregation results to hash aggregation results

--Testcase 582:
(select * from agg_hash_1 except select * from agg_group_1)
  union all
(select * from agg_group_1 except select * from agg_hash_1);

--Testcase 583:
(select * from agg_hash_2 except select * from agg_group_2)
  union all
(select * from agg_group_2 except select * from agg_hash_2);

--Testcase 584:
(select * from agg_hash_3 except select * from agg_group_3)
  union all
(select * from agg_group_3 except select * from agg_hash_3);

--Testcase 585:
(select * from agg_hash_4 except select * from agg_group_4)
  union all
(select * from agg_group_4 except select * from agg_hash_4);

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
--Testcase 586:
DROP SERVER sqlite_svr CASCADE;
--Testcase 587:
DROP EXTENSION duckdb_fdw CASCADE;
