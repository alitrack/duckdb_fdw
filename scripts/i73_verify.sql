\set QUIET on
\pset footer off
\echo '================== #73 LIMIT/SORT pushdown verification =================='
\echo
\echo '--- [1] ORDER BY only: expect Remote SQL with ORDER BY, no local Sort ---'
\set QUIET off
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_grp ORDER BY v DESC;
\set QUIET on
\echo
\echo '--- [2] LIMIT only: expect Remote SQL with LIMIT ---'
\set QUIET off
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_grp LIMIT 5;
\set QUIET on
\echo
\echo '--- [3] ORDER BY + LIMIT: expect Remote SQL with ORDER BY ... LIMIT ---'
\set QUIET off
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM ft_grp ORDER BY v DESC LIMIT 5;
\set QUIET on
\echo
\echo '--- [4] projection + ORDER BY + LIMIT (sort col not in projection) ---'
\set QUIET off
EXPLAIN (VERBOSE, COSTS OFF) SELECT g FROM ft_grp ORDER BY v DESC LIMIT 5;
\set QUIET on
\echo
\echo '================== correctness vs local ground truth =================='
\echo
CREATE TEMP TABLE t AS SELECT * FROM ft_grp;
\echo '--- C1: top-5 by v desc (FDW must equal local) ---'
EXCEPT_SYMMETRIC: SELECT g,v FROM ft_grp ORDER BY v DESC LIMIT 5 EXCEPT SELECT g,v FROM t ORDER BY v DESC LIMIT 5;
SELECT g,v FROM t ORDER BY v DESC LIMIT 5 EXCEPT SELECT g,v FROM ft_grp ORDER BY v DESC LIMIT 5;
\echo '--- C2: ORDER BY g asc, offset+limit ---'
SELECT g,v FROM ft_grp ORDER BY g ASC LIMIT 5 OFFSET 3 EXCEPT SELECT g,v FROM t ORDER BY g ASC LIMIT 5 OFFSET 3;
SELECT g,v FROM t ORDER BY g ASC LIMIT 5 OFFSET 3 EXCEPT SELECT g,v FROM ft_grp ORDER BY g ASC LIMIT 5 OFFSET 3;
\echo '--- C3: multi-col ORDER BY ---'
SELECT g,v FROM ft_grp ORDER BY g DESC, v ASC LIMIT 8 EXCEPT SELECT g,v FROM t ORDER BY g DESC, v ASC LIMIT 8;
SELECT g,v FROM t ORDER BY g DESC, v ASC LIMIT 8 EXCEPT SELECT g,v FROM ft_grp ORDER BY g DESC, v ASC LIMIT 8;
\echo '--- C4: expression sort key ---'
SELECT v FROM ft_grp ORDER BY g+1 LIMIT 4 EXCEPT SELECT v FROM t ORDER BY g+1 LIMIT 4;
SELECT v FROM t ORDER BY g+1 LIMIT 4 EXCEPT SELECT v FROM ft_grp ORDER BY g+1 LIMIT 4;
\echo '--- C5: WHERE + ORDER BY + LIMIT (filter pushdown) ---'
SELECT g,v FROM ft_grp WHERE v>100 ORDER BY v DESC LIMIT 6 EXCEPT SELECT g,v FROM t WHERE v>100 ORDER BY v DESC LIMIT 6;
SELECT g,v FROM t WHERE v>100 ORDER BY v DESC LIMIT 6 EXCEPT SELECT g,v FROM ft_grp WHERE v>100 ORDER BY v DESC LIMIT 6;
\echo
\echo '================== regression: plain scan & GROUP BY =================='
\echo '--- R1: plain scan rowcount (expect 200) ---'
SELECT count(*) FROM ft_grp;
\echo '--- R2: GROUP BY still works ---'
SELECT g, count(*) FROM ft_grp GROUP BY g ORDER BY g LIMIT 3;
\echo '--- R3: GROUP BY + HAVING (HAVING fix regression) ---'
SELECT count(*) FROM (SELECT g FROM ft_grp GROUP BY g HAVING count(*)<1) x;
\echo
\echo '================== done =================='
