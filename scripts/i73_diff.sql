\set QUIET off
CREATE TEMP TABLE lt_grp AS SELECT * FROM ft_grp;

\echo '==================== #73 correctness: FDW vs local table ===================='

\echo '--- c1: ORDER BY v DESC LIMIT 5 (exact multiset) ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,v FROM ft_grp ORDER BY v DESC LIMIT 5)
  EXCEPT
  (SELECT g,v FROM lt_grp ORDER BY v DESC LIMIT 5)
) a) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,v FROM lt_grp ORDER BY v DESC LIMIT 5)
  EXCEPT
  (SELECT g,v FROM ft_grp ORDER BY v DESC LIMIT 5)
) b) AS local_minus_fdw;

\echo '--- c2: ORDER BY v LIMIT 3 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,v FROM ft_grp ORDER BY v LIMIT 3)
  EXCEPT
  (SELECT g,v FROM lt_grp ORDER BY v LIMIT 3)
) a) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,v FROM lt_grp ORDER BY v LIMIT 3)
  EXCEPT
  (SELECT g,v FROM ft_grp ORDER BY v LIMIT 3)
) b) AS local_minus_fdw;

\echo '--- c3: LIMIT 7 (no order; rowset unspecified -> compare count+range only) ---'
SELECT (SELECT count(*) FROM ft_grp LIMIT 7) AS fdw_cnt,
       (SELECT count(*) FROM lt_grp LIMIT 7) AS local_cnt,
       (SELECT min(g) FROM ft_grp LIMIT 7) AS fdw_ming,
       (SELECT min(g) FROM lt_grp LIMIT 7) AS local_ming;

\echo '--- c4: ORDER BY v DESC NULLS LAST LIMIT 4 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,v FROM ft_grp ORDER BY v DESC NULLS LAST LIMIT 4)
  EXCEPT
  (SELECT g,v FROM lt_grp ORDER BY v DESC NULLS LAST LIMIT 4)
) a) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,v FROM lt_grp ORDER BY v DESC NULLS LAST LIMIT 4)
  EXCEPT
  (SELECT g,v FROM ft_grp ORDER BY v DESC NULLS LAST LIMIT 4)
) b) AS local_minus_fdw;

\echo '--- c5: OFFSET 3 LIMIT 5 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,v FROM ft_grp ORDER BY v DESC OFFSET 3 LIMIT 5)
  EXCEPT
  (SELECT g,v FROM lt_grp ORDER BY v DESC OFFSET 3 LIMIT 5)
) a) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,v FROM lt_grp ORDER BY v DESC OFFSET 3 LIMIT 5)
  EXCEPT
  (SELECT g,v FROM ft_grp ORDER BY v DESC OFFSET 3 LIMIT 5)
) b) AS local_minus_fdw;

\echo '--- c6: regression, plain full scan (must still match) ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,v FROM ft_grp) EXCEPT (SELECT g,v FROM lt_grp)
) a) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,v FROM lt_grp) EXCEPT (SELECT g,v FROM ft_grp)
) b) AS local_minus_fdw;
