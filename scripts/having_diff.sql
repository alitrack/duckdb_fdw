\pset footer off
CREATE TEMP TABLE t_local AS SELECT * FROM ft_grp;
\echo '--- q1: HAVING count(*)<5 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,count(*) FROM ft_grp GROUP BY g HAVING count(*)<5)
  EXCEPT
  (SELECT g,count(*) FROM t_local GROUP BY g HAVING count(*)<5)) x) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,count(*) FROM t_local GROUP BY g HAVING count(*)<5)
  EXCEPT
  (SELECT g,count(*) FROM ft_grp GROUP BY g HAVING count(*)<5)) y) AS local_minus_fdw;
\echo '--- q2: HAVING sum(v)>2800 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,sum(v) FROM ft_grp GROUP BY g HAVING sum(v)>2800)
  EXCEPT
  (SELECT g,sum(v) FROM t_local GROUP BY g HAVING sum(v)>2800)) x) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,sum(v) FROM t_local GROUP BY g HAVING sum(v)>2800)
  EXCEPT
  (SELECT g,sum(v) FROM ft_grp GROUP BY g HAVING sum(v)>2800)) y) AS local_minus_fdw;
\echo '--- q3: HAVING g>=3 AND count(*)>28 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,count(*) FROM ft_grp GROUP BY g HAVING g>=3 AND count(*)>28)
  EXCEPT
  (SELECT g,count(*) FROM t_local GROUP BY g HAVING g>=3 AND count(*)>28)) x) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,count(*) FROM t_local GROUP BY g HAVING g>=3 AND count(*)>28)
  EXCEPT
  (SELECT g,count(*) FROM ft_grp GROUP BY g HAVING g>=3 AND count(*)>28)) y) AS local_minus_fdw;
\echo '--- q4: HAVING sum(v)-count(*)>2700 ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,count(*) FROM ft_grp GROUP BY g HAVING sum(v)-count(*)>2700)
  EXCEPT
  (SELECT g,count(*) FROM t_local GROUP BY g HAVING sum(v)-count(*)>2700)) x) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,count(*) FROM t_local GROUP BY g HAVING sum(v)-count(*)>2700)
  EXCEPT
  (SELECT g,count(*) FROM ft_grp GROUP BY g HAVING sum(v)-count(*)>2700)) y) AS local_minus_fdw;
\echo '--- q5: WHERE + GROUP BY + HAVING ---'
SELECT (SELECT count(*) FROM (
  (SELECT g,count(*) FROM ft_grp WHERE v>10 GROUP BY g HAVING count(*)>5)
  EXCEPT
  (SELECT g,count(*) FROM t_local WHERE v>10 GROUP BY g HAVING count(*)>5)) x) AS fdw_minus_local,
       (SELECT count(*) FROM (
  (SELECT g,count(*) FROM t_local WHERE v>10 GROUP BY g HAVING count(*)>5)
  EXCEPT
  (SELECT g,count(*) FROM ft_grp WHERE v>10 GROUP BY g HAVING count(*)>5)) y) AS local_minus_fdw;
\echo '--- q6: HAVING with ORDER BY + LIMIT ---'
SELECT (SELECT string_agg(g::text||':'||c::text, ',' ORDER BY g) FROM (SELECT g,count(*) c FROM ft_grp GROUP BY g HAVING count(*)>20 ORDER BY c DESC, g LIMIT 3) t) AS fdw,
       (SELECT string_agg(g::text||':'||c::text, ',' ORDER BY g) FROM (SELECT g,count(*) c FROM t_local GROUP BY g HAVING count(*)>20 ORDER BY c DESC, g LIMIT 3) t) AS local;
