\echo '=== FDW rows ==='
SELECT g, sum(v) FROM ft_grp GROUP BY g HAVING sum(v)>2800 ORDER BY g;
\echo '=== local rows ==='
CREATE TEMP TABLE tl2 AS SELECT * FROM ft_grp;
SELECT g, sum(v) FROM tl2 GROUP BY g HAVING sum(v)>2800 ORDER BY g;
\echo '=== remote SQL ==='
EXPLAIN (COSTS OFF) SELECT g, sum(v) FROM ft_grp GROUP BY g HAVING sum(v)>2800;
\echo '=== what is t_local from prev txn? re-create and compare raw sums per group ==='
SELECT g, sum(v), count(*) FROM ft_grp GROUP BY g ORDER BY g;
