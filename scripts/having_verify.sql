\echo '=== A. HAVING that must filter everything (expect 0 rows) ==='
SELECT count(*) AS rows_should_be_0
FROM ft_grp GROUP BY g HAVING count(*) < 1;

\echo '=== B. remote SQL now contains HAVING ==='
EXPLAIN (COSTS OFF) SELECT count(*) FROM ft_grp GROUP BY g HAVING count(*) < 5;

\echo '=== C. HAVING count(*)>28 (only g=0,1,2,3 qualify with 29) expect 4 ==='
SELECT count(*) FROM (SELECT g FROM ft_grp GROUP BY g HAVING count(*) > 28) t;

\echo '=== D. normal GROUP BY unaffected (expect 7) ==='
SELECT count(*) FROM (SELECT g FROM ft_grp GROUP BY g) t;

\echo '=== E. HAVING on sum (expect groups whose sum matches) ==='
SELECT g, sum(v) FROM ft_grp GROUP BY g HAVING sum(v) > 3000 ORDER BY g LIMIT 3;
