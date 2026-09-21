\echo '=== 1. aggregate WITHOUT having (is it pre-existing?) ==='
SELECT g, sum(v), count(*) FROM ft_grp GROUP BY g ORDER BY g;
\echo '=== 2. raw scan sample ==='
SELECT * FROM ft_grp ORDER BY g, v LIMIT 8;
\echo '=== 3. raw scan: any v=0 rows? ==='
SELECT count(*) AS zeros, count(*) AS total FROM ft_grp WHERE v = 0;
\echo '=== 4. raw scan per-group sum computed locally from raw rows ==='
SELECT g, sum(v) FROM ft_grp GROUP BY g ORDER BY g;
