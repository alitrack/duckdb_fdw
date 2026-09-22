\echo '=== BUG B: hugeint col via FDW, first 12 rows, expect 1..12 with no zeros ==='
SELECT x, hi FROM f_hi ORDER BY x LIMIT 12;
\echo '=== BUG B: full 2000-row self-consistency, expect bad_rows = 0 ==='
SELECT count(*) AS bad_rows FROM f_hi WHERE hi <> x;
\echo '=== original repro: sum(int) grouped ==='
SELECT g, sum(v), count(*) FROM ft_grp GROUP BY g ORDER BY g;
\echo '=== regression: plain scan of bigint cols, expect cnt=2000 mn=1 mx=2000 ==='
SELECT count(*) AS cnt, min(x) AS mn, max(x) AS mx FROM f_hi;
