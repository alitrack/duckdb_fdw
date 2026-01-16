-- 11 SF3 Analytics Test
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

DROP SERVER IF EXISTS real_data_srv CASCADE;
CREATE SERVER real_data_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:',
    attach_catalogs 'tpch=https://blobs.duckdb.org/datalake/tpch-sf3.ducklake;type ducklake'
);

CREATE SCHEMA IF NOT EXISTS tpch_sf3;
IMPORT FOREIGN SCHEMA "tpch" FROM SERVER real_data_srv INTO tpch_sf3;

-- Test 1: Count lineitems
SELECT count(*) as total_lineitems FROM tpch_sf3.lineitem;

-- Test 2: Analytical query
SELECT 
    l_returnflag, 
    sum(l_quantity) as sum_qty, 
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM tpch_sf3.lineitem
WHERE l_shipdate <= '1998-09-01'
GROUP BY l_returnflag
ORDER BY l_returnflag;
