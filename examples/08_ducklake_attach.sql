-- 08 DuckLake Attach
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

CREATE SERVER ducklake_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:',
    attach_catalogs 'tpch=https://blobs.duckdb.org/datalake/tpch-sf3.ducklake;type ducklake'
);

CREATE FOREIGN TABLE remote_lineitem (
    l_orderkey BIGINT,
    l_quantity float8,
    l_shipdate DATE
) SERVER ducklake_srv OPTIONS (table 'tpch.main.lineitem');

SELECT l_orderkey, l_quantity, l_shipdate FROM remote_lineitem LIMIT 5;
