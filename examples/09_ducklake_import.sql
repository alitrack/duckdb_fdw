-- 09 DuckLake Import
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

CREATE SERVER ducklake_import_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:',
    attach_catalogs 'tpch=https://blobs.duckdb.org/datalake/tpch-sf3.ducklake;type ducklake'
);

DROP SCHEMA IF EXISTS remote_tpch CASCADE;
CREATE SCHEMA remote_tpch;

IMPORT FOREIGN SCHEMA "tpch" FROM SERVER ducklake_import_srv INTO remote_tpch;

SELECT table_name FROM information_schema.tables WHERE table_schema = 'remote_tpch';
SELECT count(*) FROM remote_tpch.lineitem LIMIT 1;
