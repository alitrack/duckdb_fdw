-- 13 Comprehensive Example Datasets Test (Final Validation)
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

-- Scenario 1: Parquet, train_services
DROP SERVER IF EXISTS srv_parquet_train CASCADE;
CREATE SERVER srv_parquet_train FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:', extensions 'httpfs,parquet');
SELECT duckdb_execute('srv_parquet_train', 'CREATE OR REPLACE VIEW __my_view__ AS SELECT * FROM read_parquet(''https://blobs.duckdb.org/train_services.parquet'')');
CREATE FOREIGN TABLE view_parquet_train (
    station_name TEXT,
    train_number TEXT,
    arrival_time TEXT
) SERVER srv_parquet_train OPTIONS (table '__my_view__');
SELECT 'Scenario 1' as test, station_name, train_number FROM view_parquet_train LIMIT 1;

-- Scenario 2: DuckLake tpch-sf3
DROP SERVER IF EXISTS srv_tpch_ducklake CASCADE;
CREATE SERVER srv_tpch_ducklake FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:', 
    attach_catalogs 'tpch=https://blobs.duckdb.org/datalake/tpch-sf3.ducklake;type ducklake'
);
CREATE FOREIGN TABLE view_tpch_lineitem (
    l_orderkey BIGINT,
    l_shipdate DATE
) SERVER srv_tpch_ducklake OPTIONS (table 'tpch.main.lineitem');
SELECT 'Scenario 2' as test, count(*) FROM view_tpch_lineitem;

-- Scenario 7: Iceberg (s3_tables)
DROP SERVER IF EXISTS srv_iceberg CASCADE;
CREATE SERVER srv_iceberg FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:', extensions 'httpfs,iceberg');
SELECT duckdb_create_s3_secret('srv_iceberg', '__my_secret__', 'YOUR_ACCESS_KEY', 'YOUR_SECRET_KEY');
SELECT duckdb_execute('srv_iceberg', 'ATTACH IF NOT EXISTS ''arn:aws:s3tables:us-east-1:259911478022:bucket/iceberg-on-the-browser'' AS __my_resource__ (TYPE iceberg, ENDPOINT_TYPE ''s3_tables'')');
SELECT duckdb_execute('srv_iceberg', 'CREATE OR REPLACE VIEW __my_view__ AS FROM __my_resource__.tpch10.part');
CREATE FOREIGN TABLE view_iceberg (
    p_partkey BIGINT,
    p_name TEXT
) SERVER srv_iceberg OPTIONS (table '__my_view__');
SELECT 'Scenario 7' as test, p_name FROM view_iceberg LIMIT 1;
