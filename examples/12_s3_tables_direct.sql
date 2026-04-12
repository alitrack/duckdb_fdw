-- 12 S3 Tables Direct Test
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

DROP SERVER IF EXISTS s3_tables_srv CASCADE;
CREATE SERVER s3_tables_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database '/tmp/duckdb_fdw_s3_tables.db',
    s3_region 'us-east-1',
    s3_access_key_id 'YOUR_ACCESS_KEY',
    s3_secret_access_key 'YOUR_SECRET_KEY',
    attach_catalogs '__my_resource__=arn:aws:s3tables:us-east-1:259911478022:bucket/iceberg-on-the-browser;type iceberg'
);

-- 1. Map and Query
CREATE FOREIGN TABLE my_view_fdw (
    p_partkey BIGINT,
    p_name TEXT,
    p_mfgr TEXT,
    p_brand TEXT,
    p_type TEXT,
    p_size INTEGER,
    p_container TEXT,
    p_retailprice DECIMAL,
    p_comment TEXT
) SERVER s3_tables_srv OPTIONS (table '__my_resource__.tpch10.part');

SELECT * FROM my_view_fdw OFFSET 0 LIMIT 22;
