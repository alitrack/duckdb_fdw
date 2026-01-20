-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

-- 🛰️ Stage 2 & 3 Integration Test: Iceberg + S3 Tables
CREATE SERVER iceberg_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:',
    s3_region 'us-east-1',
    s3_access_key_id 'YOUR_ACCESS_KEY',
    s3_secret_access_key 'YOUR_SECRET_KEY',
    attach_catalogs '__my_resource__=arn:aws:s3tables:us-east-1:259911478022:bucket/iceberg-on-the-browser;type iceberg;endpoint_type s3_tables'
);

-- 尝试导入特定的 tpch10 模式
IMPORT FOREIGN SCHEMA "tpch10" FROM SERVER iceberg_srv INTO public;

-- 验证查询
SELECT * FROM part LIMIT 10;