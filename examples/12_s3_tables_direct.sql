-- 12 S3 Tables Direct Test
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

DROP SERVER IF EXISTS s3_tables_srv CASCADE;
CREATE SERVER s3_tables_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- 1. Create Secret via wrapped function
SELECT duckdb_create_s3_secret('s3_tables_srv', '__my_secret__', 'YOUR_ACCESS_KEY', 'YOUR_SECRET_KEY');

-- 2. Attach Resource
SELECT duckdb_execute('s3_tables_srv', 'ATTACH IF NOT EXISTS ''arn:aws:s3tables:us-east-1:259911478022:bucket/iceberg-on-the-browser'' AS __my_resource__ (TYPE iceberg, ENDPOINT_TYPE ''s3_tables'')');

-- 3. Create View
SELECT duckdb_execute('s3_tables_srv', 'CREATE OR REPLACE VIEW __my_view__ AS FROM __my_resource__.tpch10.part');

-- 4. Map and Query
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
) SERVER s3_tables_srv OPTIONS (table '__my_view__');

SELECT * FROM my_view_fdw OFFSET 0 LIMIT 22;
