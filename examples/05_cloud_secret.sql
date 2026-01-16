-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

-- ☁️ Cloud Secret Management
CREATE SERVER duck_s3 FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:',
    s3_region 'us-east-1',
    s3_access_key_id 'YOUR_ACCESS_KEY',
    s3_secret_access_key 'YOUR_SECRET_KEY'
);

-- Map a potential S3 table (even if it doesn't exist, we test the connection hook)
CREATE FOREIGN TABLE s3_test_table (
    id INT
) SERVER duck_s3 OPTIONS (table 'read_parquet(''s3://non-existent-bucket/data.parquet'')');

-- Expected to fail with IO Error or bucket error, NOT connection error.
DO $$
BEGIN
    PERFORM * FROM s3_test_table;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Caught expected error, connection hook passed.';
END $$;