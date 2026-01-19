-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

-- 🛰️ Stage 2 & 3 Integration Test: Iceberg + S3 Tables + IMPORT
CREATE SERVER iceberg_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (
    database ':memory:',
    s3_region 'us-east-1',
    s3_access_key_id 'YOUR_ACCESS_KEY',
    s3_secret_access_key 'YOUR_SECRET_KEY',
    s3_endpoint_type 's3_tables',
    attach_catalogs '__my_resource__=arn:aws:s3tables:us-east-1:259911478022:bucket/iceberg-on-the-browser;type=iceberg'
);

-- Note: This is expected to fail in test environment due to fake credentials, 
-- but we verify that FDW doesn't crash and executes ATTACH.
DO $$
BEGIN
    IMPORT FOREIGN SCHEMA "__my_resource__" FROM SERVER iceberg_srv INTO public;
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Caught expected error from S3 Tables: %', SQLERRM;
END $$;