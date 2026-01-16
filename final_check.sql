-- Final Stable Verification Script
CREATE OR REPLACE FUNCTION duckdb_fdw_handler_final() RETURNS fdw_handler AS '/home/coder/workspace/pg_duck/duckdb_fdw.so', 'duckdb_fdw_handler' LANGUAGE C STRICT;
CREATE OR REPLACE FUNCTION duckdb_execute_final(server name, query text) RETURNS void AS '/home/coder/workspace/pg_duck/duckdb_fdw.so', 'duckdb_execute' LANGUAGE C STRICT;

DROP FOREIGN DATA WRAPPER IF EXISTS duckdb_final CASCADE;
CREATE FOREIGN DATA WRAPPER duckdb_final HANDLER duckdb_fdw_handler_final;
CREATE SERVER duck_srv_final FOREIGN DATA WRAPPER duckdb_final OPTIONS (database ':memory:');

-- Test 1: Simple types
SELECT duckdb_execute_final('duck_srv_final', 'CREATE TABLE simple AS SELECT 100 as val, true as flag');
CREATE FOREIGN TABLE simple_test (val INT4, flag BOOL) SERVER duck_srv_final OPTIONS (table 'simple');
SELECT * FROM simple_test;

-- Test 2: Arrays (Map to TEXT for total stability)
SELECT duckdb_execute_final('duck_srv_final', 'CREATE TABLE arrays AS SELECT [10, 20, 30] as my_arr');
CREATE FOREIGN TABLE array_test (my_arr TEXT) SERVER duck_srv_final OPTIONS (table 'arrays');
SELECT * FROM array_test;