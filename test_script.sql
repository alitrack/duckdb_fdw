-- test_script.sql
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;
CREATE SERVER DuckDB_server FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');  
SELECT duckdb_execute('duckdb_server','CREATE VIEW tables_duckdb AS SELECT *  FROM information_schema.tables');
IMPORT FOREIGN SCHEMA public  FROM SERVER DuckDB_server INTO public; 
SELECT * FROM tables_duckdb;