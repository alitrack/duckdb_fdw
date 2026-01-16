-- Clean environment
DROP EXTENSION IF EXISTS duckdb_fdw CASCADE;
CREATE EXTENSION duckdb_fdw;

CREATE SERVER duck_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

-- Setup Complex Data
SELECT duckdb_execute('duck_srv', 'CREATE TABLE complex_test (id INT, vec DOUBLE[], info JSON)');
SELECT duckdb_execute('duck_srv', 'INSERT INTO complex_test VALUES (1, [1.1, 2.2, 3.3], ''{"model": "deepseek"}'')');

-- Map to Postgres
CREATE FOREIGN TABLE complex_test (
    id INT,
    vec text[],
    info jsonb
) SERVER duck_srv OPTIONS (table 'complex_test');

SELECT * FROM complex_test;