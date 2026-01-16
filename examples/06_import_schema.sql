-- 06 Import Schema
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

DROP SCHEMA IF EXISTS remote_main CASCADE;
DROP SCHEMA IF EXISTS my_lake CASCADE;

CREATE SERVER duck_test FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

SELECT duckdb_execute('duck_test', 'CREATE TABLE local_data (id INT, price DOUBLE, tags TEXT[])');
SELECT duckdb_execute('duck_test', 'INSERT INTO local_data VALUES (1, 9.9, [''a'', ''b'']), (2, 19.9, [''c''])');
SELECT duckdb_execute('duck_test', 'COPY local_data TO ''/tmp/test_data.parquet'' (FORMAT PARQUET)');

CREATE SCHEMA remote_main;
IMPORT FOREIGN SCHEMA "main" FROM SERVER duck_test INTO remote_main;
SELECT * FROM remote_main.local_data;

CREATE SCHEMA my_lake;
IMPORT FOREIGN SCHEMA "/tmp/test_data.parquet" FROM SERVER duck_test INTO my_lake;
SELECT * FROM my_lake.test_data;
