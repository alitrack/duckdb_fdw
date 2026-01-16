-- 03 Vector Support
CREATE EXTENSION IF NOT EXISTS duckdb_fdw;

CREATE SERVER vec_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database ':memory:');

SELECT duckdb_execute('vec_srv', 'CREATE TABLE v_demo (id INT, vec FLOAT[3], label TEXT)');
SELECT duckdb_execute('vec_srv', 'INSERT INTO v_demo VALUES (1, [0.1, 0.5, 0.9], ''Vector Test'')');

CREATE FOREIGN TABLE vec_demo (
    id INT,
    vec float4[],
    label TEXT
) SERVER vec_srv OPTIONS (table 'v_demo');

SELECT id, vec, label FROM vec_demo;
SELECT id, (vec::vector <-> '[0.1, 0.5, 0.9]'::vector) as distance FROM vec_demo;