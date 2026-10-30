CREATE EXTENSION duckdb_fdw;
SET datestyle = 'ISO, YMD';
CREATE SERVER duckdb_transaction FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdb_fdw_regress_transaction.db');

SELECT duckdb_execute('duckdb_transaction',
    'CREATE OR REPLACE TABLE transaction_rows(i INTEGER CHECK (i > 0))');
CREATE FOREIGN TABLE transaction_rows(i integer) SERVER duckdb_transaction;

-- Commit and roll back inserts in separate transactions.
BEGIN;
INSERT INTO transaction_rows VALUES (1);
COMMIT;
BEGIN;
INSERT INTO transaction_rows VALUES (2);
ROLLBACK;
SELECT * FROM transaction_rows;

-- Roll back an insert to a savepoint, then insert and commit another row.
BEGIN;
SAVEPOINT s;
INSERT INTO transaction_rows VALUES (3);
SELECT * FROM transaction_rows ORDER BY i;
ROLLBACK TO s;
INSERT INTO transaction_rows VALUES (4);
RELEASE s;
COMMIT;
SELECT * FROM transaction_rows ORDER BY i;

-- Roll back the outer transaction after releasing nested savepoints.
BEGIN;
SAVEPOINT s;
SAVEPOINT inner_s;
INSERT INTO transaction_rows VALUES (5);
RELEASE inner_s;
INSERT INTO transaction_rows VALUES (6);
RELEASE s;
ROLLBACK;
SELECT * FROM transaction_rows ORDER BY i;

-- Commit an earlier insert after recovering from an error at a savepoint.
BEGIN;
INSERT INTO transaction_rows VALUES (7);
SAVEPOINT s;
SELECT 1 / 0;
ROLLBACK TO s;
COMMIT;

-- Reject foreign table access at a savepoint after an earlier insert.
-- Recover after each error and commit the earlier insert.
BEGIN;
INSERT INTO transaction_rows VALUES (8);
SAVEPOINT s;
SELECT * FROM transaction_rows;
ROLLBACK TO s;
INSERT INTO transaction_rows VALUES (9);
ROLLBACK TO s;
IMPORT FOREIGN SCHEMA main LIMIT TO (transaction_rows)
FROM SERVER duckdb_transaction INTO public;
ROLLBACK TO s;
SELECT duckdb_execute('duckdb_transaction', 'COMMIT');
ROLLBACK TO s;
COMMIT;
SELECT * FROM transaction_rows ORDER BY i;

-- Reject a read at a second savepoint after releasing an insert savepoint.
BEGIN;
SAVEPOINT first_child;
INSERT INTO transaction_rows VALUES (11);
RELEASE first_child;
SAVEPOINT second_child;
SELECT * FROM transaction_rows;
ROLLBACK TO second_child;
COMMIT;

-- Recover from a constraint violation at a savepoint and retry the insert.
BEGIN;
SAVEPOINT s;
INSERT INTO transaction_rows VALUES (12), (-1);
ROLLBACK TO s;
INSERT INTO transaction_rows VALUES (13);
RELEASE s;
COMMIT;

-- Roll back COPY at a savepoint, then retry and commit.
BEGIN;
SAVEPOINT s;
COPY transaction_rows FROM STDIN;
15
16
\.
ROLLBACK TO s;
COPY transaction_rows FROM STDIN;
17
\.
RELEASE s;
COMMIT;

-- Reject a cursor fetch at a savepoint after an earlier insert.
BEGIN;
INSERT INTO transaction_rows VALUES (19);
DECLARE owned_cursor CURSOR FOR SELECT * FROM transaction_rows ORDER BY i;
FETCH 1 FROM owned_cursor;
SAVEPOINT s;
FETCH 1 FROM owned_cursor;
ROLLBACK TO s;
CLOSE owned_cursor;
COMMIT;

-- Reject a DuckDB COMMIT while a foreign table transaction is active.
BEGIN;
SAVEPOINT s;
INSERT INTO transaction_rows VALUES (20);
SELECT duckdb_execute('duckdb_transaction', 'COMMIT');
ROLLBACK TO s;
COMMIT;

-- Roll back a savepoint on one server while retaining writes on another.
CREATE SERVER duckdb_transaction_other FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/duckdb_fdw_regress_transaction_other.db');
SELECT duckdb_execute('duckdb_transaction_other',
    'CREATE OR REPLACE TABLE transaction_other(i INTEGER)');
CREATE FOREIGN TABLE transaction_other(i integer) SERVER duckdb_transaction_other;
BEGIN;
INSERT INTO transaction_rows VALUES (21);
SAVEPOINT s;
INSERT INTO transaction_other VALUES (22);
ROLLBACK TO s;
INSERT INTO transaction_other VALUES (23);
RELEASE s;
COMMIT;
SELECT * FROM transaction_other;
DROP FOREIGN TABLE transaction_other;
DROP SERVER duckdb_transaction_other;

-- Reject PREPARE TRANSACTION after an insert into a foreign table.
BEGIN;
SAVEPOINT s;
INSERT INTO transaction_rows VALUES (24);
RELEASE s;
PREPARE TRANSACTION 'duckdb_transaction_prepared';

-- Reuse a retained connection after a savepoint rollback.
SET duckdb_fdw.keep_connection = on;
BEGIN;
SAVEPOINT s;
INSERT INTO transaction_rows VALUES (-1);
ROLLBACK TO s;
INSERT INTO transaction_rows VALUES (25);
RELEASE s;
COMMIT;
INSERT INTO transaction_rows VALUES (26);
SELECT * FROM transaction_rows ORDER BY i;
RESET duckdb_fdw.keep_connection;
DROP FOREIGN TABLE transaction_rows;

DROP SERVER duckdb_transaction;
DROP EXTENSION duckdb_fdw;
