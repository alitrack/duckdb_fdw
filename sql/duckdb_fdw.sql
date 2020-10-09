--SET log_min_messages  TO DEBUG1;
--SET client_min_messages  TO DEBUG1;
--Testcase 129:
CREATE EXTENSION duckdb_fdw;
--Testcase 130:
CREATE SERVER sqlite_svr FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (database '/tmp/sqlitefdw_test.db');
--Testcase 131:
CREATE FOREIGN TABLE department(department_id int OPTIONS (key 'true'), department_name text) SERVER sqlite_svr; 
--Testcase 132:
CREATE FOREIGN TABLE employee(emp_id int OPTIONS (key 'true'), emp_name text, emp_dept_id int) SERVER sqlite_svr;
--Testcase 133:
CREATE FOREIGN TABLE empdata(emp_id int OPTIONS (key 'true'), emp_dat bytea) SERVER sqlite_svr;
--Testcase 134:
CREATE FOREIGN TABLE numbers(a int OPTIONS (key 'true'), b varchar(255)) SERVER sqlite_svr;
--Testcase 135:
CREATE FOREIGN TABLE multiprimary(a int, b int OPTIONS (key 'true'), c int OPTIONS(key 'true')) SERVER sqlite_svr;
--Testcase 136:
CREATE FOREIGN TABLE noprimary(a int, b text) SERVER sqlite_svr;

--Testcase 1:
SELECT * FROM department LIMIT 10;
--Testcase 2:
SELECT * FROM employee LIMIT 10;
--Testcase 3:
SELECT * FROM empdata LIMIT 10;

--Testcase 4:
INSERT INTO department VALUES(generate_series(1,100), 'dept - ' || generate_series(1,100));
--Testcase 5:
INSERT INTO employee VALUES(generate_series(1,100), 'emp - ' || generate_series(1,100), generate_series(1,100));
--Testcase 6:
INSERT INTO empdata  VALUES(1, decode ('01234567', 'hex'));

--Testcase 7:
INSERT INTO numbers VALUES(1, 'One');
--Testcase 8:
INSERT INTO numbers VALUES(2, 'Two');
--Testcase 9:
INSERT INTO numbers VALUES(3, 'Three');
--Testcase 10:
INSERT INTO numbers VALUES(4, 'Four');
--Testcase 11:
INSERT INTO numbers VALUES(5, 'Five');
--Testcase 12:
INSERT INTO numbers VALUES(6, 'Six');
--Testcase 13:
INSERT INTO numbers VALUES(7, 'Seven');
--Testcase 14:
INSERT INTO numbers VALUES(8, 'Eight');
--Testcase 15:
INSERT INTO numbers VALUES(9, 'Nine');

--Testcase 16:
SELECT count(*) FROM department;
--Testcase 17:
SELECT count(*) FROM employee;
--Testcase 18:
SELECT count(*) FROM empdata;

--Testcase 19:
EXPLAIN (COSTS FALSE) SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;

--Testcase 20:
EXPLAIN (COSTS FALSE) SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;

--Testcase 21:
SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;
--Testcase 22:
SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) ORDER BY d.department_id LIMIT 10;
--Testcase 23:
SELECT * FROM empdata;

--Testcase 24:
DELETE FROM employee WHERE emp_id = 10;

--Testcase 25:
SELECT COUNT(*) FROM department LIMIT 10;
--Testcase 26:
SELECT COUNT(*) FROM employee WHERE emp_id = 10;

--Testcase 27:
UPDATE employee SET emp_name = 'UPDATEd emp' WHERE emp_id = 20;
--Testcase 28:
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'UPDATEd emp';

--Testcase 29:
UPDATE empdata SET emp_dat = decode ('0123', 'hex');
--Testcase 30:
SELECT * FROM empdata;

--Testcase 31:
SELECT * FROM employee LIMIT 10;
--Testcase 32:
SELECT * FROM employee WHERE emp_id IN (1);
--Testcase 33:
SELECT * FROM employee WHERE emp_id IN (1,3,4,5);
--Testcase 34:
SELECT * FROM employee WHERE emp_id IN (10000,1000);

--Testcase 35:
SELECT * FROM employee WHERE emp_id NOT IN (1) LIMIT 5;
--Testcase 36:
SELECT * FROM employee WHERE emp_id NOT IN (1,3,4,5) LIMIT 5;
--Testcase 37:
SELECT * FROM employee WHERE emp_id NOT IN (10000,1000) LIMIT 5;

--Testcase 38:
SELECT * FROM employee WHERE emp_id NOT IN (SELECT emp_id FROM employee WHERE emp_id IN (1,10));
--Testcase 39:
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 1', 'emp - 2') LIMIT 5;
--Testcase 40:
SELECT * FROM employee WHERE emp_name NOT IN ('emp - 10') LIMIT 5;

--Testcase 41:
SELECT * FROM numbers WHERE (CASE WHEN a % 2 = 0 THEN 1 WHEN a % 5 = 0 THEN 1 ELSE 0 END) = 1;
--Testcase 42:
SELECT * FROM numbers WHERE (CASE b WHEN 'Two' THEN 1 WHEN 'Six' THEN 1 ELSE 0 END) = 1;

--Testcase 137:
create or replace function test_param_WHERE() returns void as $$
DECLARE
  n varchar;
BEGIN
  FOR x IN 1..9 LOOP
--Testcase 138:
    SELECT b INTO n from numbers WHERE a=x;
    raise notice 'Found number %', n;
  end loop;
  return;
END
$$ LANGUAGE plpgsql;
--Testcase 43:
SELECT test_param_WHERE();

--Testcase 44:
SELECT b from numbers WHERE a=1;
--Testcase 45:
EXPLAIN(COSTS OFF) SELECT b from numbers WHERE a=1;

--Testcase 46:
SELECT a FROM numbers WHERE b = (SELECT NULL::text);


--Testcase 47:
PREPARE stmt1 (int, int) AS
  SELECT * FROM numbers WHERE a=$1 or a=$2;
--Testcase 48:
EXECUTE stmt1(1,2);
--Testcase 49:
EXECUTE stmt1(2,2); 
--Testcase 50:
EXECUTE stmt1(3,2); 
--Testcase 51:
EXECUTE stmt1(4,2);
-- generic plan
--Testcase 52:
EXECUTE stmt1(5,2); 
--Testcase 53:
EXECUTE stmt1(6,2); 
--Testcase 54:
EXECUTE stmt1(7,2); 

--Testcase 55:
DELETE FROM employee;
--Testcase 56:
DELETE FROM department;
--Testcase 57:
DELETE FROM empdata;
--Testcase 58:
DELETE FROM numbers;

BEGIN;
--Testcase 59:
INSERT INTO numbers VALUES(1, 'One');
--Testcase 60:
INSERT INTO numbers VALUES(2, 'Two');
COMMIT;

--Testcase 61:
SELECT * from numbers;

BEGIN;
--Testcase 62:
INSERT INTO numbers VALUES(3, 'Three');
ROLLBACK;
--Testcase 63:
SELECT * from numbers;

BEGIN;
--Testcase 64:
INSERT INTO numbers VALUES(4, 'Four');
SAVEPOINT my_savepoint;
--Testcase 65:
INSERT INTO numbers VALUES(5, 'Five');
ROLLBACK TO SAVEPOINT my_savepoint;
--Testcase 66:
INSERT INTO numbers VALUES(6, 'Six');
COMMIT;

--Testcase 67:
SELECT * from numbers;

-- duplicate key
--Testcase 68:
INSERT INTO numbers VALUES(1, 'One');
--Testcase 69:
DELETE from numbers;

BEGIN;
--Testcase 70:
INSERT INTO numbers VALUES(1, 'One');
--Testcase 71:
INSERT INTO numbers VALUES(2, 'Two');
COMMIT;
-- violate unique constraint
--Testcase 72:
UPDATE numbers SET b='Two' WHERE a = 1; 
--Testcase 73:
SELECT * from numbers;

-- push down
--Testcase 74:
explain (costs off) SELECT * from numbers WHERE  a = any(ARRAY[2,3,4,5]::int[]);
-- (1,2,3) is pushed down
--Testcase 75:
explain (costs off) SELECT * from numbers WHERE a in (1,2,3) AND (1,2) < (a,5);

-- not push down
--Testcase 76:
explain (costs off) SELECT * from numbers WHERE a in (a+2*a,5);
-- not push down
--Testcase 77:
explain (costs off) SELECT * from numbers WHERE  a = any(ARRAY[1,2,a]::int[]);

--Testcase 78:
SELECT * from numbers WHERE  a = any(ARRAY[2,3,4,5]::int[]);
--Testcase 79:
SELECT * from numbers WHERE  a = any(ARRAY[1,2,a]::int[]);

--Testcase 80:
INSERT INTO multiprimary VALUES(1,2,3);
--Testcase 81:
INSERT INTO multiprimary VALUES(1,2,4);
--Testcase 82:
UPDATE multiprimary SET b = 10 WHERE c = 3;
--Testcase 83:
SELECT * from multiprimary;
--Testcase 84:
UPDATE multiprimary SET a = 10 WHERE a = 1;
--Testcase 85:
SELECT * from multiprimary;
--Testcase 86:
UPDATE multiprimary SET a = 100, b=200, c=300 WHERE a=10 AND b=10;
--Testcase 87:
SELECT * from multiprimary;
--Testcase 88:
UPDATE multiprimary SET a = 1234;
--Testcase 89:
SELECT * from multiprimary;
--Testcase 90:
UPDATE multiprimary SET a = a+1, b=b+1 WHERE b=200 AND c=300;

--Testcase 91:
SELECT * from multiprimary;
--Testcase 92:
DELETE from multiprimary WHERE a = 1235;
--Testcase 93:
SELECT * from multiprimary;
--Testcase 94:
DELETE from multiprimary WHERE b = 2;
--Testcase 95:
SELECT * from multiprimary;

--Testcase 96:
INSERT INTO multiprimary VALUES(1,2,3);
--Testcase 97:
INSERT INTO multiprimary VALUES(1,2,4);
--Testcase 98:
INSERT INTO multiprimary VALUES(1,10,20);
--Testcase 99:
INSERT INTO multiprimary VALUES(2,20,40);



--Testcase 100:
SELECT count(distinct a) from multiprimary;
--Testcase 101:
SELECT sum(b),max(b), min(b) from multiprimary;
--Testcase 102:
SELECT sum(b+5)+2 from multiprimary group by b/2 order by b/2;
--Testcase 103:
SELECT sum(a) from multiprimary group by b having sum(a) > 0 order by sum(a);
--Testcase 104:
SELECT sum(a) A from multiprimary group by b having avg(abs(a)) > 0 AND sum(a) > 0 order by A;
--Testcase 105:
SELECT count(nullif(a, 1)) FROM multiprimary;
--Testcase 106:
SELECT a,a FROM multiprimary group by 1,2;
--Testcase 107:
SELECT * from multiprimary, numbers WHERE multiprimary.a=numbers.a;

--Testcase 108:
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(a) FROM multiprimary HAVING sum(a) > 0;
--Testcase 109:
SELECT sum(a) FROM multiprimary HAVING sum(a) > 0;

--Testcase 110:
INSERT INTO numbers VALUES(4, 'Four');

-- All where clauses are pushed down
--Testcase 111:
SELECT * FROM numbers WHERE abs(a) = 4 AND upper(b) = 'FOUR' AND lower(b) = 'four';
--Testcase 112:
EXPLAIN (verbose, costs off)  SELECT b, length(b) FROM numbers WHERE abs(a) = 4 AND upper(b) = 'FOUR' AND lower(b) = 'four';

-- Only "length(b) = 4" are pushed down
--Testcase 113:
SELECT b, length(b) FROM numbers WHERE length(b) = 4 AND power(1, a) != 0 AND length(reverse(b)) = 4;
--Testcase 114:
EXPLAIN (verbose, costs off) SELECT b, length(b) FROM numbers WHERE length(b) = 4 AND power(1, a) != 0 AND length(reverse(b)) = 4;

--Testcase 115:
INSERT INTO multiprimary (b,c) VALUES (99, 100);
--Testcase 116:
SELECT c FROM multiprimary WHERE COALESCE(a,b,c) = 99;


--Testcase 139:
CREATE FOREIGN TABLE multiprimary2(a int, b int, c int OPTIONS(column_name 'b')) SERVER sqlite_svr OPTIONS (table 'multiprimary');
--Testcase 117:
SELECT * FROM multiprimary2;
ALTER FOREIGN TABLE multiprimary2 ALTER COLUMN a OPTIONS(ADD column_name 'b');
--Testcase 118:
SELECT * FROM multiprimary2;
ALTER FOREIGN TABLE multiprimary2 ALTER COLUMN b OPTIONS (column_name 'nosuch column');
--Testcase 119:
SELECT * FROM multiprimary2;
--Testcase 140:
EXPLAIN (VERBOSE) SELECT * FROM multiprimary2;
--Testcase 120:
SELECT a FROM multiprimary2 WHERE b = 1;


--Testcase 141:
CREATE FOREIGN TABLE columntest(a int OPTIONS(column_name 'a a', key 'true'), "b b" int  OPTIONS(key 'true'), c int OPTIONS(column_name 'c c')) SERVER sqlite_svr;
--Testcase 121:
INSERT INTO columntest VALUES(1,2,3);
--Testcase 122:
UPDATE columntest SET c=10 WHERE a = 1;
--Testcase 123:
SELECT * FROM columntest;
--Testcase 124:
UPDATE columntest SET a=100 WHERE c = 10;
--Testcase 125:
SELECT * FROM columntest;
--Testcase 126:
INSERT INTO noprimary VALUES(1,'2');
--Testcase 127:
INSERT INTO noprimary SELECT * FROM noprimary;
--Testcase 128:
SELECT * FROM noprimary;

--Testcase 142:
DROP FUNCTION test_param_WHERE();
--Testcase 143:
DROP FOREIGN TABLE numbers;
--Testcase 144:
DROP FOREIGN TABLE department;
--Testcase 145:
DROP FOREIGN TABLE employee;
--Testcase 146:
DROP FOREIGN TABLE empdata;
--Testcase 147:
DROP FOREIGN TABLE multiprimary;
--Testcase 148:
DROP FOREIGN TABLE multiprimary2;
--Testcase 149:
DROP FOREIGN TABLE columntest;
--Testcase 150:
DROP FOREIGN TABLE noprimary;

--Testcase 151:
DROP SERVER sqlite_svr;
--Testcase 152:
DROP EXTENSION duckdb_fdw CASCADE;

