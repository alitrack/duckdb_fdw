
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS empdata;
DROP TABLE IF EXISTS numbers;
CREATE TABLE department(department_id int primary key, department_name text);
CREATE TABLE employee(emp_id int primary key, emp_name text, emp_dept_id int);
CREATE TABLE empdata(emp_id int primary key, emp_dat bytea);
CREATE TABLE numbers(a int primary key, b varchar(255) unique);
CREATE TABLE t(a integer primary key, b integer);
CREATE TABLE multiprimary(a integer, b integer, c integer, primary key(b,c));
CREATE TABLE columntest("a a" integer, "b b" integer,"c c" integer, primary key("a a","b b") );
CREATE TABLE noprimary(a integer, b text);


CREATE TABLE "type_STRING" (col text primary key);
CREATE TABLE "type_BOOLEAN" (col boolean primary key);
CREATE TABLE "type_BYTE" (col char(1) primary key);
CREATE TABLE "type_SINT" (col smallint primary key);
CREATE TABLE "type_BINT" (col bigint primary key);
CREATE TABLE "type_INTEGER" (col integer primary key); -- convert to bigint
CREATE TABLE "type_FLOAT" (col float primary key);
CREATE TABLE "type_DOUBLE" (col double primary key);
CREATE TABLE "type_TIMESTAMP" (col timestamp primary key, b timestamp);--, c date);
CREATE TABLE "type_BLOB" (col blob primary key);
CREATE TABLE BitT (p integer primary key, a BIT(3), b BIT VARYING(5));
CREATE TABLE notype (a);
CREATE TABLE typetest (i integer, v varchar(10) , c char(10), t text, d datetime, ti timestamp);

-- a table that is missing some fields
CREATE TABLE shorty (
   id  integer primary key, 
   c   character(10)
);

CREATE TABLE "A a" (col int primary key);

analyze;
