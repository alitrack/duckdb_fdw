
DROP TABLE IF EXISTS department;
DROP TABLE IF EXISTS employee;
DROP TABLE IF EXISTS empdata;
DROP TABLE IF EXISTS numbers;
DROP TABLE IF EXISTS limittest;
DROP TABLE IF EXISTS grem1_1;
DROP TABLE IF EXISTS grem1_2;

CREATE TABLE department(department_id int primary key, department_name text);
CREATE TABLE employee(emp_id int primary key, emp_name text, emp_dept_id int);
CREATE TABLE empdata(emp_id int primary key, emp_dat bytea);
CREATE TABLE numbers(a int primary key, b varchar(255) unique);
CREATE TABLE t(a integer primary key, b integer);
CREATE TABLE multiprimary(a integer, b integer, c integer, primary key(b,c));
CREATE TABLE columntest("a a" integer, "b b" integer,"c c" integer, primary key("a a","b b") );
CREATE TABLE noprimary(a integer, b text);
CREATE TABLE limittest(id int primary key, x integer, y text);
create table grem1_1 (a int primary key, b int generated always as (a * 2) stored);
create table grem1_2 (a int primary key, b int generated always as (a * 2) stored, c int generated always as (a * 3) stored, d int generated always as (a * 4) stored);

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
CREATE TABLE "type_DATE" (col date primary key);
CREATE TABLE "type_TIME" (col time primary key);
CREATE TABLE BitT (p integer primary key, a BIT(3), b BIT VARYING(5));
CREATE TABLE notype (a);
CREATE TABLE typetest (i integer, v varchar(10) , c char(10), t text, d datetime, ti timestamp);
CREATE TABLE type_TEXT (col text primary key);
CREATE TABLE alltypetest (c1 int,     c2 tinyint,  c3 smallint, c4 mediumint,  c5 bigint,           c6 unsign big int,    c7 int8,               c8 character(10),       c9 varchar(255),            c10 character varying(255),        c11 nchar(55),                    c12 native character(70),      c13 nvarchar(100),                            c14 text,                          c15 clob,                               c16 blob,                         c17 real,          c18 double,         c19 double precision,   c20 float,           c21 numeric,  c22 decimal(10,5),  c23 date,            c24 datetime);
INSERT INTO  alltypetest VALUES (583647,   127,        12767,       388607,      2036854775807,          573709551615,      2036854775807,             'abcdefghij',       'abcdefghijjhgfjfuafh',       'Côte dIvoire Fijifoxju',        'Hôm nay tôi rất vui',                 'I am happy today',              '今日はとても幸せです 今日はとても幸せです',            'The quick brown fox jumps o'       ,  'ABCDEFGHIJKLMNOPQRSTUVWX',          x'4142434445',                       3.40E+18,          1.79769E+108,          1.79769E+88,          1.79E+108,          1234,        99999.99999,        '9999-12-31',         '9999-12-31 23:59:59');

-- a table that is missing some fields
CREATE TABLE shorty (
   id  integer primary key, 
   c   character(10)
);

CREATE TABLE "A a" (col int primary key);

-- test for issue #44 github
-- CREATE VIRTUAL TABLE fts_table USING fts5(name, description, tokenize = porter);

analyze;
