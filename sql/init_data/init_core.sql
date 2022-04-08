.mode csv

DROP TABLE IF EXISTS FLOAT4_TBL;
DROP TABLE IF EXISTS FLOAT4_TMP;
DROP TABLE IF EXISTS FLOAT8_TBL;
DROP TABLE IF EXISTS FLOAT8_TMP;
DROP TABLE IF EXISTS INT4_TBL;
DROP TABLE IF EXISTS INT4_TMP;
DROP TABLE IF EXISTS INT8_TBL;
DROP TABLE IF EXISTS test_having;
DROP TABLE IF EXISTS onek;
DROP TABLE IF EXISTS tenk1;

CREATE TABLE FLOAT4_TBL (f1  REAL);
CREATE TABLE FLOAT4_TMP (f1  REAL, id integer primary key autoincrement);
CREATE TABLE FLOAT8_TBL(f1 DOUBLE PRECISION);
CREATE TABLE FLOAT8_TMP (f1 DOUBLE PRECISION, f2 DOUBLE PRECISION, id integer primary key autoincrement);
CREATE TABLE INT4_TBL(f1 int4);
CREATE TABLE INT4_TMP (f1 int4, f2 int,  id integer primary key autoincrement);
CREATE TABLE INT8_TBL(
	q1 int8,
	q2 int8,
	CONSTRAINT t1_pkey PRIMARY KEY (q1, q2)
);
CREATE TABLE INT8_TMP(
	q1 int8,
	q2 int8,
	q3 int4,
	q4 int2,
	q5 text,
	id integer primary key autoincrement
);

CREATE TABLE INT2_TBL(f1 int2);
--Testcase 1:
INSERT INTO INT2_TBL(f1) VALUES ('0   ');
--Testcase 2:
INSERT INTO INT2_TBL(f1) VALUES ('  1234 ');
--Testcase 3:
INSERT INTO INT2_TBL(f1) VALUES ('    -1234');
--Testcase 4:
INSERT INTO INT2_TBL(f1) VALUES ('34.5');
-- largest and smallest values
--Testcase 5:
INSERT INTO INT2_TBL(f1) VALUES ('32767');
--Testcase 6:
INSERT INTO INT2_TBL(f1) VALUES ('-32767');

CREATE TABLE test_having (a int, b int, c char(8), d char);
CREATE TABLE onek (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE onek2 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE tenk2 (
	unique1 	int4,
	unique2 	int4,
	two 	 	int4,
	four 		int4,
	ten			int4,
	twenty 		int4,
	hundred 	int4,
	thousand 	int4,
	twothousand int4,
	fivethous 	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);

CREATE TABLE aggtest (
	a 			int2,
	b			float4
);

CREATE TABLE student (
	name 		text,
	age			int4,
	location 	point,
	gpa 		float8
);

CREATE TABLE person (
	name 		text,
	age			int4,
	location 	point
);

-- FOR prepare.sql

CREATE TABLE road (
	name		text,
	thepath 	path
);

create table road_tmp (a int, b int, id integer primary key autoincrement);

CREATE TABLE dates (
	name			TEXT,
	date_as_text	TEXT,
	date_as_number	FLOAT8
);

.separator "\t"
.import /tmp/onek.data onek
.import /tmp/onek.data onek2
.import /tmp/tenk.data tenk1
.import /tmp/agg.data aggtest
.import /tmp/student.data student
.import /tmp/person.data person
.import /tmp/streets.data road
.import /tmp/datetimes.data dates

--Testcase 7:
INSERT INTO tenk2 SELECT * FROM tenk1;

CREATE TABLE bitwise_test(
  i2 INT2,
  i4 INT4,
  i8 INT8,
  i INTEGER,
  x INT2
);

CREATE TABLE bool_test(
  b1 BOOL,
  b2 BOOL,
  b3 BOOL,
  b4 BOOL);

CREATE TABLE bool_test_tmp(
  b1 BOOL,
  b2 BOOL, primary key (b1, b2));

-- FOR AGGREGATEQ.SQL

create table minmaxtest(f1 int);

create table agg_t1 (a int, b int, c int, d int, primary key (a, b));
create table agg_t2 (x int, y int, z int, primary key (x, y));
create table agg_t3 (a float8, b float8, id integer primary key autoincrement);
create table agg_t4 (a float4, b float4, id integer primary key autoincrement);
create table agg_t5 (a numeric, b numeric, id integer primary key autoincrement);
create table agg_t6 (a float8, id integer primary key autoincrement);
create table agg_t7 (a float8, b float8, c float8, d float8, id integer primary key autoincrement);
create table agg_t8 (a text, b text, primary key (a));
CREATE TABLE regr_test (x float8, y float8, id integer primary key autoincrement);
create table agg_t9 (a int, b int, c int, primary key (a, b));
create table agg_t10(one int, id integer primary key autoincrement);
create table agg_t11(one int, two int, id integer primary key autoincrement);
create table agg_t12(a int, id integer primary key autoincrement);
create table agg_t13(x int, id integer primary key autoincrement);
create table agg_t14(x int, y int, id integer primary key autoincrement);
create table agg_data_2k(g int , id integer primary key autoincrement);
create table agg_data_20k(g int , id integer primary key autoincrement);
create table t1(f1 int4, f2 int8);
create table t2(f1 int8, f22 int8);
create table agg_t15(a text, b int, c int, id integer primary key autoincrement);
create table agg_t16(a text, b text, id integer primary key autoincrement);
create table agg_t17(foo text, bar text);
create table agg_t18 (inner_c int);
create table agg_t19 (outer_c int);
create table agg_t20 (x text);
create table agg_t21 (x int);

-- multi-arg aggs
create table multi_arg_agg (a int, b int, c text);

create table agg_group_1 (c1 int, c2 numeric, c3 int);
create table agg_group_2 (a int , c1 numeric, c2 text, c3 int);
create table agg_group_3 (c1 numeric, c2 int, c3 int);
create table agg_group_4 (c1 numeric, c2 text, c3 int);

create table agg_hash_1 (c1 int, c2 numeric, c3 int);
create table agg_hash_2 (a int , c1 numeric, c2 text, c3 int);
create table agg_hash_3 (c1 numeric, c2 int, c3 int);
create table agg_hash_4 (c1 numeric, c2 text, c3 int);

-- FOR float4.sql
create table testdata(bits text, id integer primary key autoincrement);

-- FOR int4.sql
create table numeric_tmp(f1 numeric, f2 numeric , id integer primary key autoincrement);

CREATE TABLE VARCHAR_TBL(f1 varchar(4));

--Testcase 8:
INSERT INTO VARCHAR_TBL (f1) VALUES ('a');
--Testcase 9:
INSERT INTO VARCHAR_TBL (f1) VALUES ('ab');
--Testcase 10:
INSERT INTO VARCHAR_TBL (f1) VALUES ('abcd');

create table bytea_test_table(v bytea);

-- FOR numeric.sql

CREATE TABLE num_data (id int4, val numeric, primary key (id));
CREATE TABLE num_exp_add (id1 int4, id2 int4, expected numeric, primary key (id1, id2));
CREATE TABLE num_exp_sub (id1 int4, id2 int4, expected numeric, primary key (id1, id2));
CREATE TABLE num_exp_div (id1 int4, id2 int4, expected numeric, primary key (id1, id2));
CREATE TABLE num_exp_mul (id1 int4, id2 int4, expected numeric, primary key (id1, id2));
CREATE TABLE num_exp_sqrt (id int4, expected numeric, primary key (id));
CREATE TABLE num_exp_ln (id int4, expected numeric, primary key (id));
CREATE TABLE num_exp_log10 (id int4, expected numeric, primary key (id));
CREATE TABLE num_exp_power_10_ln (id int4, expected numeric, primary key (id));

CREATE TABLE num_result (id1 int4, id2 int4, result numeric, primary key (id1, id2));
CREATE TABLE v (id int4, x numeric, val float8, primary key (id));
INSERT INTO v(x) VALUES ('1e340'), ('-1e340');
CREATE TABLE fract_only (id int, val numeric(4,4));
CREATE TABLE ceil_floor_round (a numeric primary key);
CREATE TABLE width_bucket_tbl (id1 numeric, id2 numeric, id3 numeric, id4 int, id integer primary key autoincrement);
CREATE TABLE width_bucket_test (operand_num numeric, operand_f8 float8);
CREATE TABLE num_input_test (n1 numeric);

CREATE TABLE num_tmp (n1 numeric, n2 numeric, id integer primary key autoincrement);
CREATE TABLE to_number_tbl(a text, id integer primary key autoincrement);

-- FOR join.sql

create table q1 (i int);
create table q2 (i int);
CREATE TABLE foo (f1 int);

CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);

CREATE TABLE J2_TBL (
  i integer,
  k integer
);

create table sub_tbl (key1 int, key3 int, key5 int, key6 int, value1 int, id integer primary key autoincrement);

CREATE TABLE t11 (name TEXT, n INTEGER);
CREATE TABLE t21 (name TEXT, n INTEGER);
CREATE TABLE t31 (name TEXT, n INTEGER);
create table x (x1 int, x2 int);
create table y (y1 int, y2 int);

CREATE TABLE t12 (a int, b int);
CREATE TABLE t22 (a int, b int);
CREATE TABLE t32 (x int, y int);

CREATE TABLE tt1 ( tt1_id int4, joincol int4 );
CREATE TABLE tt2 ( tt2_id int4, joincol int4 );
create table tt3(f1 int, f2 text);
create table tt4(f1 int);
create table tt4x(c1 int, c2 int, c3 int);
create table tt5(f1 int, f2 int);
create table tt6(f1 int, f2 int);
create table xx (pkxx int);
create table yy (pkyy int, pkxx int);
create table zt1 (f1 int primary key);
create table zt2 (f2 int primary key);
create table zt3 (f3 int primary key);

create table a1 (i integer);
create table b1 (x integer, y integer);

create table a2 (
     code char not null,
     primary key (code)
);
create table b2 (
     a char not null,
     num integer not null,
     primary key (a, num)
);
create table c2 (
     name char not null,
     a char,
     primary key (name)
);

create table nt1 (
  id int primary key,
  a1 boolean,
  a2 boolean
);
create table nt2 (
  id int primary key,
  nt1_id int,
  b1 boolean,
  b2 boolean,
  foreign key (nt1_id) references nt1(id)
);
create table nt3 (
  id int primary key,
  nt2_id int,
  c1 boolean,
  foreign key (nt2_id) references nt2(id)
);

CREATE TABLE TEXT_TBL (f1 text);

--Testcase 11:
INSERT INTO TEXT_TBL VALUES ('doh!');
--Testcase 12:
INSERT INTO TEXT_TBL VALUES ('hi de ho neighbor');

CREATE TABLE a3 (id int PRIMARY KEY, b_id int);
CREATE TABLE b3 (id int PRIMARY KEY, c_id int);
CREATE TABLE c3 (id int PRIMARY KEY);
CREATE TABLE d3 (a int, b int);

create table parent (k int primary key, pd int);
create table child (k int unique, cd int);

CREATE TABLE a4 (id int PRIMARY KEY);
CREATE TABLE b4 (id int PRIMARY KEY, a_id int);

create table innertab (id int8 primary key, dat1 int8);
create table uniquetbl (f1 text unique);

create table join_pt1 (a int, b int, c varchar);

create table fkest (a int, b int, c int unique, primary key(a,b));
create table fkest1 (a int, b int, primary key(a,b) foreign key (a,b) references fkest);

create table j11 (id int primary key);
create table j21 (id int primary key);
create table j31 (id int);

create table j12 (id1 int, id2 int, primary key(id1,id2));
create table j22 (id1 int, id2 int, primary key(id1,id2));
create table j32 (id1 int, id2 int, primary key(id1,id2));

create table inserttest01 (col1 int4, col2 int4 NOT NULL, col3 text default 'testing');


CREATE TABLE update_test (
	i   INT PRIMARY KEY,
    a   INT DEFAULT 10,
    b   INT,
    c   TEXT
);

create table upsert_test (a int primary key, b text);

