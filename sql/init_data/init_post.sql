DROP TABLE IF EXISTS "T 0";
DROP TABLE IF EXISTS "T 1";
DROP TABLE IF EXISTS "T 2";
DROP TABLE IF EXISTS "T 3";
DROP TABLE IF EXISTS "T 4";
DROP TABLE IF EXISTS base_tbl;
DROP TABLE IF EXISTS local_tbl;
DROP TABLE IF EXISTS ft3;
DROP TABLE IF EXISTS foreign_tbl;
DROP TABLE IF EXISTS grem1;
DROP TABLE IF EXISTS grem1_post14;
DROP TABLE IF EXISTS loc1;
DROP TABLE IF EXISTS loct;
DROP TABLE IF EXISTS loct1;
DROP TABLE IF EXISTS loct2;
DROP TABLE IF EXISTS loct3;
DROP TABLE IF EXISTS loct4;
DROP TABLE IF EXISTS loct4_2;
DROP TABLE IF EXISTS loct5;
DROP TABLE IF EXISTS loct6;
DROP TABLE IF EXISTS loct7;
DROP TABLE IF EXISTS t1_constraint;
DROP TABLE IF EXISTS tru_rtable0;
DROP TABLE IF EXISTS tru_pk_table;
DROP TABLE IF EXISTS tru_fk_table;
DROP TABLE IF EXISTS tru_rtable_parent;
DROP TABLE IF EXISTS tru_rtable_child;
DROP TABLE IF EXISTS loct_empty;
DROP TABLE IF EXISTS batch_table;

CREATE TABLE "T 0" (
	"C 1" int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 text check (c8 IN ('foo', 'bar', 'buz')),
	CONSTRAINT t1_pkey PRIMARY KEY ("C 1")
);
CREATE TABLE "T 1" (
	"C 1" int,
	c2 int NOT NULL,
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 text check (c8 IN ('foo', 'bar', 'buz')),
	CONSTRAINT t1_pkey PRIMARY KEY ("C 1")
);
CREATE TABLE "T 2" (
	c1 int,
	c2 text,
	CONSTRAINT t2_pkey PRIMARY KEY (c1)
);
CREATE TABLE "T 3" (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	CONSTRAINT t3_pkey PRIMARY KEY (c1)
);
CREATE TABLE "T 4" (
	c1 int,
	c2 int NOT NULL,
	c3 text,
	CONSTRAINT t4_pkey PRIMARY KEY (c1)
);
CREATE TABLE base_tbl (a int, b int);
CREATE TABLE loc1 (f1 INTEGER, f2 text, id integer primary key autoincrement);
CREATE TABLE loct (aa TEXT, bb TEXT);
CREATE TABLE loct1 (f1 int, f2 int, f3 int);
CREATE TABLE loct2 (f1 int, f2 int, f3 int);
create table loct3 (a int, b text);
create table loct4 (a int, b text);
create table loct4_2 (f1 int, f2 int, f3 int);
create table loct5 (a int check (a in (1)), b text);
create table loct6 (a int check (a in (2)), b text);
create table loct7 (a int check (a in (1)), b text);

create table local_tbl (c1 int primary key, c2 int, c3 text);
create table ft3 (f1 text, f2 text, f3 text, primary key (f1, f2, f3));

create table foreign_tbl (a int primary key, b int);
create table grem1 (a int primary key, b int);
create table grem1_post14 (a int primary key, b int generated always as (a * 2) stored);

CREATE TABLE t1_constraint (
	c1 int primary key,
	c2 int NOT NULL check (c2 >= 0),
	c3 text,
	c4 timestamptz,
	c5 timestamp,
	c6 varchar(10),
	c7 char(10),
	c8 text check (c8 IN ('foo', 'bar', 'buz'))
);
CREATE TABLE tru_rtable0 (id int primary key);
CREATE TABLE tru_pk_table(id int primary key);
CREATE TABLE tru_fk_table(fkey int, CONSTRAINT tfk_pkey FOREIGN KEY (fkey) REFERENCES tru_pk_table(id) ON DELETE CASCADE);
CREATE TABLE tru_rtable_parent (id int);
CREATE TABLE tru_rtable_child (id int);
CREATE TABLE loct_empty (c1 int NOT NULL, c2 text, CONSTRAINT tloct_pkey PRIMARY KEY (c1));
CREATE TABLE batch_table ( x int );
analyze;
