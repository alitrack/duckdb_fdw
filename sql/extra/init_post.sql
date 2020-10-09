DROP TABLE IF EXISTS "T 0";
DROP TABLE IF EXISTS "T 1";
DROP TABLE IF EXISTS "T 2";
DROP TABLE IF EXISTS "T 3";
DROP TABLE IF EXISTS "T 4";
DROP TABLE IF EXISTS base_tbl;
DROP TABLE IF EXISTS loc1;
DROP TABLE IF EXISTS loct;
DROP TABLE IF EXISTS loct1;
DROP TABLE IF EXISTS loct2;
DROP TABLE IF EXISTS loct3;
DROP TABLE IF EXISTS loct4;
DROP TABLE IF EXISTS loct5;
DROP TABLE IF EXISTS loct6;
DROP TABLE IF EXISTS loct7;

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
create table loct5 (a int check (a in (1)), b text);
create table loct6 (a int check (a in (2)), b text);
create table loct7 (a int check (a in (1)), b text);

create table local_tbl (c1 int primary key, c2 int, c3 text);
create table ft3 (f1 text, f2 text, f3 text, primary key (f1, f2, f3));

create table foreign_tbl (a int primary key, b int);
create table grem1 (a int primary key, b int);

analyze;
