# DuckDB Foreign Data Wrapper for PostgreSQL

This PostgreSQL extension is a Foreign Data Wrapper for [DuckDB][1].

The current version can work with PostgreSQL 9.6, 10, 11, 12 and 13.

## Installation

### 1. Install DuckDB library

You can  [download DuckDB source code][2] and build DuckDB.

```bash
git clone https://github.com/cwida/duckdb
cd duckdb
make 
```

what we need is 

build/release/tools/sqlite3_api_wrapper/libsqlite3_api_wrapper.dylib（for Linux is libsqlite3_api_wrapper.so，and Windows is libsqlite3_api_wrapper.dll）
tools/sqlite3_api_wrapper/include/sqlite3.h

by default, ```make install``` does not install these two files.

### 2. Build and install duckdb_fdw

Add a directory of pg_config to PATH and build and install duckdb_fdw.

```bash
make USE_PGXS=1
make install USE_PGXS=1
```

If you want to build duckdb_fdw in a source tree of PostgreSQL, use

```bash
make
make install
```

## Usage

### Load extension

```sql
CREATE EXTENSION duckdb_fdw;
```

### Create server

Please specify DuckDB database path using `database` option:

```sql
CREATE SERVER DuckDB_server FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/test.db');
```

### Create foreign table

Please specify `table` option if DuckDB table name is different from foreign table name.

```sql
CREATE FOREIGN TABLE t1(a integer, b text) SERVER DuckDB_server OPTIONS (table 't1_DuckDB');
```

If you want to update tables, please add `OPTIONS (key 'true')` to a primary key or unique key like the following:

```sql
CREATE FOREIGN TABLE t1(a integer OPTIONS (key 'true'), b text) 
SERVER DuckDB_server OPTIONS (table 't1_DuckDB');
```

If you need to convert INT DuckDB column (epoch Unix Time) to be treated/visualized as TIMESTAMP in PostgreSQL, please add `OPTIONS (column_type 'INT')` when
defining FOREIGN table at PostgreSQL like the following:

```sql
CREATE FOREIGN TABLE t1(a integer, b text, c timestamp without time zone OPTIONS (column_type 'INT')) 
SERVER DuckDB_server OPTIONS (table 't1_DuckDB');
```

### Import foreign schema

```sql
IMPORT FOREIGN SCHEMA public FROM SERVER DuckDB_server INTO public;
```

### Access foreign table

```sql
SELECT * FROM t1;
```

## Features

- Update & Delete support
- Support execute sql on foreign Database directly
- Support CSV and Parquet
- Columnar-vectorized query execution engine
- DuckDB is designed to support analytical query workloads, also known as Online analytical processing (OLAP)
- WHERE clauses are pushdowned  
- Aggregate function are pushdowned
- Order By is pushdowned.
- Limit and Offset are pushdowned (*when all tables queried are fdw)
- Transactions  

## Limitations

- Insert into a partitioned table which has foreign partitions is not supported

## Special Function

```sql
FUNCTION duckdb_execute(server name, stmt text) RETURNS void
```

This function can be used to execute arbitrary SQL statements on the remote DuckDB server. That will only work with statements that do not return results (typically DDL statements).

Be careful when using this function, since it might disturb the transaction management of duckdb_fdw. Remember that running a DDL statement in DuckDB will issue an implicit COMMIT. You are best advised to use this function outside of multi-statement transactions.

It is very useful to use command that duckdb_fdw does not support, for example,

- add more table or view to DuckDB directly.
  
``` sql
SELECT duckdb_execute('duckdb_server'
,'create or replace view iris_parquet  as select * from parquet_scan(''temp/iris.parquet'');');

create foreign TABLE duckdb.iris_parquet(
"Sepal.Length" float,
"Sepal.Width" float,
"Petal.Length" float,
"Petal.Width" float,
"Species" text)
      SERVER duckdb_server OPTIONS (table 'iris_parquet');
```

- run Copy command on Foreign table

```sql
SELECT duckdb_execute('duckdb_server'
,'CREATE TABLE test (a INTEGER, b INTEGER, c VARCHAR(10));
');
SELECT duckdb_execute('duckdb_server'
,'COPY test FROM ''/tmp/test.csv'';');


```

## Contributing

Opening issues and pull requests on GitHub are welcome.

## Special thanks

https://github.com/pgspider/sqlite_fdw

## License

MIT

[1]: https://www.DuckDB.org/index.html
[2]: https://duckdb.org/docs/installation/