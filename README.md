# DuckDB Foreign Data Wrapper for PostgreSQL
This PostgreSQL extension is a Foreign Data Wrapper for [DuckDB][1].

The current version can work with PostgreSQL 9.6, 10, 11, 12 and 13.


## Installation
### 1. Install DuckDB library


You can  [download DuckDB source code][2] and build DuckDB.

### 2. Build and install duckdb_fdw

Add a directory of pg_config to PATH and build and install duckdb_fdw.
<pre>
make USE_PGXS=1
make install USE_PGXS=1
</pre>

If you want to build duckdb_fdw in a source tree of PostgreSQL, use
<pre>
make
make install
</pre>

## Usage

you need create a dumb table in DuckDB first,
```
CREATE TABLE sqlite_stat1(tbl text,idx text,stat text);
```

### Load extension
<pre>
CREATE EXTENSION duckdb_fdw;
</pre>

### Create server
Please specify DuckDB database path using `database` option:
<pre>
CREATE SERVER DuckDB_server FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '/tmp/test.db');
</pre>


### Create foreign table
Please specify `table` option if DuckDB table name is different from foreign table name.
<pre>
CREATE FOREIGN TABLE t1(a integer, b text) SERVER DuckDB_server OPTIONS (table 't1_DuckDB');
</pre>

If you want to update tables, please add `OPTIONS (key 'true')` to a primary key or unique key like the following:
<pre>
CREATE FOREIGN TABLE t1(a integer OPTIONS (key 'true'), b text) SERVER DuckDB_server OPTIONS (table 't1_DuckDB');
</pre>

If you need to convert INT DuckDB column (epoch Unix Time) to be treated/visualized as TIMESTAMP in PostgreSQL, please add `OPTIONS (column_type 'INT')` when
defining FOREIGN table at PostgreSQL like the following:
<pre>
CREATE FOREIGN TABLE t1(a integer, b text, c timestamp without time zone OPTIONS (column_type 'INT')) SERVER DuckDB_server OPTIONS (table 't1_DuckDB');
</pre>
### Import foreign schema
<pre>
IMPORT FOREIGN SCHEMA public FROM SERVER DuckDB_server INTO public;
</pre>

### Access foreign table
<pre>
SELECT * FROM t1;
</pre>

## Features
- Support CSV and parquet
- Columnar-vectorized query execution engine
- DuckDB is designed to support analytical query workloads, also known as Online analytical processing (OLAP)
- WHERE clauses are pushdowned  
- Aggregate function are pushdowned
- Order By is pushdowned.
- Limit and Offset are pushdowned (*when all tables queried are fdw)
- Transactions  

## Limitations
- `COPY` command for foreign tables is not supported
- Insert into a partitioned table which has foreign partitions is not supported
- Update and Delete still do not support
## Contributing
Opening issues and pull requests on GitHub are welcome.


## Special thanks
https://github.com/pgspider/sqlite_fdw

## License
MIT

[1]: https://www.DuckDB.org/index.html
[2]: https://duckdb.org/docs/installation/