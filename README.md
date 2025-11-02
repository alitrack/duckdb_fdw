# DuckDB Foreign Data Wrapper for PostgreSQL

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [DuckDB](https://duckdb.org/) database files through DuckDB's SQLite compatibility layer. This FDW works with PostgreSQL 9.6 ... 18 and uses DuckDB's built-in SQLite API compatibility.

<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" align="center" height="100" alt="PostgreSQL"/>	+	<img src="https://user-images.githubusercontent.com/41448637/222924178-7e622cad-fec4-49e6-b8fb-33be4447f17d.png" align="center" height="100" alt="DuckDB"/>

## Architecture Overview

**Important Technical Note**: This FDW uses DuckDB's SQLite compatibility layer rather than direct DuckDB API calls. The implementation leverages DuckDB's ability to act as a drop-in replacement for SQLite, providing:

- SQLite API compatibility (`sqlite3.h`)
- DuckDB's advanced query optimization and performance
- Support for DuckDB's rich data types and functions
- Direct file-based database access

## Contents

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Identifier case handling](#identifier-case-handling)
7. [Generated columns](#generated-columns)
8. [Character set handling](#character-set-handling)
9. [Examples](#examples)
10. [Limitations](#limitations)
11. [Tests](#tests)
12. [Contributing](#contributing)
13. [Useful links](#useful-links)
14. [License](#license)

## Features

### Common features

- Transactions
- Support `TRUNCATE` by deparsing into `DELETE` statement without `WHERE` clause
- Allow control over whether foreign servers keep connections open after transaction completion. This is controlled by `keep_connections` and defaults to on
- Support list cached connections to foreign servers by using function `duckdb_fdw_get_connections()`
- Support discard cached connections to foreign servers by using function `duckdb_fdw_disconnect()`, `duckdb_fdw_disconnect_all()`.
- Support Bulk `INSERT` by using `batch_size` option
- Support `INSERT`/`UPDATE` with generated column

### Pushdowning

- WHERE clauses with operators, functions and operators
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- GROUP BY clauses
- ORDER BY clauses
- JOIN operations (inner, left, right, full)
- LIMIT/OFFSET clauses

### Notes about pushdowning

- Pushdown is controlled by DuckDB's query optimizer through the SQLite compatibility layer
- Complex queries may be partially pushed down depending on DuckDB's capabilities
- Some PostgreSQL-specific functions may not push down and will be executed locally

## Supported platforms

`duckdb_fdw` was developed on macOS and tested on Linux, so it should run on any reasonably POSIX-compliant system.

## Installation

### Package installation

There's a `duckdb_fdw` rpm available on Pigsty's PGSQL [yum repository](https://repo.pigsty.cc/repo) for el8 and el9

### Source installation

Prerequisites:

- `postgresql-server-{version}-dev`
- `gcc`
- `make`
- `curl` (for download script)

#### 1. Download source

```BASH
git clone https://github.com/alitrack/duckdb_fdw
cd duckdb_fdw
```

#### 2. Download DuckDB library using automated script

The project includes an automated download script that fetches the latest DuckDB version:

```bash
# Download latest DuckDB library (automatically detects platform)
./download_libduckdb.sh

# Or download specific version manually
DUCKDB_VERSION=1.4.1
wget -c https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-amd64.zip
unzip -d . libduckdb-linux-amd64.zip
```

**Note**: The download script automatically:

- Detects your platform (Linux/macOS) and architecture
- Downloads the appropriate DuckDB library

For Enterprise Linux (RHEL/CentOS 8/9), you may need to compile from source using `libduckdb-src.zip` due to glibc version compatibility.

#### 3. Build and install duckdb_fdw

Add a directory of `pg_config` to PATH and build and install `duckdb_fdw`.

```sh
# The .duckdb_version file is automatically created by the download script
make USE_PGXS=1
make install USE_PGXS=1
```

If you want to build `duckdb_fdw` in a source tree of PostgreSQL, use

```sh
make
make install
```

## Usage

## CREATE SERVER options

`duckdb_fdw` accepts the following options via the `CREATE SERVER` command:

- **database** as *string*, **required**

  DuckDB database path.
- **truncatable** as *boolean*, optional, default *false*

  Allows foreign tables to be truncated using the `TRUNCATE` command.
- **keep_connections** as *boolean*, optional, default *false*

  Allows to keep connections to DuckDB while there is no SQL operations between PostgreSQL and DuckDB.
- **batch_size** as *integer*, optional, default *1*

  Specifies the number of rows which should be inserted in a single `INSERT` operation. This setting can be overridden for individual tables.
- **temp_directory** as *string*,  optional, default *NULL*

  Specifies the directory to which to write temp files.

## CREATE USER MAPPING options

There is no user or password conceptions in DuckDB, hence `duckdb_fdw` no need any `CREATE USER MAPPING` command.

In OS `duckdb_fdw` works as executed code with permissions of user of PostgreSQL server. Usually it is `postgres` OS user. For interacting with DuckDB database without access errors ensure this user have permissions on DuckDB file and, sometimes, directory of the file.

- read permission on all directories by path to the DuckDB database file;
- read permission on DuckDB database file;

## CREATE FOREIGN TABLE options

`duckdb_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command:

- **table** as *string*, optional, no default

  DuckDB table name. Use if not equal to name of foreign table in PostgreSQL. Also see about [identifier case handling](#identifier-case-handling).
- **truncatable** as *boolean*, optional, default from the same `CREATE SERVER` option

  See `CREATE SERVER` options section for details.
- **batch_size** as *integer*, optional, default from the same `CREATE SERVER` option

  See `CREATE SERVER` options section for details.

`duckdb_fdw` accepts the following column-level options via the
`CREATE FOREIGN TABLE` command:

- **column_name** as *string*, optional, no default

  This option gives the column name to use for the column on the remote server. Also see about [identifier case handling](#identifier-case-handling).
- **column_type** as *string*, optional, no default

  Option to convert INT DuckDB column (epoch Unix Time) to be treated/visualized as TIMESTAMP in PostgreSQL.
- **key** as *boolean*, optional, default *false*

  Indicates a column as a part of primary key or unique key of DuckDB table.

## IMPORT FOREIGN SCHEMA options

`duckdb_fdw` supports [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html)
(PostgreSQL 9.5+) and accepts no custom options for this command.

## TRUNCATE support

`duckdb_fdw` implements the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14.

As SQlite does not provide a `TRUNCATE` command, it is simulated with a
simple unqualified `DELETE` operation.

`TRUNCATE ... CASCADE` support *not described*.

## Functions

As well as the standard `duckdb_fdw_handler()` and `duckdb_fdw_validator()`
functions, `duckdb_fdw` provides the following user-callable utility functions:

- SETOF record **duckdb_fdw_get_connections**(server_name text, valid bool)
- bool **duckdb_fdw_disconnect**(text)

  Closes connection from PostgreSQL to DuckDB in the current session.
- bool **duckdb_fdw_disconnect_all()**
- **duckdb_fdw_version()**;

  Returns standard "version integer" as `major version * 10000 + minor version * 100 + bugfix`.

```
duckdb_fdw_version
--------------------
              10000  
```

### DuckDB_execute

```sql
FUNCTION duckdb_execute(server name, stmt text) RETURNS void
```

This function can be used to execute arbitrary SQL statements on the remote DuckDB server. That will only work with statements that do not return results (typically DDL statements).

Be careful when using this function, since it might disturb the transaction management of duckdb_fdw. Remember that running a DDL statement in DuckDB will issue an implicit COMMIT.
You are best advised to use this function outside multi-statement transactions.

It is very useful to use command that duckdb_fdw does not support, for example,

- add more table or view to DuckDB directly.

```sql
SELECT duckdb_execute('duckdb_server'
,'create or replace view iris_parquet  as select * from parquet_scan(''temp/iris.parquet'');');

create foreign TABLE duckdb.iris_parquet(
"Sepal.Length" float,  
"Sepal.Width" float,
"Petal.Length" float,
"Petal.Width" float,  
"Species" text)
      SERVER duckdb_server OPTIONS (table 'iris_parquet');

-- or an easy way

IMPORT FOREIGN SCHEMA public limit to (iris_parquet) FROM SERVER  
duckdb_server INTO duckdb;
```

- run Copy command on Foreign table

```sql
SELECT duckdb_execute('duckdb_server'
,'CREATE TABLE test (a INTEGER, b INTEGER, c VARCHAR(10));
');
SELECT duckdb_execute('duckdb_server'  
,'COPY test FROM ''/tmp/test.csv'';');
```

## Identifier case handling

PostgreSQL folds identifiers to lower case by default. DuckDB preserves case sensitivity through the SQLite compatibility layer. It's important
to be aware of potential issues with table and column names.

## Generated columns

DuckDB provides support for [generated columns](https://www.duckdb.org/gencol.html).
Behaviour of `duckdb_fdw` with these columns _isn't yet described_.

Note that while `duckdb_fdw` will `INSERT` or `UPDATE` the generated column value
in DuckDB, there is nothing to stop the value being modified within DuckDB,
and hence no guarantee that in subsequent `SELECT` operations the column will
still contain the expected generated value. This limitation also applies to
`postgres_fdw`.

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)

## Character set handling

**Yet not described**

## Examples

### Install the extension

Once for a database you need, as PostgreSQL superuser.

```sql
CREATE EXTENSION duckdb_fdw;
```

### Create a foreign server with appropriate configuration:

Once for a foreign datasource you need, as PostgreSQL superuser. Please specify DuckDB database path using `database` option.

```sql
CREATE SERVER duckdb_server
FOREIGN DATA WRAPPER duckdb_fdw
OPTIONS (
    database '/path/to/database'
);
```

### Grant usage on foreign server to normal user in PostgreSQL:

Once for a normal user (non-superuser) in PostgreSQL, as PostgreSQL superuser. It is a good idea to use a superuser only where really necessary, so let's allow a normal user to use the foreign server (this is not required for the example to work, but it's secirity recomedation).

```sql
GRANT USAGE ON FOREIGN SERVER duckdb_server TO pguser;
```

Where `pguser` is a sample user for works with foreign server (and foreign tables).

### User mapping

There is no user or password conceptions in DuckDB, hence `duckdb_fdw` no need any `CREATE USER MAPPING` command. About access problems see in [CREATE USER MAPPING options](#create-user-mapping-options).

### Create foreign table

All `CREATE FOREIGN TABLE` SQL commands can be executed as a normal PostgreSQL user if there were correct `GRANT USAGE ON FOREIGN SERVER`. No need PostgreSQL supersuer for secirity reasons but also works with PostgreSQL supersuer.

Please specify `table` option if DuckDB table name is different from foreign table name.

```sql
	CREATE FOREIGN TABLE t1 (
	  a integer,
	  b text
	)
	SERVER duckdb_server
	OPTIONS (
	  table 't1_duckdb'
	);
```

If you want to update tables, please add `OPTIONS (key 'true')` to a primary key or unique key like the following:

```sql
	CREATE FOREIGN TABLE t1(
	  a integer OPTIONS (key 'true'),
	  b text
	)
	SERVER duckdb_server 
	OPTIONS (
	  table 't1_duckdb'
	);
```

If you need to convert INT DuckDB column (epoch Unix Time) to be treated/visualized as `TIMESTAMP` in PostgreSQL, please add `OPTIONS (column_type 'INT')` when defining FOREIGN table at PostgreSQL like the following:

```sql
	CREATE FOREIGN TABLE t1(
	  a integer,
	  b text,
	  c timestamp without time zone OPTIONS (column_type 'INT')
	)
	SERVER duckdb_server
	OPTIONS (
	  table 't1_duckdb'
	);
```

As above, but with aliased column names:

```sql
	CREATE FOREIGN TABLE t1(
	  a integer,
	  b text OPTIONS (column_name 'test_id'),
	  c timestamp without time zone OPTIONS (column_type 'INT', column_name 'unixtime')
	)
	SERVER duckdb_server
	OPTIONS (
	  table 't1_duckdb'
	);
```

### Import a DuckDB database as schema to PostgreSQL:

```sql
	IMPORT FOREIGN SCHEMA someschema
	FROM SERVER duckdb_server
	INTO public;
```

Note: `someschema` has no particular meaning and can be set to an arbitrary value.

### Access foreign table

For the table from previous examples

```sql
	SELECT * FROM t1;
```

## Limitations

- `INSERT` into a partitioned table which has foreign partitions is not supported. Error `Not support partition insert` will display.
- `TRUNCATE` in `duckdb_fdw` always delete data of both parent and child tables (no matter user inputs `TRUNCATE table CASCADE` or `TRUNCATE table RESTRICT`) if there are foreign-keys references with `ON DELETE CASCADE` clause.
- `RETURNING` is not supported.

## Tests

All tests are based on `make check`, main testing script see in [test.sh](test.sh) file. We don't profess a specific environment. You can use any POSIX-compliant system.
Testing scripts from PosgreSQL-side is multi-versioned. Hence, you need install PostgreSQL packages in versions listed in [sql](sql) directory.
PostgreSQL server locale for messages in tests must be *english*. About base testing mechanism see in [PostgreSQL documentation](https://www.postgresql.org/docs/current/regress-run.html).

Testing directory have structure as following:

```
+---sql
    +---10.18
    |       filename1.sql
    |       filename2.sql
    | 
    +---11.13
    |       filename1.sql
    |       filename2.sql
    | 
.................  
    \---14.0
           filename1.sql
           filename2.sql
```

The test cases for each version are based on the test of corresponding version of PostgreSQL.
You can execute test by `test.sh` directly.
The version of PostgreSQL is detected automatically by `$(VERSION)` variable in Makefile.

## Contributing

Opening issues and pull requests on GitHub are welcome.

You don't need to squash small commits to one big in pull requests.

For pull request, please make sure these items below for testing:

- Create test cases (if needed) for the latest version of PostgreSQL supported by `duckdb_fdw`.
- Execute test cases and update expectations for the latest version of PostgreSQL
- Test creation and execution for other PostgreSQL versions are welcome but not required.

## Useful links

### Source

- https://github.com/alitrack/duckdb_fdw
- https://pgxn.org/dist/duckdb_fdw/

Reference FDW realisation, `postgres_fdw`

- https://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=contrib/postgres_fdw;hb=HEAD

### General FDW Documentation

- https://www.postgresql.org/docs/current/ddl-foreign-data.html
- https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
- https://www.postgresql.org/docs/current/sql-createforeigntable.html
- https://www.postgresql.org/docs/current/sql-importforeignschema.html
- https://www.postgresql.org/docs/current/fdwhandler.html
- https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

- https://wiki.postgresql.org/wiki/Fdw
- https://pgxn.org/tag/fdw/

## Special thanks

Authors of https://github.com/pgspider/sqlite_fdw (base implementation)

## License

[MIT License](LICENSE)
