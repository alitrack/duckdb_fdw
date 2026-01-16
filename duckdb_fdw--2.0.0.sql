/* duckdb_fdw--2.0.0.sql */

CREATE FUNCTION duckdb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION duckdb_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER duckdb_fdw
  HANDLER duckdb_fdw_handler
  VALIDATOR duckdb_fdw_validator;

CREATE FUNCTION duckdb_fdw_version()
  RETURNS text STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION duckdb_execute(server name, statement text)
RETURNS void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION duckdb_create_s3_secret(server name, secret_name text, key_id text, secret text, region text DEFAULT NULL)
RETURNS void
AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION duckdb_execute(name, text)
IS 'executes an arbitrary SQL statement on DuckDB';
