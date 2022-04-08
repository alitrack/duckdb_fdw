/* contrib/duckdb_fdw/duckdb_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION duckdb_fdw" to load this file. \quit

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

CREATE OR REPLACE FUNCTION duckdb_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OR REPLACE FUNCTION duckdb_execute(server name, statement text)
RETURNS void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;

COMMENT ON FUNCTION duckdb_execute(name, text)
IS 'executes an arbitrary SQL statement  return no results on the DuckDB';