/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        duckdb_fdw--1.0.sql
 *
 *-------------------------------------------------------------------------
 */

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
