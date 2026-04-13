/* duckdb_fdw--2.0.0--2.0.1.sql */

CREATE FUNCTION duckdb_fdw_runtime_compatibility_status()
  RETURNS text
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION duckdb_fdw_runtime_fingerprint()
  RETURNS jsonb
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION duckdb_fdw_preflight()
  RETURNS jsonb
  AS 'MODULE_PATHNAME' LANGUAGE C;

SELECT duckdb_fdw_preflight();
