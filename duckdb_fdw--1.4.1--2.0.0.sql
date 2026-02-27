/* duckdb_fdw--1.4.1--2.0.0.sql */

-- Ensure signature changes are handled safely across upgrades.
DROP FUNCTION IF EXISTS duckdb_fdw_version();
CREATE FUNCTION duckdb_fdw_version()
  RETURNS text STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

DROP FUNCTION IF EXISTS duckdb_execute(server name, statement text);
CREATE FUNCTION duckdb_execute(server name, statement text)
  RETURNS void STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE FUNCTION IF NOT EXISTS duckdb_create_s3_secret(server name, secret_name text, key_id text, secret text, region text DEFAULT NULL)
  RETURNS void
  AS 'MODULE_PATHNAME' LANGUAGE C;

DROP FUNCTION IF EXISTS duckdb_fdw_get_connections();
DROP FUNCTION IF EXISTS duckdb_fdw_disconnect(text);
DROP FUNCTION IF EXISTS duckdb_fdw_disconnect_all();
