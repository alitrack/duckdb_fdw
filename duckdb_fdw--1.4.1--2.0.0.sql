/* duckdb_fdw--1.4.1--2.0.0.sql */

-- 2.0.0 重构：确保函数定义是最新的
-- 虽然之前的版本可能已有部分函数，但我们在这里进行幂等更新

CREATE OR REPLACE FUNCTION duckdb_fdw_version()
  RETURNS text STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE OR REPLACE FUNCTION duckdb_execute(server name, statement text)
RETURNS void STRICT
AS 'MODULE_PATHNAME' LANGUAGE C;
