-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION duckdb_fdw UPDATE TO '1.3.2'" to load this file. \quit

-- Update for DuckDB 1.3.2 library support
-- This version update primarily adds support for the newer DuckDB library version
-- No structural changes needed, just version bump for library compatibility