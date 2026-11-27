-- Verify that the supported upgrade chain can start at the historical 1.0.0 version.
CREATE EXTENSION duckdb_fdw VERSION '1.0.0';

SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '1.1.2';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '1.1.3';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '1.3.2';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '1.4.1';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '2.0.0';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '2.0.1';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

ALTER EXTENSION duckdb_fdw UPDATE TO '2.0.2';
SELECT extversion FROM pg_extension WHERE extname = 'duckdb_fdw';

SELECT duckdb_fdw_version() IS NOT NULL AS has_version;

DROP EXTENSION duckdb_fdw;
