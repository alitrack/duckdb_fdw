/* duckdb_fdw--2.0.1--2.0.2.sql */

-- No SQL-level changes between 2.0.1 and 2.0.2: the 2.0.2 release ships a
-- rebuilt module (DuckDB 2.0.x runtime support in the version guard, 2.0
-- release-asset naming in the download helpers, 2.0 CI target).  The
-- function set is unchanged, so upgrading is a no-op at the SQL layer;
-- the new duckdb_fdw.so is picked up on the next backend reload.
