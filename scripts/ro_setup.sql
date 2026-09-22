CREATE SERVER IF NOT EXISTS ro_srv FOREIGN DATA WRAPPER duckdb_fdw
  OPTIONS (database '/home/lhy/g72/t.duckdb', force_readonly 'true');
