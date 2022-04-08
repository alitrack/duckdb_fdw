rm -rf /tmp/duckdbfdw_test*.db
rm -rf /tmp/*.data
rm -rf /tmp/duckdbfdw_test*.db
cp -a sql/init_data/*.data /tmp/

duckdb /tmp/duckdbfdw_test_post.db < sql/init_data/init_post.sql
duckdb /tmp/duckdbfdw_test_core.db < sql/init_data/init_core.sql
duckdb /tmp/duckdbfdw_test.db < sql/init_data/init.sql
duckdb /tmp/duckdbfdw_test_selectfunc.db < sql/init_data/init_selectfunc.sql

sed -i 's/REGRESS =.*/REGRESS = extra\/duckdb_fdw_post extra\/float4 extra\/float8 extra\/int4 extra\/int8 extra\/numeric extra\/join extra\/limit extra\/aggregates extra\/prepare extra\/select_having extra\/select extra\/insert extra\/update extra\/timestamp duckdb_fdw type aggregate selectfunc /' Makefile

make clean
make
make check | tee make_check.out
