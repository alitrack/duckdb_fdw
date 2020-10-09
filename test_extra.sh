rm -rf /tmp/sqlitefdw_test*.db
rm -rf /tmp/*.data
cp -a sql/extra/*.data /tmp/

sqlite3 /tmp/sqlitefdw_test_post.db < sql/extra/init_post.sql
sqlite3 /tmp/sqlitefdw_test_core.db < sql/extra/init_core.sql

export USE_PGXS=1
sed -i 's/REGRESS =.*/REGRESS = extra\/duckdb_fdw_post extra\/float4 extra\/float8 extra\/int4 extra\/int8 extra\/numeric extra\/join extra\/limit extra\/aggregates extra\/prepare extra\/select_having extra\/select extra\/insert extra\/update extra\/timestamp /' Makefile

make clean
make
mkdir -p results/extra || true
make check | tee make_check.out
