DUCKDB_VERSION=1.1.2
PG_VERSION=17
export PATH=/Applications/Postgres.app/Contents/Versions/${PG_VERSION}/bin:$PATH

# clone duckdb_fdw
git clone https://github.com/alitrack/duckdb_fdw
cd duckdb_fdw

# download libduckdb
wget -c https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-osx-universal.zip
unzip -d . libduckdb-osx-universal.zip
cp libduckdb.dylib $(pg_config --libdir)

# compile
make clean USE_PGXS=1
make USE_PGXS=1

# just for macOS
install_name_tool -change @rpath/libduckdb.dylib $(pg_config --libdir) libduckdb.dylib duckdb_fdw.dylib

sudo make install USE_PGXS=1