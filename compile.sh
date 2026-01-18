#!/bin/bash
set -e
PG_INC="/usr/include/postgresql/15/server"
gcc -fPIC -I. -I$PG_INC -c connection.c -o connection.o
gcc -fPIC -I. -I$PG_INC -c option.c -o option.o
gcc -fPIC -I. -I$PG_INC -c deparse.c -o deparse.o
gcc -fPIC -I. -I$PG_INC -c duckdb_optimization.c -o duckdb_optimization.o
gcc -fPIC -I. -I$PG_INC -c nanoarrow.c -o nanoarrow.o
gcc -fPIC -I. -I$PG_INC -c import.c -o import.o
gcc -fPIC -I. -I$PG_INC -c duckdb_fdw.c -o duckdb_fdw.o

gcc -shared -o duckdb_fdw.so duckdb_fdw.o connection.o option.o deparse.o duckdb_optimization.o nanoarrow.o import.o -L. -lduckdb -Wl,-rpath,'$ORIGIN'
echo "Build complete: duckdb_fdw.so"
