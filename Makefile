######################################################################-------------------------------------------------------------------------
#
# DuckDB Foreign Data Wrapper for PostgreSQL
#
# Portions Copyright (c) 2018, TOSHIBA CORPORATION
#
# IDENTIFICATION
# 		Makefile
#
##########################################################################

MODULE_big = duckdb_fdw
OBJS = connection.o option.o deparse.o sqlite_query.o duckdb_fdw.o

EXTENSION = duckdb_fdw
DATA = duckdb_fdw--1.0.sql

REGRESS = extra/duckdb_fdw_post extra/float4 extra/float8 extra/int4 extra/int8 extra/numeric extra/join extra/limit extra/aggregates extra/prepare extra/select_having extra/select extra/insert extra/update extra/timestamp duckdb_fdw type aggregate 

SQLITE_LIB = sqlite3_api_wrapper

UNAME = uname
OS := $(shell $(UNAME))
ifeq ($(OS), Darwin)
DLSUFFIX = .dylib
else
DLSUFFIX = .so
endif

# SHLIB_LINK := -lsqlite3_api_wrapper -Wl,-undefined,dynamic_lookup
SHLIB_LINK := -lsqlite3_api_wrapper

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
ifeq (,$(findstring $(MAJORVERSION),9.6 10 11 12 13))
$(error PostgreSQL  9.6, 10, 11, 12 or 13 is required to compile this extension)
endif

else
subdir = contrib/duckdb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

