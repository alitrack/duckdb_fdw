######################################################################-------------------------------------------------------------------------
#
# DuckDB Foreign Data Wrapper for PostgreSQL
#
# Portions Copyright (c) 2021, TOSHIBA CORPORATION
#
# IDENTIFICATION
# 		Makefile
#
##########################################################################

MODULE_big = duckdb_fdw
OBJS = connection.o option.o deparse.o sqlite_query.o duckdb_fdw.o sqlite3_api_wrapper.o

EXTENSION = duckdb_fdw
DATA = duckdb_fdw--1.0.sql duckdb_fdw--1.0--1.1.sql

REGRESS = extra/duckdb_fdw_post extra/float4 extra/float8 extra/int4 extra/int8 extra/numeric extra/join extra/limit extra/aggregates extra/prepare extra/select_having extra/select extra/insert extra/update extra/timestamp duckdb_fdw type aggregate selectfunc 

SQLITE_LIB = duckdb

ifeq '$(findstring ;,$(PATH))' ';'
    detected_OS := Windows
else
    detected_OS := $(shell uname 2>/dev/null || echo Unknown)
    detected_OS := $(patsubst CYGWIN%,Cygwin,$(detected_OS))
    detected_OS := $(patsubst MSYS%,MSYS,$(detected_OS))
    detected_OS := $(patsubst MINGW%,MSYS,$(detected_OS))
endif
ifeq ($(detected_OS),Windows)
    # DLSUFFIX = .dll
endif
ifeq ($(detected_OS),Darwin)        # Mac OS X
    # DLSUFFIX = .dylib
	PG_CPPFLAGS = -std=c++11
endif
ifeq ($(detected_OS),Linux)
    # DLSUFFIX = .so
endif

SHLIB_LINK := -lduckdb -lstdc++

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
VERSION := $(shell $(PG_CONFIG) --version)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
ifeq (,$(findstring $(MAJORVERSION), 10 11 12 13 14 15 16))
$(error PostgreSQL 10, 11, 12, 13, 14, 15 or 16 is required to compile this extension)
endif

else
subdir = contrib/duckdb_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

ifdef REGRESS_PREFIX
REGRESS_PREFIX_SUB = $(REGRESS_PREFIX)
else
REGRESS_PREFIX_SUB = $(VERSION)
endif

REGRESS := $(addprefix $(REGRESS_PREFIX_SUB)/,$(REGRESS))
$(shell mkdir -p results/$(REGRESS_PREFIX_SUB)/extra)
