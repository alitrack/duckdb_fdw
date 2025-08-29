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

# Check if DUCKDB_VERSION is defined
ifeq ($(DUCKDB_VERSION),)
    # Try to read from .duckdb_version file
    -include .duckdb_version

    # If still not defined, try to get from environment
    ifndef DUCKDB_VERSION
        $(error DUCKDB_VERSION is not set. Please set it using: make DUCKDB_VERSION=x.x.x or create a .duckdb_version file)
    endif
endif

MODULE_big = duckdb_fdw
OBJS = connection.o option.o deparse.o sqlite_query.o duckdb_fdw.o sqlite3_api_wrapper.o

EXTENSION = duckdb_fdw
DATA = duckdb_fdw--1.0.0.sql duckdb_fdw--1.0.0--1.1.2.sql duckdb_fdw--1.1.2--1.1.3.sql duckdb_fdw--1.1.3--1.3.2.sql

REGRESS = extra/duckdb_fdw_post extra/float4 extra/float8 extra/int4 extra/int8 extra/numeric extra/join extra/limit extra/aggregates extra/prepare extra/select_having extra/select extra/insert extra/update extra/timestamp duckdb_fdw type aggregate selectfunc 

ifeq '$(findstring ;,$(PATH))' ';'
    detected_OS := Windows
else
    detected_OS := $(shell uname 2>/dev/null || echo Unknown)
    detected_OS := $(patsubst CYGWIN%,Cygwin,$(detected_OS))
    detected_OS := $(patsubst MSYS%,MSYS,$(detected_OS))
    detected_OS := $(patsubst MINGW%,MSYS,$(detected_OS))
endif
ifeq ($(detected_OS),Windows)
    DLSUFFIX = .dll
endif
ifeq ($(detected_OS),Darwin)        # Mac OS X
    DLSUFFIX = .dylib
    PG_CXXFLAGS = -std=c++11

endif
ifeq ($(detected_OS),Linux)
    # DLSUFFIX = .so
    PG_CXXFLAGS = -std=c++11
    detected_arch := $(shell uname -m)
    ifeq ($(detected_arch),x86_64)
        PG_CXXFLAGS = -std=c++11 -D_GLIBCXX_USE_CXX11_ABI=0
    endif
endif

SHLIB_LINK := -L. -lduckdb.$(DUCKDB_VERSION) -lstdc++

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
VERSION := $(shell $(PG_CONFIG) --version)
include $(PGXS)
ifndef MAJORVERSION
MAJORVERSION := $(basename $(VERSION))
endif
ifeq (,$(findstring $(MAJORVERSION), 10 11 12 13 14 15 16 17))
$(error PostgreSQL 10, 11, 12, 13, 14, 15, 16 or 17 is required to compile this extension)
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


install-duckdb:  $(shlib)
	$(install_bin) -m 755 libduckdb.$(DUCKDB_VERSION)$(DLSUFFIX) $(DESTDIR)$(PG_LIB)

install: install-duckdb

uninstall-duckdb:
	rm -f $(DESTDIR)$(PG_LIB)/libduckdb.$(DUCKDB_VERSION)$(DLSUFFIX)

uninstall: uninstall-duckdb