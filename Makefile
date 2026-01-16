#-------------------------------------------------------------------------
#
# DuckDB Foreign Data Wrapper for PostgreSQL
#
# IDENTIFICATION
# 		Makefile
#
#--------------------------------------------------------------------------

MODULE_big = duckdb_fdw
OBJS = connection.o option.o deparse.o duckdb_fdw.o duckdb_optimization.o

EXTENSION = duckdb_fdw
DATA = $(wildcard duckdb_fdw--*.sql)

# 默认回归测试
REGRESS = duckdb_fdw type aggregate selectfunc 

# 操作系统检测
ifeq '$(findstring ;,$(PATH))' ';'
    detected_OS := Windows
else
    detected_OS := $(shell uname 2>/dev/null || echo Unknown)
endif

# 编译选项
PG_CPPFLAGS = -I.
SHLIB_LINK = -L. -lduckdb -lstdc++

ifeq ($(detected_OS),Darwin)
    DLSUFFIX = .dylib
    PG_CXXFLAGS = -std=c++11
endif

ifeq ($(detected_OS),Linux)
    DLSUFFIX = .so
    PG_CXXFLAGS = -std=c++11
    # 针对现代环境的 ABI 兼容性设置
    PG_CPPFLAGS += -D_GLIBCXX_USE_CXX11_ABI=0
endif

# 使用 PGXS 基础设施
ifdef USE_PGXS
    PG_CONFIG = pg_config
    PGXS := $(shell $(PG_CONFIG) --pgxs)
    PG_LIB := $(shell $(PG_CONFIG) --pkglibdir)
    include $(PGXS)
else
    subdir = contrib/duckdb_fdw
    top_builddir = ../..
    include $(top_builddir)/src/Makefile.global
    include $(top_srcdir)/contrib/contrib-global.mk
endif

# 设置 RPATH，确保运行时能找到同目录下的 libduckdb.so
SHLIB_LINK += -Wl,-rpath,'$$ORIGIN' -Wl,-rpath,$(PG_LIB)

# 安装钩子
install-duckdb:
	$(install_bin) -m 755 libduckdb$(DLSUFFIX) $(DESTDIR)$(PG_LIB)

install: install-duckdb

.PHONY: install-duckdb
