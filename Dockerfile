# 第一阶段：编译构建
ARG POSTGRES_VERSION=17
FROM postgres:${POSTGRES_VERSION} AS builder

LABEL maintainer="duckdb_fdw team"

# 安装构建依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-${POSTGRES_VERSION} \
    curl \
    unzip \
    wget \
    git \
    libstdc++-12-dev

WORKDIR /build

# 复制源码
COPY . .

# 下载 DuckDB 原生内核 (v1.4.3+)
RUN bash ./download_libduckdb.sh

# 编译 duckdb_fdw 2.0 (原生版)
RUN make USE_PGXS=1 && \
    make install USE_PGXS=1

# 第二阶段：运行环境
FROM postgres:${POSTGRES_VERSION}
ARG POSTGRES_VERSION=17

# 安装运行时库 (libstdc++)
RUN apt-get update && apt-get install -y libstdc++6 && rm -rf /var/lib/apt/lists/*

# 从编译阶段拷贝插件和依赖库
COPY --from=builder /usr/lib/postgresql/${POSTGRES_VERSION}/lib/duckdb_fdw.so /usr/lib/postgresql/${POSTGRES_VERSION}/lib/
COPY --from=builder /usr/share/postgresql/${POSTGRES_VERSION}/extension/duckdb_fdw* /usr/share/postgresql/${POSTGRES_VERSION}/extension/
COPY --from=builder /build/libduckdb.so /usr/local/lib/

# 刷新动态库缓存
RUN ldconfig

# 设置环境变量，方便测试
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=postgres

# 预创建挂载目录
RUN mkdir -p /var/lib/postgresql/duckdb && chown postgres:postgres /var/lib/postgresql/duckdb

USER postgres