# Use an official PostgreSQL image as the base image
ARG POSTGRES_VERSION=16
FROM postgres:${POSTGRES_VERSION} AS builder

# Set default values for build arguments
ARG POSTGRES_VERSION=16
ARG DUCKDB_VERSION=1.0.0

# Install build dependencies
RUN apt-get update && apt-get install -y \
  git \
  build-essential \
  cmake \
  postgresql-server-dev-${POSTGRES_VERSION} \
  postgresql-client-${POSTGRES_VERSION} \
  wget \
  unzip

# add local checkout
ADD . duckdb_fdw

# build fdw
RUN cd duckdb_fdw \
   && export DUCK_ARCH=$(uname -m | sed -e s/arm64/aarch64/ | sed -e s/x86_64/amd64/) \
   && wget -c https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/libduckdb-linux-${DUCK_ARCH}.zip \
   && unzip -o -d . libduckdb-linux-${DUCK_ARCH}.zip \
   && cp libduckdb.so $(pg_config --libdir) \
   && make USE_PGXS=1 \
   && make install USE_PGXS=1

# Set environment variables
ENV POSTGRES_HOST_AUTH_METHOD='trust'


# Switch to the postgres user
USER postgres

# Optionally, you might want to include additional configurations or initialization steps here

# Create the final image
FROM postgres:${POSTGRES_VERSION}
ARG POSTGRES_VERSION=16

# Copy duckdb_fdw artifacts from the builder stage
COPY --from=builder duckdb_fdw/duckdb_fdw.so /usr/lib/postgresql/${POSTGRES_VERSION}/lib/
COPY --from=builder duckdb_fdw/duckdb_fdw.control /usr/share/postgresql/${POSTGRES_VERSION}/extension/
COPY --from=builder duckdb_fdw/duckdb_fdw*.sql /usr/share/postgresql/${POSTGRES_VERSION}/extension/

# Horrible workaround for Docker's completely brain damaged multiplatform builds
RUN mkdir /usr/lib/platform
COPY --from=builder /usr/lib/*-linux-gnu/libduckdb.so /usr/lib/platform/
RUN ln -sf /usr/lib/platform/libduckdb.so /usr/lib/$(uname -m | sed -e s/arm64/aarch64/)-linux-gnu/libduckdb.so
