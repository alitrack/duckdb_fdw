#!/bin/bash
#
# download_libduckdb.sh — fetch the prebuilt libduckdb matching this platform.
#
# Fail hard on any error: the previous "curl + unzip and hope" behaviour let
# a 404 (e.g. a wrong asset name) download GitHub's HTML error page, which
# only exploded later at `make` time with "duckdb.h: No such file or
# directory".  set -euo pipefail + curl --fail + a post-download check make
# the failure point the download step itself.

set -euo pipefail

DEFAULT_DUCKDB_VERSION="1.5.1"

normalize_version_tag() {
    case "$1" in
        v*) echo "$1" ;;
        *) echo "v$1" ;;
    esac
}

# Function to get system info
get_system_info() {
    OS=$(uname -s)
    ARCH=$(uname -m)

    case "$OS" in
        "Darwin")
            PLATFORM="osx"
            # For macOS, we'll use universal build
            ARCH="universal"
            LIB_EXT="dylib"
            ;;
        "Linux")
            PLATFORM="linux"
            case "$ARCH" in
                "x86_64")
                    ARCH="amd64"
                    ;;
                "aarch64"|"arm64")
                    # DuckDB names the Linux ARM64 asset "arm64" (NOT
                    # "aarch64"): .../libduckdb-linux-arm64.zip.  The
                    # aarch64 name 404s and silently breaks the build on
                    # ubuntu-*-arm runners.
                    ARCH="arm64"
                    ;;
            esac
            LIB_EXT="so"
            ;;
        MINGW*|CYGWIN*|MSYS*)
            PLATFORM="windows"
            ARCH="amd64"
            LIB_EXT="dll"
            ;;
        *)
            echo "Unsupported operating system: $OS" >&2
            exit 1
            ;;
    esac
}

# Get system information
get_system_info

# Resolve requested version
REQUESTED_VERSION=${DUCKDB_VERSION:-$DEFAULT_DUCKDB_VERSION}
VERSION=$(normalize_version_tag "$REQUESTED_VERSION")

# Construct download URL
DOWNLOAD_URL="https://github.com/duckdb/duckdb/releases/download/${VERSION}/libduckdb-${PLATFORM}-${ARCH}.zip"

echo "Downloading DuckDB ${VERSION} for ${PLATFORM}-${ARCH}..."
echo "URL: ${DOWNLOAD_URL}"

# Download and extract.
# --fail: return an error on HTTP 4xx/5xx (GitHub redirects a 404 asset to an
# HTML error page; without --fail, curl exits 0 and we unzip HTML).
curl --fail -L --retry 3 -o duckdb-temp.zip "${DOWNLOAD_URL}"

unzip -o duckdb-temp.zip

rm -f duckdb-temp.zip

# Verify the payload actually landed: the zip must unpack duckdb.h plus the
# platform library.  Anything else (e.g. an HTML 404 page that slipped past)
# fails here instead of at `make` time.
if [ ! -f duckdb.h ]; then
    echo "ERROR: download did not produce duckdb.h — is ${DOWNLOAD_URL} valid?" >&2
    echo "       (404 assets yield an HTML page; check the asset name for ${PLATFORM}-${ARCH})" >&2
    exit 1
fi
case "${PLATFORM}-${ARCH}" in
    osx-universal)
        [ -f libduckdb.dylib ] || { echo "ERROR: libduckdb.dylib missing after download" >&2; exit 1; }
        ;;
    *)
        [ -f "libduckdb.${LIB_EXT}" ] || { echo "ERROR: libduckdb.${LIB_EXT} missing after download" >&2; exit 1; }
        ;;
esac

echo "Downloaded DuckDB ${VERSION}: duckdb.h + libduckdb.${LIB_EXT}"
