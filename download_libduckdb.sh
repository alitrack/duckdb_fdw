#!/bin/bash

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
                    ARCH="aarch64"
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
            echo "Unsupported operating system: $OS"
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

# Download and extract
curl -L -o duckdb-temp.zip "${DOWNLOAD_URL}"
unzip -o duckdb-temp.zip

rm duckdb-temp.zip
