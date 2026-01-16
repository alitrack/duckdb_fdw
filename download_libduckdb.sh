#!/bin/bash

# Function to get latest release version
get_latest_version() {
    curl --silent "https://api.github.com/repos/duckdb/duckdb/releases/latest" | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/'
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
        *)
            echo "Unsupported operating system: $OS"
            exit 1
            ;;
    esac
}

# Get system information
get_system_info

# Get latest version
VERSION=$(get_latest_version)
# Remove 'v' prefix from version for filename
DUCKDB_VERSION=${VERSION#v}



# Construct download URL
DOWNLOAD_URL="https://github.com/duckdb/duckdb/releases/download/${VERSION}/libduckdb-${PLATFORM}-${ARCH}.zip"

echo "Downloading DuckDB ${VERSION} for ${PLATFORM}-${ARCH}..."
echo "URL: ${DOWNLOAD_URL}"

# Download and extract
curl -L -o duckdb-temp.zip "${DOWNLOAD_URL}"
unzip -o duckdb-temp.zip

rm duckdb-temp.zip

