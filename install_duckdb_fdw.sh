#!/usr/bin/env bash

# Exit immediately if any command fails
set -e

# --- Default Parameters ---
# On Linux, target directories are resolved dynamically using pg_config if available.
if command -v pg_config >/dev/null 2>&1; then
  DEFAULT_PG_LIB=$(pg_config --pkglibdir)
  DEFAULT_PG_EXT=$(pg_config --sharedir)/extension
else
  # Standard fallback paths for Debian/Ubuntu packaging structures
  DEFAULT_PG_LIB="/usr/lib/postgresql/18/lib"
  DEFAULT_PG_EXT="/usr/share/postgresql/18/extension"
fi

# Establish context paths relative to current script/working directory
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SOURCE_DIR}/build"

# --- Usage Help ---
usage() {
  echo "Usage: $0 [options]"
  echo "Options:"
  echo "  --pg-lib DIR    Target PostgreSQL library directory (Default: ${DEFAULT_PG_LIB})"
  echo "  --pg-ext DIR    Target PostgreSQL extension SQL directory (Default: ${DEFAULT_PG_EXT})"
  echo "  --source DIR    Repository source code root (Default: ${SOURCE_DIR})"
  echo "  --build DIR     Compiled binaries build root (Default: ${BUILD_DIR})"
  echo "  -h, --help      Display this help menu"
  exit 0
}

# --- Parse Arguments ---
while [[ $# -gt 0 ]]; do
  case $1 in
  --pg-lib)
    DEFAULT_PG_LIB="$2"
    shift 2
    ;;
  --pg-ext)
    DEFAULT_PG_EXT="$2"
    shift 2
    ;;
  --source)
    SOURCE_DIR="$2"
    shift 2
    ;;
  --build)
    BUILD_DIR="$2"
    shift 2
    ;;
  -h | --help) usage ;;
  *)
    echo "Unknown parameter: $1"
    usage
    exit 1
    ;;
  esac
done

# --- Administrative Validation ---
if [ "$EUID" -ne 0 ]; then
  echo "Error: Root privileges required. Please execute this script using 'sudo'."
  exit 1
fi

echo -e "\e[36mStarting deployment of duckdb_fdw for Linux...\e[0m"
echo -e "\e[90mSource Context: ${SOURCE_DIR}\e[0m"
echo -e "\e[90mBuild Context:  ${BUILD_DIR}\e[0m"
echo -e "\e[90mTarget Lib:     ${DEFAULT_PG_LIB}\e[0m"
echo -e "\e[90mTarget Ext:     ${DEFAULT_PG_EXT}\e[0m\n"

# Create destination targets if missing
mkdir -p "${DEFAULT_PG_LIB}" "${DEFAULT_PG_EXT}"

# --- 1. Deploy Compiled Extension Binary (.so) ---
COMPILED_SO="${BUILD_DIR}/duckdb_fdw.so"
if [ -f "${COMPILED_SO}" ]; then
  cp "${COMPILED_SO}" "${DEFAULT_PG_LIB}/"
  chmod 755 "${DEFAULT_PG_LIB}/duckdb_fdw.so"
  echo -e "\e[32m[OK] duckdb_fdw.so successfully deployed to ${DEFAULT_PG_LIB}\e[0m"
else
  echo "Error: Could not find duckdb_fdw.so at '${BUILD_DIR}'."
  echo "Verify your Ninja compilation task finished successfully in Release mode."
  exit 1
fi

# --- 2. Deploy Extension Control and SQL Scripts ---
CONTROL_FILE="${SOURCE_DIR}/duckdb_fdw.control"
if [ -f "${CONTROL_FILE}" ]; then
  cp "${CONTROL_FILE}" "${DEFAULT_PG_EXT}/"
  chmod 644 "${DEFAULT_PG_EXT}/duckdb_fdw.control"

  # Track down and copy all version initialization arrays
  # Safe expansion matching using find to prevent globbing failures
  find "${SOURCE_DIR}" -maxdepth 1 -name "duckdb_fdw--*.sql" -exec cp {} "${DEFAULT_PG_EXT}/" \;
  find "${DEFAULT_PG_EXT}" -maxdepth 1 -name "duckdb_fdw--*.sql" -exec chmod 644 {} \;
  echo -e "\e[32m[OK] Control file and SQL migration scripts deployed to ${DEFAULT_PG_EXT}\e[0m"
else
  echo "Error: Could not locate 'duckdb_fdw.control' inside target source directory: ${SOURCE_DIR}"
  exit 1
fi

# --- 3. Note on Dependent Shared Library Handling ---
# Linux utilizes RPATH configuration embedded in the compilation layer ($ORIGIN).
# It will automatically find 'libduckdb.so' if it resides in the same directory as duckdb_fdw.so,
# or if it sits right inside the PostgreSQL pkglibdir.
if [ -f "${SOURCE_DIR}/libduckdb.so" ]; then
  cp "${SOURCE_DIR}/libduckdb.so" "${DEFAULT_PG_LIB}/"
  chmod 755 "${DEFAULT_PG_LIB}/libduckdb.so"
  echo -e "\e[32m[OK] Core engine runtime (libduckdb.so) deployed to ${DEFAULT_PG_LIB}\e[0m"
fi

echo -e "\n\e[32mDeployment completed successfully.\e[0m"
echo -e "\e[33mRemember to restart the PostgreSQL service before executing 'CREATE EXTENSION duckdb_fdw;'\e[0m"
