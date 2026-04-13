#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DUCKDB_FDW_SO="${REPO_ROOT}/duckdb_fdw.so"
PGHOST="${PGHOST:-/tmp}"
PGPORT="${PGPORT:-5433}"
PGUSER="${PGUSER:-$(whoami)}"
PGDATABASE="${PGDATABASE:-postgres}"
PSQL_BIN="${PSQL_BIN:-psql}"
CC_BIN="${CC_BIN:-cc}"

if [[ ! -f "${DUCKDB_FDW_SO}" ]]; then
	echo "[FAIL] 未找到 ${DUCKDB_FDW_SO}，请先执行 make USE_PGXS=1"
	exit 1
fi

if ! command -v pg_config >/dev/null 2>&1; then
	echo "[FAIL] 未找到 pg_config"
	exit 1
fi

STUB_DIR="$(mktemp -d /tmp/duckdb_fdw_coexistence.XXXXXX)"
trap 'rm -rf "${STUB_DIR}"' EXIT

PG_SERVER_INCLUDE="$(pg_config --includedir-server)"
PG_INTERNAL_INCLUDE="$(pg_config --includedir)/internal"
STUB_C="${STUB_DIR}/pg_duckdb_stub.c"
STUB_SO="${STUB_DIR}/pg_duckdb.so"

cat > "${STUB_C}" <<'EOF'
#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

void _PG_init(void) {}
EOF

"${CC_BIN}" -fPIC -shared \
	-I"${PG_SERVER_INCLUDE}" \
	-I"${PG_INTERNAL_INCLUDE}" \
	-o "${STUB_SO}" \
	"${STUB_C}"

psql_run() {
	PGHOST="${PGHOST}" PGPORT="${PGPORT}" PGUSER="${PGUSER}" "${PSQL_BIN}" -d "${PGDATABASE}" -v ON_ERROR_STOP=1 "$@"
}

assert_contains() {
	local haystack="$1"
	local needle="$2"

	if [[ "${haystack}" != *"${needle}"* ]]; then
		echo "[FAIL] 输出不包含预期片段: ${needle}"
		echo "------ output ------"
		echo "${haystack}"
		echo "--------------------"
		exit 1
	fi
}

echo "[1/4] 验证 peer-loaded backend 默认阻断 duckdb_fdw_version()"
if blocked_version_output="$(psql_run -c "LOAD '${STUB_SO}'; LOAD '${DUCKDB_FDW_SO}'; CREATE FUNCTION pg_temp.duckdb_fdw_version_tmp() RETURNS text AS '${DUCKDB_FDW_SO}', 'duckdb_fdw_version' LANGUAGE C STRICT; SELECT pg_temp.duckdb_fdw_version_tmp();" 2>&1)"; then
	echo "[FAIL] 预期 duckdb_fdw_version() 在 peer-loaded 场景下失败，但命令成功了"
	echo "${blocked_version_output}"
	exit 1
fi
assert_contains "${blocked_version_output}" "strict coexistence policy rejected the current DuckDB runtime combination"
assert_contains "${blocked_version_output}" "peer_loaded_need_validation"

echo "[2/4] 验证 override 会告警并放行 duckdb_fdw_version()"
override_version_output="$(psql_run -c "LOAD '${STUB_SO}'; LOAD '${DUCKDB_FDW_SO}'; SET duckdb_fdw.allow_unsupported_pg_duckdb_coexistence = on; CREATE FUNCTION pg_temp.duckdb_fdw_version_tmp() RETURNS text AS '${DUCKDB_FDW_SO}', 'duckdb_fdw_version' LANGUAGE C STRICT; SELECT pg_temp.duckdb_fdw_version_tmp() IS NOT NULL;" 2>&1)"
assert_contains "${override_version_output}" "duckdb_fdw is running in unsupported pg_duckdb coexistence mode"
assert_contains "${override_version_output}" "t"

echo "[3/4] 验证 peer-loaded backend 默认阻断 duckdb_get_connection() 路径"
if blocked_execute_output="$(psql_run -c "BEGIN; LOAD '${STUB_SO}'; LOAD '${DUCKDB_FDW_SO}'; CREATE FUNCTION pg_temp.duckdb_fdw_handler() RETURNS fdw_handler AS '${DUCKDB_FDW_SO}', 'duckdb_fdw_handler' LANGUAGE C STRICT; CREATE FUNCTION pg_temp.duckdb_fdw_validator(text[], oid) RETURNS void AS '${DUCKDB_FDW_SO}', 'duckdb_fdw_validator' LANGUAGE C STRICT; CREATE FUNCTION pg_temp.duckdb_execute(name, text) RETURNS void AS '${DUCKDB_FDW_SO}', 'duckdb_execute' LANGUAGE C STRICT; CREATE FOREIGN DATA WRAPPER duckdb_fdw_tmp HANDLER pg_temp.duckdb_fdw_handler VALIDATOR pg_temp.duckdb_fdw_validator; CREATE SERVER duckdb_guard_srv FOREIGN DATA WRAPPER duckdb_fdw_tmp OPTIONS (database ':memory:'); SELECT pg_temp.duckdb_execute('duckdb_guard_srv', 'SELECT 1'); ROLLBACK;" 2>&1)"; then
	echo "[FAIL] 预期 duckdb_execute() 在 peer-loaded 场景下失败，但命令成功了"
	echo "${blocked_execute_output}"
	exit 1
fi
assert_contains "${blocked_execute_output}" "strict coexistence policy rejected the current DuckDB runtime combination"
assert_contains "${blocked_execute_output}" "peer_loaded_need_validation"

echo "[4/4] 验证无 peer 时正常放行"
normal_execute_output="$(psql_run -c "BEGIN; LOAD '${DUCKDB_FDW_SO}'; CREATE FUNCTION pg_temp.duckdb_fdw_handler() RETURNS fdw_handler AS '${DUCKDB_FDW_SO}', 'duckdb_fdw_handler' LANGUAGE C STRICT; CREATE FUNCTION pg_temp.duckdb_fdw_validator(text[], oid) RETURNS void AS '${DUCKDB_FDW_SO}', 'duckdb_fdw_validator' LANGUAGE C STRICT; CREATE FUNCTION pg_temp.duckdb_execute(name, text) RETURNS void AS '${DUCKDB_FDW_SO}', 'duckdb_execute' LANGUAGE C STRICT; CREATE FOREIGN DATA WRAPPER duckdb_fdw_tmp HANDLER pg_temp.duckdb_fdw_handler VALIDATOR pg_temp.duckdb_fdw_validator; CREATE SERVER duckdb_guard_srv FOREIGN DATA WRAPPER duckdb_fdw_tmp OPTIONS (database ':memory:'); SELECT pg_temp.duckdb_execute('duckdb_guard_srv', 'SELECT 1'); ROLLBACK;" 2>&1)"
assert_contains "${normal_execute_output}" "ROLLBACK"

echo "[OK] pg_duckdb coexistence runtime guard verification passed"
