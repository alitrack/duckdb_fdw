#!/usr/bin/env bash
set -euo pipefail

PG_MAJOR="${PG_MAJOR:-17}"
FAILURES=0

usage() {
    cat <<'EOF'
Usage: scripts/verify_pg_env.sh [options]

Verify PostgreSQL development prerequisites for duckdb_fdw.

Options:
  --pg-major <13-18>   Expected PostgreSQL major version (default: 17)
  -h, --help           Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pg-major)
            PG_MAJOR="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "未知参数: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

pass() {
    echo "[OK] $1"
}

fail() {
    echo "[FAIL] $1"
    FAILURES=$((FAILURES + 1))
}

resolve_pg_tool() {
    local tool="$1"

    if command -v "${tool}" >/dev/null 2>&1; then
        command -v "${tool}"
        return 0
    fi

    if [[ -n "${BINDIR:-}" && -x "${BINDIR}/${tool}" ]]; then
        printf '%s\n' "${BINDIR}/${tool}"
        return 0
    fi

    return 1
}

if command -v pg_config >/dev/null 2>&1; then
    PG_CONFIG_PATH="$(command -v pg_config)"
    PG_CONFIG_VERSION="$(pg_config --version 2>/dev/null || true)"
    pass "找到 pg_config: ${PG_CONFIG_PATH} (${PG_CONFIG_VERSION})"
else
    fail "未找到 pg_config"
fi

if command -v pg_config >/dev/null 2>&1; then
    PGXS_PATH="$(pg_config --pgxs 2>/dev/null || true)"
    if [[ -n "${PGXS_PATH}" && -f "${PGXS_PATH}" ]]; then
        pass "PGXS 可用: ${PGXS_PATH}"
    else
        fail "pg_config --pgxs 未返回有效文件"
    fi

    INCLUDE_DIR="$(pg_config --includedir-server 2>/dev/null || true)"
    if [[ -n "${INCLUDE_DIR}" && -f "${INCLUDE_DIR}/postgres.h" ]]; then
        pass "服务端头文件可用: ${INCLUDE_DIR}/postgres.h"
    else
        fail "缺少 PostgreSQL 服务端头文件 postgres.h"
    fi

    BINDIR="$(pg_config --bindir 2>/dev/null || true)"
    if [[ -n "${BINDIR}" ]]; then
        pass "PostgreSQL bin 目录: ${BINDIR}"
    else
        fail "无法解析 pg_config --bindir"
    fi
fi

for tool in psql pg_ctl initdb; do
    if TOOL_PATH="$(resolve_pg_tool "${tool}")"; then
        pass "找到 ${tool}: ${TOOL_PATH}"
    else
        fail "未找到 ${tool}"
    fi
done

if command -v psql >/dev/null 2>&1; then
    PSQL_VERSION="$(psql --version 2>/dev/null || true)"
    if [[ "${PSQL_VERSION}" == *"${PG_MAJOR}"* ]]; then
        pass "psql 版本匹配期望主版本 ${PG_MAJOR}: ${PSQL_VERSION}"
    else
        echo "[WARN] psql 版本未显式匹配期望主版本 ${PG_MAJOR}: ${PSQL_VERSION}"
    fi
fi

if command -v make >/dev/null 2>&1; then
    pass "找到 make: $(command -v make)"
else
    fail "未找到 make"
fi

if [[ -f ./download_libduckdb.sh ]]; then
    pass "DuckDB bootstrap 脚本存在"
else
    fail "仓库根目录缺少 ./download_libduckdb.sh"
fi

echo
if [[ "${FAILURES}" -eq 0 ]]; then
    echo "环境检查通过。下一步建议："
    echo "  ./download_libduckdb.sh"
    echo "  make USE_PGXS=1"
    echo "  make USE_PGXS=1 install"
    exit 0
fi

echo "环境检查未通过，失败项数量: ${FAILURES}"
echo "建议先运行:"
echo "  scripts/install_pg_env.sh --pg-major ${PG_MAJOR} --apply"
exit 1
