#!/usr/bin/env bash
set -euo pipefail

PG_MAJOR="${PG_MAJOR:-17}"
APPLY=0
WITH_SERVER=1
SETUP_PGDG=1

usage() {
    cat <<'EOF'
Usage: scripts/install_pg_env.sh [options]

Prepare PostgreSQL development prerequisites for duckdb_fdw on Debian/Ubuntu.

Options:
  --pg-major <13-18>   PostgreSQL major version to install (default: 17)
  --no-server          Skip postgresql-<major> and postgresql-contrib-<major>
  --skip-pgdg          Do not add/update the PGDG apt repository
  --apply              Execute apt/repository changes instead of printing the plan
  -h, --help           Show this help

Environment:
  PG_MAJOR             Default PostgreSQL major version if --pg-major is omitted

Examples:
  scripts/install_pg_env.sh --pg-major 17
  scripts/install_pg_env.sh --pg-major 17 --apply
  sudo scripts/install_pg_env.sh --pg-major 17 --apply
EOF
}

require_supported_os() {
    if [[ ! -r /etc/os-release ]]; then
        echo "无法识别当前系统：缺少 /etc/os-release" >&2
        exit 1
    fi

    # shellcheck disable=SC1091
    . /etc/os-release
    DISTRO_ID="${ID:-}"
    DISTRO_CODENAME="${VERSION_CODENAME:-}"

    case "${DISTRO_ID}" in
        ubuntu|debian)
            ;;
        *)
            echo "当前脚本仅支持 Debian/Ubuntu 系环境，检测到: ${DISTRO_ID:-unknown}" >&2
            exit 1
            ;;
    esac

    if [[ -z "${DISTRO_CODENAME}" ]]; then
        echo "无法从 /etc/os-release 解析 VERSION_CODENAME" >&2
        exit 1
    fi
}

validate_pg_major() {
    case "${PG_MAJOR}" in
        13|14|15|16|17|18)
            ;;
        *)
            echo "不支持的 PostgreSQL 主版本: ${PG_MAJOR}。允许值: 13-18" >&2
            exit 1
            ;;
    esac
}

run_cmd() {
    if [[ "${APPLY}" -eq 1 ]]; then
        "${SUDO[@]}" "$@"
    else
        printf 'PLAN:'
        printf ' %q' "$@"
        printf '\n'
    fi
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --pg-major)
            PG_MAJOR="$2"
            shift 2
            ;;
        --no-server)
            WITH_SERVER=0
            shift
            ;;
        --skip-pgdg)
            SETUP_PGDG=0
            shift
            ;;
        --apply)
            APPLY=1
            shift
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

require_supported_os
validate_pg_major

if [[ "${EUID}" -eq 0 ]]; then
    SUDO=()
else
    SUDO=(sudo)
fi

BASE_PACKAGES=(
    ca-certificates
    curl
    gnupg
    build-essential
    pkg-config
    unzip
    libpq-dev
    postgresql-client-"${PG_MAJOR}"
    postgresql-server-dev-"${PG_MAJOR}"
)

if [[ "${WITH_SERVER}" -eq 1 ]]; then
    BASE_PACKAGES+=(
        postgresql-"${PG_MAJOR}"
        postgresql-contrib-"${PG_MAJOR}"
    )
fi

echo "目标系统: ${DISTRO_ID} (${DISTRO_CODENAME})"
echo "目标 PostgreSQL 主版本: ${PG_MAJOR}"
echo "安装模式: $([[ "${APPLY}" -eq 1 ]] && echo 'apply' || echo 'plan-only')"
echo

if [[ "${SETUP_PGDG}" -eq 1 ]]; then
    echo "将按 PostgreSQL 官方 PGDG APT 方式准备仓库配置。"
    run_cmd apt-get update
    run_cmd apt-get install -y ca-certificates curl gnupg postgresql-common
    run_cmd install -d /usr/share/postgresql-common/pgdg
    run_cmd curl -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc --fail https://www.postgresql.org/media/keys/ACCC4CF8.asc

    if [[ "${APPLY}" -eq 1 ]]; then
        REPO_LINE="deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${DISTRO_CODENAME}-pgdg main"
        printf '%s\n' "${REPO_LINE}" | "${SUDO[@]}" tee /etc/apt/sources.list.d/pgdg.list >/dev/null
    else
        echo "PLAN: write /etc/apt/sources.list.d/pgdg.list"
        echo "      deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt ${DISTRO_CODENAME}-pgdg main"
    fi

    run_cmd apt-get update
fi

echo "将安装以下包:"
printf '  - %s\n' "${BASE_PACKAGES[@]}"
echo

run_cmd apt-get install -y "${BASE_PACKAGES[@]}"

echo
if [[ "${APPLY}" -eq 1 ]]; then
    echo "PostgreSQL 开发环境安装步骤已执行完成。"
else
    echo "当前为 plan-only 模式，未修改系统。"
    echo "如需真正执行，请运行:"
    echo "  scripts/install_pg_env.sh --pg-major ${PG_MAJOR} $([[ "${WITH_SERVER}" -eq 0 ]] && echo '--no-server ')--apply"
fi

echo "安装后建议执行:"
echo "  scripts/verify_pg_env.sh --pg-major ${PG_MAJOR}"
