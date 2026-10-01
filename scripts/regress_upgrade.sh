#!/bin/bash
# ============================================================================
# scripts/regress_upgrade.sh — 扩展升级链回归 (CREATE EXTENSION 路径)
#
# 用法:
#   scripts/regress_upgrade.sh
# 环境变量 (均有默认值, 可覆盖, 与 regress_correctness.sh 对齐):
#   PSQL_BIN     psql 可执行文件          (默认 psql)
#   PGHOST       连接地址                 (默认 127.0.0.1)
#   PGPORT       端口                     (默认 5433)
#   PGUSER       用户(需超级用户)         (默认 lhy)
#   PGDATABASE   管理库                   (默认 postgres)
#   UPGRADE_DB   升级测试专用库名         (默认 duckdb_fdw_upgrade, 脚本自删)
#   FDW_TEMP_DB  DuckDB 临时库文件        (默认 /tmp/fdw_upgrade.db)
#
# 说明:
#  - 直载 .so 的 wrapper 模式无法测扩展版本切换, 必须走 CREATE EXTENSION,
#    而 make install 需要写 PG 系统目录 → 需要 sudo。
#  - 前置: PG 可连 + 仓库已 make 出 $REPO_ROOT/duckdb_fdw.so + `sudo -n true`
#    无密码 sudo 可用。三者任一不满足:
#      缺 .so / 连不上 PG      → exit 2 (前置失败)
#      sudo 不可用             → 打印 SKIP 原因并 exit 0 (CI 无权限场景)
#  - 测试内容 (见下 "=== [A]/[B] ==="):
#      [A] 中段链: CREATE EXTENSION duckdb_fdw VERSION '1.4.1'
#          → 逐条 ALTER EXTENSION duckdb_fdw UPDATE 直至 2.0.1, 逐边断言
#            版本推进 (1.4.1→2.0.0→2.0.1), 终态做基础功能验证
#            (建 server + 外表 + count, 走已安装的 .so)。
#      [B] 全链:  DROP EXTENSION → CREATE EXTENSION duckdb_fdw VERSION '1.0.0'
#          → 逐边 UPDATE 覆盖全部升级边
#            1.0.0→1.1.2→1.1.3→1.3.2→1.4.1→2.0.0→2.0.1, 任一边断即 FAIL
#            并输出断在哪条边 (ALTER 报错信息 / 实际停留版本)。
#  - 结尾自清理: DROP EXTENSION + 专用库 + temp db; 系统目录里 make install
#    的产物保留 (重跑会覆盖)。
#  - 全过 exit 0; 有 FAIL exit 1; 前置失败(缺 .so / 连不上) exit 2。
# ============================================================================
set -u

PSQL_BIN=${PSQL_BIN:-psql}
PGHOST=${PGHOST:-127.0.0.1}
PGPORT=${PGPORT:-5433}
PGUSER=${PGUSER:-lhy}
PGDATABASE=${PGDATABASE:-postgres}
UPGRADE_DB=${UPGRADE_DB:-duckdb_fdw_upgrade}
FDW_TEMP_DB=${FDW_TEMP_DB:-/tmp/fdw_upgrade.db}

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
SO="$REPO_ROOT/duckdb_fdw.so"
PSQL="$PSQL_BIN -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -X"

psql_a() { $PSQL -Atq -v ON_ERROR_STOP=1 -c "$1" 2>&1; }
psql_x() { $PSQL -q -v ON_ERROR_STOP=1 -c "$1" >/dev/null 2>&1; }

FAILS=0; TOTAL=0
check() { # $1=用例名 $2=期望 $3=实际
    TOTAL=$((TOTAL+1))
    if [ "$2" = "$3" ]; then
        echo "PASS  $1  [=$3]"
    else
        FAILS=$((FAILS+1))
        echo "FAIL  $1  expected='$2' actual='$3'"
    fi
}

echo "=== 前置检查 ==="
[ -f "$SO" ] || { echo "缺少 $SO, 请先 make"; exit 2; }
if ! $PSQL -Atc "SELECT 1" >/dev/null 2>&1; then
    echo "连不上 $PGHOST:$PGPORT (user=$PGUSER db=$PGDATABASE)"; exit 2; fi
if ! sudo -n true 2>/dev/null; then
    echo "SKIP: 无密码 sudo 不可用 — make install 需要写 PG 系统目录"
    echo "     (extension 控制文件查找路径 \$libdir/../extension 固定,"
    echo "      直载 .so 的 wrapper 模式无法测 CREATE EXTENSION 版本切换)。"
    echo "     有 sudo 的机器上运行本脚本即可实测全链。"
    exit 0
fi
echo "连上: $($PSQL -Atc 'SELECT version()') (sudo 可用, 走 make install 路径)"

echo "=== 安装 (sudo make install: .so + libduckdb + extension SQL 脚本) ==="
if ! sudo -n make -C "$REPO_ROOT" install >/tmp/fdw_upgrade_install.log 2>&1; then
    echo "前置失败: sudo make install 出错, 日志尾部:"
    tail -5 /tmp/fdw_upgrade_install.log
    exit 2
fi

echo "=== 专用测试库: $UPGRADE_DB ==="
psql_x "DROP DATABASE IF EXISTS $UPGRADE_DB"
psql_x "CREATE DATABASE $UPGRADE_DB" || { echo "前置失败: 建库 $UPGRADE_DB (需超级用户)"; exit 2; }
PSQLDB="$PSQL_BIN -h $PGHOST -p $PGPORT -U $PGUSER -d $UPGRADE_DB -X"
psql_db_a() { $PSQLDB -Atq -v ON_ERROR_STOP=1 -c "$1" 2>&1; }
psql_db_x() { $PSQLDB -q -v ON_ERROR_STOP=1 -c "$1" >/dev/null 2>&1; }

cleanup() {
    psql_db_x "DROP EXTENSION IF EXISTS duckdb_fdw CASCADE" 2>/dev/null
    psql_x "DROP DATABASE IF EXISTS $UPGRADE_DB"
    rm -f "$FDW_TEMP_DB"
    echo "已清理: $UPGRADE_DB / temp db; make install 产物保留"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# run_upgrade_chain <期望版本序列...>
# 扩展须已安装 (由调用方 CREATE EXTENSION ... VERSION 'x' 保证起点);
# 对序列中每个版本依次执行
# ALTER EXTENSION duckdb_fdw UPDATE; 并断言 /extversion 恰推进到该版本
# (PG 每次 UPDATE 只前进一条边)。任一边断: 输出断在哪条边
# (ALTER 的报错, 或实际停留的版本) 并停止本链。
run_upgrade_chain() {
    local cur prev v err

    cur=$(psql_db_a "SELECT extversion FROM pg_extension WHERE extname='duckdb_fdw'")
    if [ -z "$cur" ]; then
        check "扩展已安装(升级链起点)" "ok" "MISSING"
        return 1
    fi
    for v in "$@"; do
        prev=$cur
        if ! psql_db_x "ALTER EXTENSION duckdb_fdw UPDATE;"; then
            err=$(psql_db_a "ALTER EXTENSION duckdb_fdw UPDATE;" | tail -1)
            check "升级边 $prev→$v" "ok" "ALTER 失败: $err"
            return 1
        fi
        cur=$(psql_db_a "SELECT extversion FROM pg_extension WHERE extname='duckdb_fdw'")
        if [ "$cur" != "$v" ]; then
            check "升级边 $prev→$v" "$v" "实际停留 $cur (升级边 $prev→$v 断)"
            return 1
        fi
        check "升级边 $prev→$v" "ok" "ok"
    done
    return 0
}

echo "=== [A] 中段链 1.4.1 → 2.0.1 + 终态基础功能 ==="
psql_db_x "DROP EXTENSION IF EXISTS duckdb_fdw CASCADE"
if psql_db_x "CREATE EXTENSION duckdb_fdw VERSION '1.4.1'"; then
    run_upgrade_chain 2.0.0 2.0.1
else
    check "CREATE EXTENSION duckdb_fdw VERSION '1.4.1'" "ok" "ERROR: $(psql_db_a "CREATE EXTENSION duckdb_fdw VERSION '1.4.1'" | tail -1)"
fi
VER_A=$(psql_db_a "SELECT extversion FROM pg_extension WHERE extname='duckdb_fdw'")
check "A 终态版本=2.0.1" "2.0.1" "${VER_A:-MISSING}"

# 基础功能: 已安装 .so + 2.0.1 SQL 状态下端到端走通 (建 server + 外表 + count)
rm -f "$FDW_TEMP_DB"
psql_db_x "DROP SERVER IF EXISTS u2_srv CASCADE"
if psql_db_x "CREATE SERVER u2_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '$FDW_TEMP_DB')" \
   && psql_db_x "CREATE USER MAPPING FOR $PGUSER SERVER u2_srv" \
   && psql_db_x "SELECT duckdb_execute('u2_srv', 'CREATE TABLE t AS SELECT (i)::INTEGER AS i FROM range(1, 4) t(i)')" \
   && psql_db_x "CREATE FOREIGN TABLE u2_t (i int) SERVER u2_srv OPTIONS (table 't')"; then
    check "A 基础查询 (server+外表+count)" "3" "$(psql_db_a 'SELECT count(*) FROM u2_t')"
else
    check "A 基础查询 (server+外表+count)" "3" "ERROR (建 server/外表/duckdb_execute 失败)"
fi
psql_db_x "DROP FOREIGN TABLE IF EXISTS u2_t"
psql_db_x "DROP USER MAPPING IF EXISTS FOR $PGUSER SERVER u2_srv"
psql_db_x "DROP SERVER IF EXISTS u2_srv CASCADE"

echo "=== [B] 全链 1.0.0 → 2.0.1 (覆盖所有升级边) ==="
psql_db_x "DROP EXTENSION IF EXISTS duckdb_fdw CASCADE"
if psql_db_x "CREATE EXTENSION duckdb_fdw VERSION '1.0.0'"; then
    run_upgrade_chain 1.1.2 1.1.3 1.3.2 1.4.1 2.0.0 2.0.1
else
    check "CREATE EXTENSION duckdb_fdw VERSION '1.0.0'" "ok" "ERROR: $(psql_db_a "CREATE EXTENSION duckdb_fdw VERSION '1.0.0'" | tail -1)"
fi
VER_B=$(psql_db_a "SELECT extversion FROM pg_extension WHERE extname='duckdb_fdw'")
check "B 终态版本=2.0.1" "2.0.1" "${VER_B:-MISSING}"

echo "=== 结果: $TOTAL 项, PASS $((TOTAL-FAILS)), FAIL $FAILS ==="
if [ "$FAILS" -eq 0 ]; then
    echo "ALL GREEN"
    exit 0
else
    echo "HAS FAILURES"
    exit 1
fi
