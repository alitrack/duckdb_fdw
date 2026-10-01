#!/bin/bash
# ============================================================================
# scripts/regress_correctness.sh — 可复现的正确性回归 (P0+P1+P2 修复批次)
#
# 用法:
#   scripts/regress_correctness.sh
# 环境变量 (均有默认值, 可覆盖):
#   PSQL_BIN     psql 可执行文件          (默认 psql)
#   PGHOST       连接地址                 (默认 127.0.0.1)
#   PGPORT       端口                     (默认 5433)
#   PGUSER       用户                     (默认 lhy)
#   PGDATABASE   数据库                   (默认 postgres)
#   FDW_TEMP_DB  DuckDB 临时库文件        (默认 /tmp/fdw_regress.db)
#
# 说明:
#  - 不走 make install: 直载 $REPO_ROOT/duckdb_fdw.so (参考 build_and_test.sh),
#    要求仓库已 make 出 duckdb_fdw.so。
#  - 自建 DuckDB temp db (FDW_TEMP_DB), 结尾自清理: DROP FOREIGN TABLE /
#    SERVER / USER MAPPING, 删 temp db; 函数与 wrapper 保留(指向新 .so)。
#  - 副作用: 与 review-20261001 参考脚本一致, 会重建 duckdb_fdw wrapper,
#    其级联删除该 wrapper 下的既有 server(如 p0_srv, 可重跑参考脚本重建)。
#  - 全过 exit 0; 有 FAIL exit 1; 前置失败(缺 .so / 连不上 / 版本不符) exit 2。
# ============================================================================
set -u

PSQL_BIN=${PSQL_BIN:-psql}
PGHOST=${PGHOST:-127.0.0.1}
PGPORT=${PGPORT:-5433}
PGUSER=${PGUSER:-lhy}
PGDATABASE=${PGDATABASE:-postgres}
FDW_TEMP_DB=${FDW_TEMP_DB:-/tmp/fdw_regress.db}

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
SO="$REPO_ROOT/duckdb_fdw.so"
PSQL="$PSQL_BIN -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -X"

# psql -Atq: 单值/单行输出; ON_ERROR_STOP 防止单语句错误吞掉后续语句
psql_a() { $PSQL -Atq -v ON_ERROR_STOP=1 -c "$1" 2>&1; }
# psql_x: 执行语句, 出错返回非零
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
    echo "连不上 $PGHOST:$PGPORT (user=$PGUSER db=$PGDATABASE)"; exit 2
fi
rm -f "$FDW_TEMP_DB"
echo "连上: $($PSQL -Atc "SELECT version()")"

echo "=== 环境重建 (直载 $SO) ==="
for ft in r2_wide r2_t r2_s r2_ts r2_ftx r2_fcol r2_fcolc r2_j1 r2_j2 r2_fgrp r2_fcpy r2_q13c r2_q13o r2_mis r2_mistwin; do
    psql_x "DROP FOREIGN TABLE IF EXISTS $ft CASCADE"
done
psql_x "DROP SERVER IF EXISTS r2_srv CASCADE"
psql_x "DROP USER MAPPING IF EXISTS FOR $PGUSER SERVER r2_srv"
psql_x "DROP FOREIGN DATA WRAPPER IF EXISTS duckdb_fdw CASCADE"
psql_x "DROP FUNCTION IF EXISTS duckdb_fdw_handler()"
psql_x "DROP FUNCTION IF EXISTS duckdb_fdw_validator(text[], oid)"
psql_x "DROP FUNCTION IF EXISTS duckdb_fdw_version()"
psql_x "DROP FUNCTION IF EXISTS duckdb_execute(name, text)"
psql_x "CREATE FUNCTION duckdb_fdw_handler() RETURNS fdw_handler AS '$SO' LANGUAGE C STRICT" || exit 2
psql_x "CREATE FUNCTION duckdb_fdw_validator(text[], oid) RETURNS void AS '$SO' LANGUAGE C STRICT" || exit 2
psql_x "CREATE FUNCTION duckdb_fdw_version() RETURNS text AS '$SO' LANGUAGE C STRICT" || exit 2
psql_x "CREATE FUNCTION duckdb_execute(server name, statement text) RETURNS void AS '$SO' LANGUAGE C STRICT" || exit 2
psql_x "CREATE FOREIGN DATA WRAPPER duckdb_fdw HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator" || exit 2
psql_x "CREATE SERVER r2_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '$FDW_TEMP_DB')" || exit 2
psql_x "CREATE USER MAPPING FOR $PGUSER SERVER r2_srv" || exit 2

# C2 版本断言: 注册的 .so 必须报 1.5.x (FDW 在首次建连时亦会硬断言 [1.5,1.6))
LIBVER=$(psql_a "SELECT duckdb_fdw_version()")
case "$LIBVER" in
    1.5.*|v1.5.*) : ;;
    *) echo "前置失败: libduckdb 版本 $LIBVER 不在已测范围 1.5.x"; exit 2 ;;
esac
echo "libduckdb: $LIBVER"

echo "=== 建数据 (DuckDB temp db: $FDW_TEMP_DB) ==="
# $2 内嵌单引号需按 SQL 字面量规则翻倍
ddl() { # $1=表名 $2=表定义(列清单或 "AS SELECT ..." 子句)
    local esc=${2//"'"/"''"}
    psql_x "SELECT duckdb_execute('r2_srv', 'CREATE OR REPLACE TABLE $1 $esc')" \
        || { echo "前置失败: 建 DuckDB 表 $1"; exit 2; }
}
ddl r2_src  "AS SELECT (i)::INTEGER AS id, ((i*7)%100)::SMALLINT AS s16, (i%7)::TINYINT AS t8, (i/10)::REAL AS r4, 'row'||i AS txt, DATE '2020-01-01' + (i)::INTEGER AS d, (TIMESTAMP '2020-01-01' + ((i*37)%86400) * INTERVAL 1 SECOND)::TIMESTAMPTZ AS ts, [i, i+1] AS arr FROM range(1, 3001) t(i)"
ddl r2_tx   "AS SELECT 1 AS v"
ddl r2_cols "(a int, b int, c text DEFAULT 'extra')"
ddl r2_j1   "AS SELECT (i)::INTEGER AS id, 'w'||i AS w FROM range(1, 101) t(i)"
ddl r2_j2   "AS SELECT (i)::INTEGER AS id FROM range(1, 101) t(i)"
ddl r2_grp  "AS SELECT (i%5)::INTEGER AS g, i::BIGINT AS v FROM range(1, 201) t(i)"
ddl r2_cpy  "(v int)"
# Q13 形状数据: cust 5 行 (id 1..5); orders: c1 一条 'pending%' (被 ON 子句
# NOT LIKE 过滤) + 一条 'ok', c2 两条普通, c3/c4/c5 无订单
# (VALUES 中单引号由 ddl() 翻倍转义)
ddl r2_q13c "AS SELECT (i)::INTEGER AS id FROM range(1, 6) t(i)"
ddl r2_q13o "AS SELECT * FROM (VALUES (1, 1, 'pending%'), (2, 1, 'ok'), (3, 2, 'plain'), (4, 2, 'plain2')) AS t(id, cid, comment)"
# [15] 数据: i 为 INTEGER (故意宽于 PG 侧 int2 声明), ts 为 TIMESTAMPTZ 确定值
# (2024-03-15 09:00+00 .. 13:00+00, DuckDB 默认 TimeZone=UTC)
ddl r2_mis  "AS SELECT (i)::INTEGER AS i, (TIMESTAMP '2024-03-15 08:00:00' + i * INTERVAL 1 HOUR)::TIMESTAMPTZ AS ts FROM range(1, 5) t(i)"

psql_x "CREATE FOREIGN TABLE r2_wide (s16 int, t8 int, id int) SERVER r2_srv OPTIONS (table 'r2_src')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_t (id int, s16 smallint, t8 smallint, r4 real, txt text, d date, ts timestamptz, arr int[]) SERVER r2_srv OPTIONS (table 'r2_src')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_s (s16 smallint, r4 real, id int) SERVER r2_srv OPTIONS (table 'r2_src')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_ts (s16 smallint, ts timestamptz, id int) SERVER r2_srv OPTIONS (table 'r2_src')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_ftx (v int) SERVER r2_srv OPTIONS (table 'r2_tx')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_fcol (b int, a int) SERVER r2_srv OPTIONS (table 'r2_cols')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_fcolc (a int, b int, c text) SERVER r2_srv OPTIONS (table 'r2_cols')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_j1 (id int, w text) SERVER r2_srv OPTIONS (table 'r2_j1')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_j2 (id int) SERVER r2_srv OPTIONS (table 'r2_j2')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_fgrp (g int, v bigint) SERVER r2_srv OPTIONS (table 'r2_grp')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_fcpy (v int) SERVER r2_srv OPTIONS (table 'r2_cpy')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_q13c (id int) SERVER r2_srv OPTIONS (table 'r2_q13c')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_q13o (id int, cid int, comment text) SERVER r2_srv OPTIONS (table 'r2_q13o')" || exit 2
# [15] r2_mis 故意宽度不符: PG 声明 i int2 对应 DuckDB INTEGER; r2_mistwin 为同表正确类型声明 (i int4), 作 oracle
psql_x "CREATE FOREIGN TABLE r2_mis (i int2, ts timestamptz) SERVER r2_srv OPTIONS (table 'r2_mis')" || exit 2
psql_x "CREATE FOREIGN TABLE r2_mistwin (i int4, ts timestamptz) SERVER r2_srv OPTIONS (table 'r2_mis')" || exit 2

# $1=id ; 输出 "1" = 纯TZ chunk 路径 与 INT2+TZ 混合路径同值、年份 2020、
# 且未落在 2000-01-01 epoch 窗口 (旧 TIMESTAMPTZ 文本回退 bug 的特征值)
ts_paths_same() {
    local pure mixed year
    pure=$(psql_a "SELECT (extract(epoch from ts))::bigint FROM r2_t WHERE id=$1")
    mixed=$(psql_a "SELECT (extract(epoch from ts))::bigint FROM (SELECT s16, ts FROM r2_ts WHERE id=$1) x")
    year=$(psql_a "SELECT extract(year from ts) FROM (SELECT ts FROM r2_t WHERE id=$1) x")
    [ -n "$pure" ] && [ "$pure" = "$mixed" ] || return 1
    [ "$year" = "2020" ] || return 1
    case "$pure" in ''|*[!0-9]*) return 1 ;; esac
    # 2000-01-01 00:00 UTC = 946684800; 落在 +/-1 天窗口内即命中旧 epoch bug
    [ "$pure" -ge 946608000 ] && [ "$pure" -le 946771200 ] && return 1
    echo 1
}

echo "=== [1] INT 宽度: PG int4 读 DuckDB SMALLINT/TINYINT (p0_wide 形状) ==="
check "1a wide id=1 (s16,t8)" "7|1" "$(psql_a "SELECT s16, t8 FROM r2_wide WHERE id=1")"
check "1b wide 抽样 id 1..3"   "7
14
21" "$(psql_a "SELECT s16 FROM r2_wide WHERE id IN (1,2,3) ORDER BY s16")"
check "1c wide 全表 min/max"   "3000|0|99" "$(psql_a "SELECT count(*), min(s16), max(s16) FROM r2_wide")"

echo "=== [2] NULLIF 两查询计数 ==="
check "2a nullif(t8,0) IS NOT NULL" "2572" "$(psql_a "SELECT count(*) FROM r2_t WHERE nullif(t8, 0) IS NOT NULL")"
check "2b nullif(t8,3) IS NULL"     "429"  "$(psql_a "SELECT count(*) FROM r2_t WHERE nullif(t8, 3) IS NULL")"

echo "=== [3] 正则 ~ ==="
check "3a txt ~ '^row1'" "1111" "$(psql_a "SELECT count(*) FROM r2_t WHERE txt ~ '^row1'")"

echo "=== [4] ANY int/text ==="
check "4a id = ANY(int[])"   "3" "$(psql_a "SELECT count(*) FROM r2_t WHERE id = ANY(ARRAY[1,2,3])")"
check "4b txt = ANY(text[])" "1" "$(psql_a "SELECT count(*) FROM r2_t WHERE txt = ANY(ARRAY['row1','a,b'])")"

echo "=== [5] DateStyle ISO/DMY/Postgres 三态同值 ==="
check "5a DateStyle=ISO"      "2879" "$(psql_a "SET DateStyle='ISO'; SELECT count(*) FROM r2_t WHERE d > '2020-05-01'")"
check "5b DateStyle=DMY"      "2879" "$(psql_a "SET DateStyle='DMY'; SELECT count(*) FROM r2_t WHERE d > '2020-05-01'")"
check "5c DateStyle=Postgres" "2879" "$(psql_a "SET DateStyle='Postgres'; SELECT count(*) FROM r2_t WHERE d > '2020-05-01'")"

echo "=== [6] ROLLBACK 原子性 + COMMIT 持久 ==="
N0=$(psql_a "SELECT count(*) FROM r2_ftx")
psql_x "BEGIN; INSERT INTO r2_ftx VALUES (99); ROLLBACK;"
N1=$(psql_a "SELECT count(*) FROM r2_ftx")
check "6a ROLLBACK 后零残留" "$N0" "$N1"
psql_x "BEGIN; INSERT INTO r2_ftx VALUES (100); COMMIT;"
N2=$(psql_a "SELECT count(*) FROM r2_ftx")
check "6b COMMIT 持久 +1" "$((N1+1))" "$N2"
if psql_x "BEGIN; SELECT count(*) FROM r2_ftx; COMMIT;"; then
    check "6c COMMIT 后新会话可读" "$N2" "$(psql_a 'SELECT count(*) FROM r2_ftx')"
else
    check "6c COMMIT 后新会话可读" "$N2" "ERROR"
fi

echo "=== [7] SAVEPOINT 回滚后查询存活 ==="
# B1 既定语义(connection.c duckdb_connection_subxact_callback 注释): 被回滚子事务
# 写入的行在父事务 COMMIT 时保留于远端(远端事务全有或全无), 父 ABORT 时随之回滚;
# 因此此处期望 N2+2, 用例重点 = 子事务回滚后同会话/新会话查询不崩且结果确定。
N3=$($PSQL -Atq -v ON_ERROR_STOP=1 -c "
BEGIN;
INSERT INTO r2_ftx VALUES (101);
SAVEPOINT sp;
INSERT INTO r2_ftx VALUES (102);
ROLLBACK TO SAVEPOINT sp;
SELECT count(*) FROM r2_ftx;
COMMIT;" 2>&1)
check "7a 子事务回滚后同会话查询存活(B1)" "$((N2+2))" "$N3"
N4=$(psql_a "SELECT count(*) FROM r2_ftx")
check "7b 新会话查询存活且持久" "$((N2+2))" "$N4"

echo "=== [8] 反序外表按名写列 ==="
psql_x "INSERT INTO r2_fcol VALUES (7, 42)" \
    || check "8a INSERT 反序外表" "ok" "ERROR"
# duckdb_execute 无返回值, 经外表读回 (PG 侧声明顺序 (b,a), DuckDB 侧 (a,b,c));
# r2_fcolc 带 c 列以验证 DuckDB 侧多出的列落 NULL
GOT=$(psql_a "SELECT a || '|' || b || '|' || coalesce(c, '') FROM r2_fcolc")
check "8b (b,a) 反序 → DuckDB (a=42,b=7,c=NULL)" "42|7|" "$GOT"

echo "=== [9] join 本地条件 (md5) ==="
# length(md5(..)) 恒为 32 → fj2.id = fj1.id + 32 ∈ 1..100 → fj1.id ∈ 1..68 → 68
check "9a md5 join 计数" "68" "$(psql_a "SELECT count(*) FROM r2_j1, r2_j2 WHERE r2_j1.id = r2_j2.id + length(md5(r2_j1.w))")"

echo "=== [10] GROUPING SETS/ROLLUP 总计行 ==="
# r2_grp: i=1..200, g=i%5; 各组和 4100/3940/3980/4020/4060, 总计行 g=NULL 和 20100
check "10a ROLLUP 全输出(含 g=NULL 总计行)" "|20100
0|4100
1|3940
2|3980
3|4020
4|4060" "$(psql_a "SELECT g, sum(v) FROM r2_fgrp GROUP BY ROLLUP(g) ORDER BY g NULLS FIRST")"

echo "=== [11] COPY FROM 计数 ==="
if printf '1\n2\n3\n4\n5\n' | $PSQL -q -v ON_ERROR_STOP=1 -c "COPY r2_fcpy (v) FROM STDIN" >/dev/null 2>&1; then
    check "11a COPY 执行" "ok" "ok"
else
    check "11a COPY 执行" "ok" "ERROR"
fi
check "11b COPY 后行数=5" "5" "$(psql_a "SELECT count(*) FROM r2_fcpy")"

echo "=== [12] C1 原生定宽单一事实源: 原生 chunk + 文本回退双路径 ==="
check "12a INT2 原生 SMALLINT chunk 直读" "7" "$(psql_a "SELECT s16 FROM r2_s WHERE id=1")"
check "12b FLOAT4 原生 REAL chunk 直读"  "2.1" "$(psql_a "SELECT r4 FROM r2_s WHERE id=21")"
check "12c 纯 TZ 投影(chunk 路径)非 2000 epoch" "1" "$(ts_paths_same 1)"
check "12d INT2+TZ 混合行(全定宽→chunk)" "1" "$(ts_paths_same 2)"
# 复杂列混排行: 文本回退 + 远端 CAST(ts AS VARCHAR), 必须与 chunk 路径同值
PURE12=$(psql_a "SELECT (extract(epoch from ts))::bigint FROM r2_t WHERE id=1")
MIXC12=$(psql_a "SELECT (extract(epoch from ts))::bigint FROM (SELECT ts, arr FROM r2_t WHERE id=1) x")
check "12e 复杂列混排行(文本回退+TZ cast)与 chunk 路径同值" "$PURE12" "$MIXC12"
check "12f 文本回退路径非 2000 epoch" "1" \
    "$( case "$MIXC12" in ''|*[!0-9]*) echo 0 ;; *) [ "$MIXC12" -ge 946771200 ] && echo 1 || echo 0 ;; esac )"

echo "=== [13] Q13 形状: LEFT JOIN + 聚合 + ON 子句 NOT LIKE (外连接禁下推回归) ==="
# 历史 bug: 外连接下推曾丢失 NULL 行组 (41 vs 42)。现外连接一律回退本地 join:
# 期望 5 组: c1=1(仅 'ok' 那条, 'pending%' 被 ON 过滤), c2=2, c3/c4/c5=0(NULL 行组保留)
check "13a Q13 逐行 (c1=1,c2=2,c3..c5=0)" "1|1
2|2
3|0
4|0
5|0" "$(psql_a "SELECT c.id, count(o.id) FROM r2_q13c c LEFT JOIN r2_q13o o ON c.id=o.cid AND o.comment NOT LIKE 'pending%' GROUP BY c.id ORDER BY c.id")"
# EXPLAIN 断言 (以实测形态为基准, 对 merge/hash/nested-loop 均稳定):
#  - 每个外表各一个 Foreign Scan (join 未合并进单个扫描); 若 join 被下推则只剩 1 个扫描
#  - 存在本地 Left Join 节点 (PG 节点名 "... Left Join "; DuckDB 下推形态为 Remote SQL 里的 'LEFT JOIN' 大写, 不会误命中)
Q13PLAN=$(psql_a "EXPLAIN SELECT c.id, count(o.id) FROM r2_q13c c LEFT JOIN r2_q13o o ON c.id=o.cid AND o.comment NOT LIKE 'pending%' GROUP BY c.id ORDER BY c.id")
NLJOIN=$(printf '%s\n' "$Q13PLAN" | grep -c ' Left Join')
NFS=$(printf '%s\n' "$Q13PLAN" | grep -c 'Foreign Scan on')
if [ "${NLJOIN:-0}" -ge 1 ] && [ "${NFS:-0}" -eq 2 ]; then
    check "13b EXPLAIN 本地左连接 (2x ForeignScan + 本地 Left Join)" "ok" "ok"
else
    check "13b EXPLAIN 本地左连接 (2x ForeignScan + 本地 Left Join)" "ok" "nljoin=$NLJOIN fscans=$NFS"
fi

echo "=== [14] keep_connection GUC: commit 保留连接 / abort 断开, 连续事务功能正确 ==="
# 单会话多语句 (GUC 为 session 级, 跨 psql 调用不保留): 先触发 .so 加载,
# 再 SET keep_connection=on, 然后两笔显式 COMMIT 事务 + 一笔 ABORT 事务 +
# 一笔隐式事务, 四次 count 全部成功且等值。功能断言 (两事务都能查);
# 连接复用带来的性能收益不硬断言 (见 README 权衡说明)。
KEEP_OUT=$($PSQL -Atq -v ON_ERROR_STOP=1 -c "
SELECT duckdb_fdw_version();
SET duckdb_fdw.keep_connection = on;
BEGIN; SELECT count(*) FROM r2_t; COMMIT;
BEGIN; SELECT count(*) FROM r2_t; COMMIT;
BEGIN; SELECT count(*) FROM r2_t; ABORT;
SELECT count(*) FROM r2_t;" 2>&1)
KEEP_EXP="$LIBVER
3000
3000
3000
3000"
check "14a keep_connection=on: 两笔 commit + 一笔 abort 后查询均成功" "$KEEP_EXP" "$KEEP_OUT"

# 对照: 默认 (off) 下同一序列行为不变 (每事务结束均清理缓存)
KEEP_OFF_OUT=$($PSQL -Atq -v ON_ERROR_STOP=1 -c "
SELECT duckdb_fdw_version();
SET duckdb_fdw.keep_connection = off;
BEGIN; SELECT count(*) FROM r2_t; COMMIT;
BEGIN; SELECT count(*) FROM r2_t; COMMIT;
BEGIN; SELECT count(*) FROM r2_t; ABORT;
SELECT count(*) FROM r2_t;" 2>&1)
check "14b keep_connection=off(默认): 同一序列行为不变" "$KEEP_EXP" "$KEEP_OFF_OUT"

echo "=== [15] C1 残角: 宽度不符 INT2 + 同投影裸 TZ → 文本回退下 TZ 读 NULL (E3 修复: 保守 cast) ==="
# 机理: INT2 在共享定宽集合 → deparse 裸输出; 但 DuckDB 侧实为 INTEGER,
# 运行期 duckdb_chunk_types_ok 拒入快路径 → 整行文本回退。修复前文本路径对
# 裸 TIMESTAMP_TZ 用 duckdb_value_varchar → NULL (C1 残角); 修复后同投影存在
# INT2/FLOAT4 即保守 CAST(ts AS VARCHAR), timestamptz_in 解析 → 与正确声明
# 的孪生表 (r2_mistwin, i int4 宽度假设成立, 走 chunk 快路径) 同值。
check "15a 宽度不符行 i 正确 (文本回退读整数)" "2" "$(psql_a 'SELECT i FROM r2_mis WHERE i=2')"
MIS_TS=$(psql_a "SELECT ts::text FROM r2_mis WHERE i=2")
TWIN_TS=$(psql_a "SELECT ts::text FROM r2_mistwin WHERE i=2")
check "15b 不符表 ts 与正确声明孪生表同值 (修复前为空=NULL)" "$TWIN_TS" "$MIS_TS"
case "$MIS_TS" in
  ''|NULL|'2000-01-01'*) check "15c ts 非 NULL 且非 2000 epoch" "ok" "epoch/null: '${MIS_TS}'" ;;
  *) check "15c ts 非 NULL 且非 2000 epoch" "ok" "ok" ;;
esac

echo "=== 自清理 ==="
for ft in r2_fcpy r2_fgrp r2_j1 r2_j2 r2_fcol r2_fcolc r2_ftx r2_ts r2_s r2_t r2_wide r2_q13c r2_q13o r2_mis r2_mistwin; do
    psql_x "DROP FOREIGN TABLE IF EXISTS $ft CASCADE"
done
psql_x "DROP USER MAPPING IF EXISTS FOR $PGUSER SERVER r2_srv"
psql_x "DROP SERVER IF EXISTS r2_srv CASCADE"
rm -f "$FDW_TEMP_DB"
echo "已清理: 外表/r2_srv/temp db; 保留 wrapper+函数(指向 $SO)"

echo "=== 结果: $TOTAL 项, PASS $((TOTAL-FAILS)), FAIL $FAILS ==="
if [ "$FAILS" -eq 0 ]; then
    echo "ALL GREEN"
    exit 0
else
    echo "HAS FAILURES"
    exit 1
fi
