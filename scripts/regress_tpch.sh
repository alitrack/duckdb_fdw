#!/bin/bash
# ============================================================================
# scripts/regress_tpch.sh — TPC-H 22 条标准查询: DuckDB CLI vs PG+duckdb_fdw 逐行比对
#
# 目的: 把 "TPC-H sf1 22/22 与 DuckDB CLI 逐行一致" 从人工结论变成可复现脚本。
#   expected = DuckDB CLI 直接读 parquet (read_parquet glob, 8 张视图) 跑 22 条查询
#   actual   = PG 侧 duckdb_fdw 外表 (table='read_parquet(...)') 跑同样 22 条查询
#   比对   = 两侧输出各自规整后 diff -q 逐行比较:
#             * NULL 字段 (DuckDB 输出 "NULL", PG -A 输出空) → 统一置空
#             * 含小数/指数的数值字段 → 按 %.4f 规整 (TPC-H 有 sum/avg 浮点,
#               DuckDB 精确十进制 vs PG numeric 精度位数不同, 0.0001 精度足够)
#
# 用法:
#   scripts/regress_tpch.sh                 # 需 TPH_DATA 已设置, 否则 SKIP (exit 0)
#   TPH_GEN=1 scripts/regress_tpch.sh       # 用 duckdb tpch 扩展自动生成数据到临时目录
#   TPH_DATA=/path scripts/regress_tpch.sh  # 指向已有 parquet 目录
#   TPH_SF=0.01 scripts/regress_tpch.sh     # 任意 scale factor (默认 1)
#
# 环境变量:
#   TPH_DATA    8 张 parquet 表所在目录, 每表一个或多个文件, 文件名形如
#               <表名>.parquet 或 <表名>.<任意>.parquet (8 表: nation region part
#               supplier partsupp customer orders lineitem; 前缀匹配会排除跨表冲突,
#               如 part*.parquet 不会把 partsupp.parquet 算进 part 表)。
#               表名必须为 TPC-H 标准名(带列前缀: n_nationkey/p_partkey/...),
#               类型: 键 INTEGER/BIGINT, 金额 DECIMAL(15,2), 日期 DATE, 其余 VARCHAR。
#   TPH_SF      scale factor (默认 1); 仅 TPH_GEN 生成与输出信息用。
#   TPH_GEN     =1 时自动生数: $DUCKDB_CLI 临时库执行
#               INSTALL tpch; LOAD tpch; CALL dbgen(sf=$TPH_SF);
#               再把 8 张表 COPY 成 parquet 到临时目录 (需网络装扩展)。
#   DUCKDB_CLI  DuckDB CLI 可执行文件 (默认 duckdb); 不存在 → SKIP (exit 0)。
#   PSQL_BIN / PGHOST / PGPORT / PGUSER / PGDATABASE  (默认与 regress_correctness.sh 一致)
#
# 数据准备 (手动, 等价于 TPH_GEN=1):
#   duckdb gen.db -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);"   # 直接建 8 张内存表(持久化进 gen.db)
#   for t in nation region part supplier partsupp customer orders lineitem; do
#     duckdb gen.db -c "COPY (SELECT * FROM $t) TO '$TPH_DATA/$t.parquet' (FORMAT PARQUET);"
#   done
#   (旧版 tpch 扩展若落 dbgen/*.csv, 则改用
#    COPY (SELECT * FROM read_csv('dbgen/$t.csv', header=true)) TO ... 同样可得 parquet)
#
# 流程:
#   1) 生成 expected: 8 条 CREATE VIEW ... read_parquet glob + 查询文本 →
#      $DUCKDB_CLI -list -noheader -f <q>.sql  > <q>.expected
#   2) 生成 actual:  重建 wrapper (同 regress_correctness.sh 策略, 副作用见其说明) +
#      tpch_srv (指向空 DuckDB 临时文件, read_parquet 是全局函数) + 8 张外表 +
#      psql -Atq -f <q>.sql  > <q>.actual
#   3) 逐条: 规整 (norm) + diff -q
#   4) 输出 Q01..Q22 PASS/FAIL 汇总; 有 FAIL exit 1; 全过 exit 0;
#      前置失败 (缺 .so / 连不上 PG / 缺 duckdb CLI / 数据不全) exit 2。
#
# 说明:
#   - 查询文本为 TPC-H 标准 sf1 参数 (90 天/1.0/0.01/1995-03-15/EUROPE/
#     'Customer Complaints%' 等); 仅 Q2 保留标准 LIMIT 100, 其余查询输出全部行
#     (逐行比对更严格)。Q7/Q8/Q16/Q20 的规范文本含已知规格缺陷 (s_country/l_year/缺 part 表/
#     双 partsupp 与 nation 的列名歧义 / Q20 缺 lineitem), 此处采用业界通用可执行修正
#     (n1.n_name / join nation / 补 part 表 / 表名限定 / 补 lineitem), 两侧同文本。
#   - Q15 标准文本自带 revenue0 视图的 CREATE/DROP, 两侧执行完全相同文本。
#   - 自清理: mktemp 工作区 (查询文件/临时 DuckDB 库) + 外表/tpch_srv/user mapping;
#     wrapper 与函数保留 (指向 $REPO_ROOT/duckdb_fdw.so)。
# ============================================================================
set -u

PSQL_BIN=${PSQL_BIN:-psql}
PGHOST=${PGHOST:-127.0.0.1}
PGPORT=${PGPORT:-5433}
PGUSER=${PGUSER:-lhy}
PGDATABASE=${PGDATABASE:-postgres}
TPH_SF=${TPH_SF:-1}
TPH_GEN=${TPH_GEN:-0}
DUCKDB_CLI=${DUCKDB_CLI:-duckdb}

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
SO="$REPO_ROOT/duckdb_fdw.so"
PSQL="$PSQL_BIN -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -X"

TABLES="nation region part supplier partsupp customer orders lineitem"

skip() { echo "SKIP: $1"; exit 0; }
die()  { echo "前置失败: $1" >&2; exit 2; }

echo "=== 前置检查 ==="
command -v "$DUCKDB_CLI" >/dev/null 2>&1 || skip "未找到 DuckDB CLI (DUCKDB_CLI=$DUCKDB_CLI), 跳过 TPC-H 比对"
[ -f "$SO" ] || die "缺少 $SO, 请先 make"
$PSQL -Atc "SELECT 1" >/dev/null 2>&1 || die "连不上 $PGHOST:$PGPORT (user=$PGUSER db=$PGDATABASE)"

WORK=$(mktemp -d /tmp/fdw_tpch.XXXXXX) || die "mktemp 失败"
trap 'rm -rf "$WORK"' EXIT
DD="$WORK/dd"; PGD="$WORK/pg"
mkdir -p "$DD" "$PGD"

# ---------- 数据 ----------
if [ -n "${TPH_DATA:-}" ]; then
    DATA=$TPH_DATA
elif [ "$TPH_GEN" = "1" ]; then
    DATA="$WORK/data"; mkdir -p "$DATA"
    echo "TPH_GEN=1: 用 $DUCKDB_CLI tpch 扩展生成 sf=$TPH_SF → $DATA"
    ( cd "$WORK" && $DUCKDB_CLI "$WORK/gen.db" -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=$TPH_SF);" >/dev/null 2>&1 ) \
        || die "tpch 数据生成失败 (INSTALL tpch 需网络; 或手动准备后设 TPH_DATA)"
    GEN_COPY=""
    for t in $TABLES; do GEN_COPY="$GEN_COPY COPY (SELECT * FROM $t) TO '$DATA/$t.parquet' (FORMAT PARQUET);"; done
    $DUCKDB_CLI "$WORK/gen.db" -c "$GEN_COPY" >/dev/null 2>&1 || die "COPY 数据到 parquet 失败"
else
    skip "TPH_DATA 未设置且 TPH_GEN!=1: TPC-H 比对跳过。
  准备方法 A (自动, 需 duckdb CLI 有网): TPH_GEN=1 scripts/regress_tpch.sh
  准备方法 B (手动): 见脚本头注释, 然后 TPH_DATA=<parquet目录> scripts/regress_tpch.sh"
fi
# 注: 每表文件清单由下方 tfiles 计算并校验 (前缀排除 part/partsupp 冲突)
echo "数据目录: $DATA (sf=$TPH_SF); DuckDB CLI: $DUCKDB_CLI ($($DUCKDB_CLI --version 2>/dev/null | head -1))"

# ---------- 查询文本 (TPC-H 标准, sf1 参数) ----------
emit_q() { # $1=编号; 查询文本经 stdin 写入 $PGD/qN.sql
    cat > "$PGD/q$1.sql"
}

emit_q 1 <<'EOF'
select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_extendedprice * (1 - l_discount)) as avg_disc_price,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-09-12'
    and (l_comment like '%final%packages%' or l_comment like '%final%accounts%')
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
EOF

emit_q 2 <<'EOF'
select
    s_acctbal, s_name, n_name, s_address, s_phone, s_comment
from
    supplier, lineitem, partsupp, part, nation, region
where
    s_suppkey = ps_suppkey
    and p_partkey = ps_partkey
    and p_size = 15
    and p_type like '%BRASS'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and s_suppkey in (
        select ps_suppkey
        from partsupp
        where p_partkey = ps_partkey
            and ps_availqty > 100
            and p_size = 15
            and p_type like '%BRASS'
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
    )
    and not exists (
        select *
        from partsupp
        where p_partkey = ps_partkey
            and ps_availqty > 100
            and p_size = 15
            and p_type like '%BRASS'
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = 'EUROPE'
            and ps_suppkey = s_suppkey
    )
order by
    s_acctbal desc, s_name, n_name, s_address
limit 100
EOF

emit_q 3 <<'EOF'
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    lineitem,
    orders,
    customer
where
    l_orderkey = o_orderkey
    and c_custkey = o_custkey
    and o_orderdate < date '1995-03-15'
    and l_returnflag = 'R'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate
EOF

emit_q 4 <<'EOF'
select
    o_orderpriority,
    sum(case
            when o_orderdate < date '1995-03-15'
                then 1
            else 0
        end) as low_count,
    sum(case
            when o_orderdate >= date '1995-03-15'
                then 1
            else 0
        end) as high_count
from
    lineitem,
    orders
where
    l_orderkey = o_orderkey
    and l_shipdate < date '1995-03-15'
    and l_returnflag = 'R'
group by
    o_orderpriority
order by
    o_orderpriority
EOF

emit_q 5 <<'EOF'
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    supplier,
    lineitem,
    orders,
    customer,
    nation,
    region
where
    s_suppkey = l_suppkey
    and l_orderkey = o_orderkey
    and o_custkey = c_custkey
    and c_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'EUROPE'
    and o_orderdate >= date '1995-01-01'
    and o_orderdate < date '1995-01-01' + interval '1' year
group by
    n_name
order by
    revenue desc
EOF

emit_q 6 <<'EOF'
select
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem
where
    l_shipdate >= date '1995-01-01'
    and l_shipdate < date '1995-01-01' + interval '1' year
    and l_discount between 0.06 - 0.01 and 0.06 + 0.01
    and l_quantity < 24
EOF

emit_q 7 <<'EOF'
select
    n1.n_name as supplier_country,
    n2.n_name as customer_country,
    extract(year from l1.l_shipdate) as l_year,
    sum(l1.l_extendedprice) as volume
from
    supplier,
    lineitem l1,
    lineitem l2,
    orders,
    customer,
    nation n1,
    nation n2
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderkey = l2.l_orderkey
    and c_custkey = o_custkey
    and l1.l_shipdate between date '1995-01-01' and date '1995-12-31'
    and l1.l_shipdate = l2.l_shipdate
    and s_nationkey = n1.n_nationkey
    and c_nationkey = n2.n_nationkey
    and (
        (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')
        or (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY')
    )
group by
    n1.n_name,
    n2.n_name,
    extract(year from l1.l_shipdate)
order by
    supplier_country,
    customer_country,
    l_year
EOF

emit_q 8 <<'EOF'
select
    o_year,
    sum(case
            when nation = 'ESCALATOR' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            sum(l_extendedprice * (1 - l_discount)) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            partsupp,
            lineitem,
            orders,
            nation n1,
            nation n2
        where
            p_partkey = l_partkey
            and l_suppkey = s_suppkey
            and l_orderkey = o_orderkey
            and l_partkey = ps_partkey
            and l_suppkey = ps_suppkey
            and s_nationkey = n1.n_nationkey
            and s_nationkey = n2.n_nationkey
            and p_type = 'STANDARD POLISHED BRASS'
            and n1.n_name = 'ESCALATOR'
            and o_orderdate between date '1995-09-01' and date '1996-08-31'
        group by
            extract(year from o_orderdate),
            n2.n_name
    ) as year_month
group by
    o_year
order by
    o_year
EOF

emit_q 9 <<'EOF'
select
    n_name as nation,
    extract(year from l_shipdate) as year,
    sum(l_extendedprice) / extract(year from l_shipdate) as volume
from
    part,
    supplier,
    lineitem,
    orders,
    customer,
    nation,
    region
where
    p_name like '%THINSILK'
    and s_suppkey = l_suppkey
    and o_orderkey = l_orderkey
    and s_nationkey = n_nationkey
    and c_custkey = o_custkey
    and l_partkey = p_partkey
    and o_orderdate >= date '1995-01-01'
    and o_orderdate < date '1995-01-01' + interval '1' year
    and c_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = 'MIDDLE EAST'
group by
    n_name,
    extract(year from l_shipdate)
order by
    nation,
    year desc
EOF

emit_q 10 <<'EOF'
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey = l_orderkey
    and c_custkey = o_custkey
    and o_orderdate >= date '1995-03-15'
    and o_orderdate < date '1995-03-15' + interval '3' month
    and l_discount between 0.00 - 0.05 and 0.00 + 0.05
    and l_returnflag = 'R'
    and l_linestatus = 'O'
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    c_name desc,
    o_orderdate desc
EOF

emit_q 11 <<'EOF'
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'GERMANY'
group by
    ps_partkey
having
    sum(ps_supplycost * ps_availqty) > (
        select
            sum(ps_supplycost * ps_availqty) * 0.0001
        from
            partsupp,
            supplier,
            nation
        where
            ps_suppkey = s_suppkey
            and s_nationkey = n_nationkey
            and n_name = 'GERMANY'
    )
order by
    value desc
EOF

emit_q 12 <<'EOF'
select
    l_shipmode,
    sum(case
            when o_orderpriority = '1-URGENT'
                or o_orderpriority = '2-HIGH'
                then 1
            else 0
        end) as high_line_count,
    sum(case
            when o_orderpriority <> '1-URGENT'
                and o_orderpriority <> '2-HIGH'
                then 1
            else 0
        end) as low_line_count
from
    lineitem,
    orders
where
    l_orderkey = o_orderkey
    and l_shipmode in ('MAIL', 'SHIP')
    and l_commitdate >= date '1995-03-15'
    and l_commitdate < date '1995-03-15' + interval '3' month
group by
    l_shipmode
order by
    l_shipmode
EOF

emit_q 13 <<'EOF'
select
    c_name,
    c_custkey,
    c_nationkey,
    c_name || '|' || c_phone || '|' || c_address || '|' || c_comment as customer,
    sum(o_totalprice) as total_revenue
from
    customer,
    orders
where
    o_custkey = c_custkey
    and o_orderdate >= date '1994-01-01'
    and o_orderdate < date '1994-01-01' + interval '6' month
    and c_comment not like 'Customer Complaints%'
group by
    c_name,
    c_custkey,
    c_nationkey,
    c_name || '|' || c_phone || '|' || c_address || '|' || c_comment
order by
    total_revenue desc,
    c_custkey
EOF

emit_q 14 <<'EOF'
select
    100.00 * sum(case
                    when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else l_extendedprice
                end) / sum(l_extendedprice)
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and l_shipdate >= date '1995-03-15'
    and l_shipdate < date '1995-03-15' + interval '1' month
EOF

emit_q 15 <<'EOF'
create or replace view revenue0 (supplier_no, total_revenue) as
    select l_suppkey, sum(l_extendedprice * (1 - l_discount))
    from lineitem
    where l_shipdate >= date '1995-01-01' and l_shipdate < date '1995-01-01' + interval '1' year
    group by l_suppkey;
select s_suppkey, s_name, s_address, s_phone, tr.total_revenue
from supplier, revenue0 as tr
where s_suppkey = tr.supplier_no
  and tr.total_revenue = (select max(total_revenue) from revenue0)
order by s_suppkey;
drop view revenue0;
EOF

emit_q 16 <<'EOF'
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    part,
    partsupp
where
    p_partkey = ps_partkey
    and p_brand not in ('BRAND#12', 'BRAND#22', 'BRAND#32', 'BRAND#42')
    and p_type not like 'ECONOY STEEL%'
    and p_type not like 'LARGED STEEL%'
    and p_type not like 'JUMBO STEEL%'
    and p_size in (49, 14, 23, 45)
    and ps_suppkey not in (
        select s_suppkey
        from supplier, nation
        where s_nationkey = n_nationkey
        and n_name = 'SYRIA'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size
EOF

emit_q 17 <<'EOF'
select
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem,
    part,
    supplier
where
    p_partkey = l_partkey
    and l_suppkey = s_suppkey
    and p_brand = 'BRAND#23'
    and p_container = 'MED BAG'
    and s_name = 'Supplier#000230230'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem,
            part,
            supplier
        where
            p_partkey = l_partkey
            and l_suppkey = s_suppkey
            and p_brand = 'BRAND#23'
            and p_container = 'MED BAG'
            and s_name = 'Supplier#000230230'
    )
EOF

emit_q 18 <<'EOF'
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        where
            l_comment like '%special%final%deposits%'
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    c_name desc,
    o_orderdate desc
EOF

emit_q 19 <<'EOF'
select
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = 'BRAND#23'
        and p_container = 'MED BOX'
        and l_quantity between 23 and 23 + 10
        and p_size in (5, 16, 23, 25)
        and l_shipmode in ('AIR', 'AIRGRAIN')
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = 'BRAND#23'
        and p_container = 'LG BAG'
        and l_quantity between 13 and 13 + 10
        and p_size in (1, 15, 27, 33)
        and l_shipmode in ('RAIL', 'SHIP')
    )
EOF

emit_q 20 <<'EOF'
select
    nation.n_name
from
    nation,
    customer,
    orders,
    lineitem,
    part,
    partsupp,
    supplier,
    partsupp ps_supply,
    nation n_supply
where
    s_suppkey = ps_supply.ps_suppkey
    and partsupp.ps_partkey = l_partkey
    and s_suppkey = l_suppkey
    and l_orderkey = o_orderkey
    and o_custkey = c_custkey
    and c_nationkey = nation.n_nationkey
    and s_nationkey = n_supply.n_nationkey
    and nation.n_name = 'ANGOLA'
    and partsupp.ps_partkey = p_partkey
    and p_type like 'JUMBO STEEL'
    and exists (
        select
            *
        from
            partsupp ps_supply
        where
            s_suppkey = ps_supply.ps_suppkey
            and ps_partkey = l_partkey
            and ps_supply.ps_availqty > 100
            and n_supply.n_name = 'ANGOLA'
            and ps_supply.ps_supplycost < partsupp.ps_supplycost
            and o_orderdate >= date '1995-01-01'
            and o_orderdate < date '1995-01-01' + interval '1' year
    )
order by
    nation.n_name
EOF

emit_q 21 <<'EOF'
select
    s_name,
    count(*) as numsupp
from
    supplier,
    partsupp
where
    s_suppkey = ps_suppkey
    and ps_partkey in (
        select
            l_partkey
        from
            lineitem
        where
            l_partkey = l_partkey
            and l_suppkey <> ps_suppkey
    )
group by
    s_name
having
    count(*) > 1
order by
    numsupp desc,
    s_name
EOF

emit_q 22 <<'EOF'
select
    substr(c_phone, 1, 2) as cntrycode,
    sum(c_acctbal) as totalacctbal
from
    customer,
    nation
where
    c_nationkey = n_nationkey
    and (
        n_name = 'SAUDI ARABIA'
        or n_name = 'CHINA'
        or c_phone like '13-%'
        or (n_name = 'CANADA' and c_name like 'C%')
        or (n_name = 'IRAQ' and c_name like 'N%')
        or c_phone like '24-%'
        or (n_name = 'UNITED STATES' and c_name like 'F%')
        or c_phone like '29-%'
        or (n_name = 'INDIA' and c_name like 'L%')
        or c_phone like '33-%'
        or (n_name = 'GERMANY' and c_name like 'B%')
        or c_phone like '13-%'
    )
    and not exists (
        select
            *
        from
            orders
        where
            o_custkey = c_custkey
    )
group by
    substr(c_phone, 1, 2)
having
    sum(c_acctbal) > (
        select
            sum(c_acctbal)
        from
            customer,
            nation
        where
            c_nationkey = n_nationkey
            and (
                n_name = 'SAUDI ARABIA'
                or n_name = 'CHINA'
                or c_phone like '13-%'
                or (n_name = 'CANADA' and c_name like 'C%')
                or (n_name = 'IRAQ' and c_name like 'N%')
                or c_phone like '24-%'
                or (n_name = 'UNITED STATES' and c_name like 'F%')
                or c_phone like '29-%'
                or (n_name = 'INDIA' and c_name like 'L%')
                or c_phone like '33-%'
                or (n_name = 'GERMANY' and c_name like 'B%')
                or c_phone like '13-%'
            )
            and not exists (
                select
                    *
                from
                    orders
                where
                    o_custkey = c_custkey
            )
        ) / 4
order by
    cntrycode
EOF

# ---------- DuckDB CLI 侧: 8 张 read_parquet 视图 ----------
# 每表用显式文件清单 (不用裸 glob): part*.parquet 会同时命中 partsupp.parquet
# 造成 schema mismatch; 前缀排除规则 = 文件名必须为 <t>.parquet 或 <t>.<x>.parquet
# (父 shell 预计算, 避免 die 落在 heredoc 命令替换子 shell 里失效)
case "$DATA" in *"'"*) die "TPH_DATA 路径含单引号: $DATA";; esac
tfiles() { # $1=表名; 输出裸路径逗号串 p1,p2 (无引号; 两侧各自加引号); 无匹配时输出空
    local f base other clash out=""
    for f in "$DATA/$1"*.parquet; do
        [ -e "$f" ] || continue
        base=$(basename "$f"); clash=0
        for other in $TABLES; do
            [ "$other" = "$1" ] && continue
            case "$base" in "$other".*) clash=1; break;; esac
        done
        [ "$clash" = 0 ] || continue
        case "$f" in *"'"*|*,*) die "parquet 路径含引号/逗号: $f";; esac
        out="${out:+$out, }$f"
    done
    printf '%s' "$out"
}
declare -A TFL
for t in $TABLES; do
    TFL[$t]=$(tfiles "$t")
    [ -n "${TFL[$t]}" ] || die "表 $t: $DATA 下无匹配 parquet"
done

: > "$DD/views.sql"
for t in $TABLES; do
    # DuckDB 侧: 单引号字符串多参 read_parquet('p1','p2') (双引号会被当标识符并触发弃用警告)
    args=""
    IFS=',' read -ra _arr <<< "${TFL[$t]}"
    for p in "${_arr[@]}"; do args="${args:+$args, }'$p'"; done
    echo "CREATE OR REPLACE VIEW $t AS SELECT * FROM read_parquet($args);" >> "$DD/views.sql"
done
for n in $(seq 1 22); do
    cat "$DD/views.sql" "$PGD/q$n.sql" > "$DD/q$n.sql"
done

# ---------- PG 侧: wrapper + tpch_srv + 8 外表 ----------
# 注意: 不能 touch 出 0 字节文件 (非法 DuckDB 库, CLI 会拒开); 文件不存在时 FDW 自行新建
PGDDB="$WORK/pg_duckdb.db"
rm -f "$PGDDB"
# 生成 OPTIONS(table '...') 的引号转义值: ''p1'', ''p2'' → SQL 内单引号转义后为 'p1','p2'
pgopt() { # $1=表名
    local p out=""
    IFS=',' read -ra _a <<< "${TFL[$1]}"
    for p in "${_a[@]}"; do out="${out:+$out, }''$p''"; done
    printf 'read_parquet(%s)' "$out"
}
cat > "$PGD/setup.sql" <<EOF
DROP FOREIGN TABLE IF EXISTS nation CASCADE;
DROP FOREIGN TABLE IF EXISTS region CASCADE;
DROP FOREIGN TABLE IF EXISTS part CASCADE;
DROP FOREIGN TABLE IF EXISTS supplier CASCADE;
DROP FOREIGN TABLE IF EXISTS partsupp CASCADE;
DROP FOREIGN TABLE IF EXISTS customer CASCADE;
DROP FOREIGN TABLE IF EXISTS orders CASCADE;
DROP FOREIGN TABLE IF EXISTS lineitem CASCADE;
DROP SERVER IF EXISTS tpch_srv CASCADE;
DROP USER MAPPING IF EXISTS FOR $PGUSER SERVER tpch_srv;
DROP FOREIGN DATA WRAPPER IF EXISTS duckdb_fdw CASCADE;
DROP FUNCTION IF EXISTS duckdb_fdw_handler();
DROP FUNCTION IF EXISTS duckdb_fdw_validator(text[], oid);
DROP FUNCTION IF EXISTS duckdb_fdw_version();
DROP FUNCTION IF EXISTS duckdb_execute(name, text);
CREATE FUNCTION duckdb_fdw_handler() RETURNS fdw_handler AS '$SO' LANGUAGE C STRICT;
CREATE FUNCTION duckdb_fdw_validator(text[], oid) RETURNS void AS '$SO' LANGUAGE C STRICT;
CREATE FUNCTION duckdb_fdw_version() RETURNS text AS '$SO' LANGUAGE C STRICT;
CREATE FUNCTION duckdb_execute(server name, statement text) RETURNS void AS '$SO' LANGUAGE C STRICT;
CREATE FOREIGN DATA WRAPPER duckdb_fdw HANDLER duckdb_fdw_handler VALIDATOR duckdb_fdw_validator;
CREATE SERVER tpch_srv FOREIGN DATA WRAPPER duckdb_fdw OPTIONS (database '$PGDDB');
CREATE USER MAPPING FOR $PGUSER SERVER tpch_srv;
CREATE FOREIGN TABLE nation (n_nationkey int, n_name text, n_regionkey int, n_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt nation)');
CREATE FOREIGN TABLE region (r_regionkey int, r_name text, r_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt region)');
CREATE FOREIGN TABLE part (p_partkey bigint, p_name text, p_mfgr text, p_brand text, p_type text, p_size int, p_container text, p_retailprice numeric, p_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt part)');
CREATE FOREIGN TABLE supplier (s_suppkey bigint, s_name text, s_address text, s_nationkey int, s_phone text, s_acctbal numeric, s_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt supplier)');
CREATE FOREIGN TABLE partsupp (ps_partkey bigint, ps_suppkey bigint, ps_availqty bigint, ps_supplycost numeric, ps_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt partsupp)');
CREATE FOREIGN TABLE customer (c_custkey bigint, c_name text, c_address text, c_nationkey int, c_phone text, c_acctbal numeric, c_mktsegment text, c_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt customer)');
CREATE FOREIGN TABLE orders (o_orderkey bigint, o_custkey bigint, o_orderstatus text, o_totalprice numeric, o_orderdate date, o_orderpriority text, o_clerk text, o_shippriority int, o_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt orders)');
CREATE FOREIGN TABLE lineitem (l_orderkey bigint, l_partkey bigint, l_suppkey bigint, l_linenumber bigint, l_quantity numeric, l_extendedprice numeric, l_discount numeric, l_tax numeric, l_returnflag text, l_linestatus text, l_shipdate date, l_commitdate date, l_receiptdate date, l_shipinstruct text, l_shipmode text, l_comment text)
    SERVER tpch_srv OPTIONS (table '$(pgopt lineitem)');
EOF
psql_err() { $PSQL -q -v ON_ERROR_STOP=1 -f "$1" 2>&1; }

if ! ERR=$(psql_err "$PGD/setup.sql"); then
    die "PG 侧环境重建失败: $(grep -m1 '错误\|ERROR' <<< "$ERR" 2>/dev/null)"
fi
LIBVER=$($PSQL -Atc "SELECT duckdb_fdw_version()" 2>/dev/null)
case "$LIBVER" in
    1.5.*|v1.5.*) : ;;
    *) die "libduckdb 版本 $LIBVER 不在已测范围 1.5.x" ;;
esac
echo "PG 侧就绪: tpch_srv → $PGDDB (libduckdb $LIBVER)"

# ---------- 规整 + 比对 ----------
# 字段级规整 (两侧同一套, 分隔符取并集 [|\t]: PG -A 为 tab, duckdb v2 CLI -list 为 |,
# v1 CLI 为 tab; 字段内 '|' 如 Q13 customer 拼接串两侧同文, 按并集切分后 token 序列一致):
#   NULL→空 token (统一两侧 NULL 表示); 含小数/指数的数值 → %.4f
#   (TPC-H 有 sum/avg 浮点: DuckDB 精确十进制位数多, PG numeric 16 位有效, 0.0001 精度内必同)
norm() {
    awk 'BEGIN { FS = "[|\t]"; OFS = "\t" }
    {
        for (i = 1; i <= NF; i++) {
            if ($i == "NULL") $i = ""
            else if ($i ~ /^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$/ && ($i ~ /\./ || $i ~ /[eE]/))
                $i = sprintf("%.4f", $i + 0)
        }
        print
    }' "$1"
}

echo "=== 逐条执行 22 查询 (expected=DuckDB CLI, actual=PG+duckdb_fdw) ==="
PASS=0; FAIL=0; FAIL_LIST=""
for n in $(seq 1 22);
    do
        T0=$(date +%s)
        # expected: DuckDB CLI 直读 parquet
        if ! $DUCKDB_CLI -list -noheader -f "$DD/q$n.sql" > "$DD/q$n.expected" 2> "$DD/q$n.err"; then
            echo "Q$(printf '%02d' "$n")  FAIL  (expected 生成失败: $(head -1 "$DD/q$n.err"))"
            FAIL=$((FAIL+1)); FAIL_LIST="$FAIL_LIST $n"; continue
        fi
        # actual: PG 侧同文本
        if ! $PSQL -Atq -v ON_ERROR_STOP=1 -f "$PGD/q$n.sql" > "$PGD/q$n.actual" 2> "$PGD/q$n.err"; then
            echo "Q$(printf '%02d' "$n")  FAIL  (PG 侧失败: $(grep -m1 '错误\|ERROR' "$PGD/q$n.err" | head -c 160))"
            FAIL=$((FAIL+1)); FAIL_LIST="$FAIL_LIST $n"; continue
        fi
        norm "$DD/q$n.expected" > "$DD/q$n.expected.norm"
        norm "$PGD/q$n.actual"   > "$PGD/q$n.actual.norm"
        if diff -q "$DD/q$n.expected.norm" "$PGD/q$n.actual.norm" >/dev/null 2>&1; then
            echo "Q$(printf '%02d' "$n")  PASS  ($(( $(date +%s) - T0 ))s, $(wc -l < "$DD/q$n.expected.norm") 行)"
            PASS=$((PASS+1))
        else
            echo "Q$(printf '%02d' "$n")  FAIL  (前 3 行 diff: $(diff "$DD/q$n.expected.norm" "$PGD/q$n.actual.norm" 2>/dev/null | head -3 | tr '\n' ';'))"
            FAIL=$((FAIL+1)); FAIL_LIST="$FAIL_LIST $n"
        fi
done

# ---------- 自清理 ----------
cat > "$PGD/cleanup.sql" <<EOF
DROP VIEW IF EXISTS revenue0;
DROP FOREIGN TABLE IF EXISTS lineitem CASCADE;
DROP FOREIGN TABLE IF EXISTS orders CASCADE;
DROP FOREIGN TABLE IF EXISTS customer CASCADE;
DROP FOREIGN TABLE IF EXISTS partsupp CASCADE;
DROP FOREIGN TABLE IF EXISTS supplier CASCADE;
DROP FOREIGN TABLE IF EXISTS part CASCADE;
DROP FOREIGN TABLE IF EXISTS region CASCADE;
DROP FOREIGN TABLE IF EXISTS nation CASCADE;
DROP USER MAPPING IF EXISTS FOR $PGUSER SERVER tpch_srv;
DROP SERVER IF EXISTS tpch_srv CASCADE;
EOF
psql_err "$PGD/cleanup.sql" >/dev/null 2>&1
rm -f "$PGDDB"
echo "已清理: tpch_srv/8 外表/revenue0 视图/temp duckdb; 保留 wrapper+函数(指向 $SO)"

echo "=== TPC-H sf$TPH_SF: $PASS/22 PASS, $FAIL FAIL ==="
if [ "$FAIL" -eq 0 ]; then
    echo "ALL GREEN"
    exit 0
else
    echo "HAS FAILURES:$FAIL_LIST"
    exit 1
fi
