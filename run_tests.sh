#!/bin/bash

# 🎨 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 参数解析
VERBOSE=false
PROFILE=${PROFILE:-core}
RUN_PG_DUCKDB_COEXISTENCE_CHECK=${RUN_PG_DUCKDB_COEXISTENCE_CHECK:-0}
TOTAL_STEPS=3
while [[ $# -gt 0 ]]; do
    case "$1" in
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -p|--profile)
            PROFILE="$2"
            shift 2
            ;;
        *)
            echo "未知参数: $1"
            echo "用法: $0 [--verbose] [--profile core|integration|cloud|all]"
            exit 1
            ;;
    esac
done

if [[ "${RUN_PG_DUCKDB_COEXISTENCE_CHECK}" == "1" ]]; then
    TOTAL_STEPS=4
fi

# Environment Configuration
PSQL_BIN=${PSQL_BIN:-psql}
PGPORT=${PGPORT:-5433}
PGHOST=${PGHOST:-/tmp}
PGUSER=${PGUSER:-$(whoami)}
PGDATABASE=${PGDATABASE:-$(whoami)}

echo -e "${BLUE}====================================================${NC}"
echo -e "${BLUE}      pg_duck (DuckDB FDW) 自动化集成测试脚本        ${NC}"
echo -e "${BLUE}====================================================${NC}"

if [ "$VERBOSE" = true ]; then
    echo -e "${YELLOW}模式: 详细输出 (Verbose)${NC}"
else
    echo -e "${YELLOW}模式: 静默执行 (使用 -v 开启详细输出)${NC}"
fi
echo -e "${YELLOW}测试档位: ${PROFILE}${NC}"
if [[ "${RUN_PG_DUCKDB_COEXISTENCE_CHECK}" == "1" ]]; then
    echo -e "${YELLOW}附加验证: 启用 pg_duckdb 共存守卫检查${NC}"
fi

# 1. 编译
echo -e "\n${BLUE}[1/${TOTAL_STEPS}] 编译并安装插件...${NC}"
# Use nproc if available, else sysctl (macOS), else 1
NPROC=1
if command -v nproc > /dev/null; then
    NPROC=$(nproc)
elif [ "$(uname)" == "Darwin" ]; then
    NPROC=$(sysctl -n hw.ncpu)
fi

make clean > /dev/null 2>&1
make -j$NPROC > /dev/null 2>&1
if [ $? -ne 0 ]; then
    echo -e "${RED}编译失败！请检查编译日志。${NC}"
    exit 1
fi
if ! make install > /dev/null 2>&1; then
    if command -v sudo > /dev/null && sudo -n make install > /dev/null 2>&1; then
        :
    else
        echo -e "${RED}安装 duckdb_fdw 失败。请先执行: sudo make install${NC}"
        exit 1
    fi
fi
echo -e "${GREEN}编译完成。${NC}"

# 2. 定义测试文件（分层）
CORE_TEST_FILES=(
    "examples/01_quick_start.sql"
    "examples/01_full_demo.sql"
    "examples/02_parquet_scan.sql"
    "examples/02_complex_types.sql"
    "examples/03_vector_support.sql"
    "examples/06_import_schema.sql"
    "examples/14_arrow_vectorized_types.sql"
    "examples/15_high_performance_insert.sql"
    "examples/16_aggregate_pushdown.sql"
    "examples/17_join_pushdown_test.sql"
    "examples/18_high_performance_v2.sql"
)

INTEGRATION_TEST_FILES=(
    "examples/07_iceberg_lakehouse.sql"
    "examples/08_ducklake_attach.sql"
    "examples/09_ducklake_import.sql"
    "examples/10_iceberg_direct_scan.sql"
    "examples/11_sf3_analytics.sql"
)

CLOUD_TEST_FILES=(
    "examples/05_cloud_secret.sql"
    "examples/12_s3_tables_direct.sql"
    "examples/13_all_datasets_test.sql"
)

TEST_FILES=()
case "$PROFILE" in
    core)
        TEST_FILES=("${CORE_TEST_FILES[@]}")
        ;;
    integration)
        TEST_FILES=("${INTEGRATION_TEST_FILES[@]}")
        ;;
    cloud)
        TEST_FILES=("${CLOUD_TEST_FILES[@]}")
        ;;
    all)
        TEST_FILES=("${CORE_TEST_FILES[@]}" "${INTEGRATION_TEST_FILES[@]}" "${CLOUD_TEST_FILES[@]}")
        ;;
    *)
        echo -e "${RED}无效测试档位: ${PROFILE} (可选: core|integration|cloud|all)${NC}"
        exit 1
        ;;
esac

# 2.2 可选依赖检查（例如 pgvector）
if [[ "$PROFILE" == "core" || "$PROFILE" == "all" ]]; then
    PG_SHAREDIR=$(pg_config --sharedir 2>/dev/null)
    VECTOR_CONTROL_FILE="$PG_SHAREDIR/extension/vector.control"
    if [[ ! -f "$VECTOR_CONTROL_FILE" ]]; then
        echo -e "${YELLOW}未检测到 pgvector，跳过 examples/03_vector_support.sql${NC}"
        FILTERED_TEST_FILES=()
        for f in "${TEST_FILES[@]}"; do
            if [[ "$f" != "examples/03_vector_support.sql" ]]; then
                FILTERED_TEST_FILES+=("$f")
            fi
        done
        TEST_FILES=("${FILTERED_TEST_FILES[@]}")
    fi
fi

# 2.5 加载本地环境变量 (.env)
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    echo -e "${YELLOW}已加载本地 .env 凭证进行测试。${NC}"
fi

if [[ "$PROFILE" == "cloud" || "$PROFILE" == "all" ]]; then
    if [[ -z "${S3_ACCESS_KEY:-}" || -z "${S3_SECRET_KEY:-}" ]]; then
        echo -e "${RED}cloud 档位需要 S3_ACCESS_KEY/S3_SECRET_KEY 环境变量。${NC}"
        exit 1
    fi
fi

# 3. 运行测试
echo -e "\n${BLUE}[2/${TOTAL_STEPS}] 开始执行测试脚本...${NC}"
SUCCESS_COUNT=0
TOTAL_COUNT=${#TEST_FILES[@]}

# Current directory for path replacement
CURRENT_DIR=$(pwd)

for f in "${TEST_FILES[@]}"; do
    echo -e "${BLUE}>>> 正在运行: $f${NC}"
    
    # 1. 构造 sed 替换序列
    # 使用 @ 作为分隔符，避免路径中的 / 冲突
    SED_EXPR="s@\\@PROJECT_PATH@$CURRENT_DIR@g"
    if [ ! -z "$S3_ACCESS_KEY" ]; then
        SED_EXPR="$SED_EXPR; s@YOUR_ACCESS_KEY@$S3_ACCESS_KEY@g; s@YOUR_SECRET_KEY@$S3_SECRET_KEY@g"
    fi

    # 2. 直接通过管道运行，不再产生临时文件
    # 使用 -f - 让 psql 从标准输入读取
    CMD_BASE="$PSQL_BIN -v ON_ERROR_STOP=1 -p $PGPORT -h $PGHOST -d $PGDATABASE -U $PGUSER -f -"

    if [ "$VERBOSE" = true ]; then
        sed "$SED_EXPR" "$f" | $CMD_BASE
        RET=$?
    else
        ERROR_MSG=$(sed "$SED_EXPR" "$f" | $CMD_BASE 2>&1 > /dev/null)
        RET=$?
    fi
    
    if [ $RET -eq 0 ]; then
        echo -e "${GREEN}[OK] $f${NC}\n"
        ((SUCCESS_COUNT++))
    else
        echo -e "${RED}[FAILED] $f${NC}"
        if [ "$VERBOSE" = false ]; then
            echo "错误细节: $ERROR_MSG"
        fi
        echo -e ""
    fi
done

# 4. 汇总
if [[ "${RUN_PG_DUCKDB_COEXISTENCE_CHECK}" == "1" ]]; then
    echo -e "\n${BLUE}[3/4] 运行 pg_duckdb 共存守卫验证...${NC}"
    if ./scripts/verify_pg_duckdb_coexistence.sh; then
        echo -e "${GREEN}共存守卫验证通过。${NC}"
    else
        echo -e "${RED}共存守卫验证失败。${NC}"
        exit 1
    fi
fi

SUMMARY_STEP="[3/${TOTAL_STEPS}]"
if [[ "${RUN_PG_DUCKDB_COEXISTENCE_CHECK}" == "1" ]]; then
    SUMMARY_STEP="[4/4]"
fi

echo -e "${BLUE}${SUMMARY_STEP} 测试汇总${NC}"
echo "----------------------------------------------------"
if [ $SUCCESS_COUNT -eq $TOTAL_COUNT ]; then
    echo -e "${GREEN}全部测试通过! ($SUCCESS_COUNT/$TOTAL_COUNT)${NC}"
else
    echo -e "${RED}部分测试失败 (通过 $SUCCESS_COUNT/$TOTAL_COUNT)。${NC}"
    exit 1
fi
echo "----------------------------------------------------"
