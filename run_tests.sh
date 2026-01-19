#!/bin/bash

# 🎨 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 参数解析
VERBOSE=false
if [[ "$1" == "-v" || "$1" == "--verbose" ]]; then
    VERBOSE=true
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

# 1. 编译
echo -e "\n${BLUE}[1/3] 编译并安装插件...${NC}"
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
echo -e "${GREEN}编译完成。${NC}"

# 2. 定义测试文件
TEST_FILES=(
    "examples/01_quick_start.sql"
    "examples/01_full_demo.sql"
    "examples/02_parquet_scan.sql"
    "examples/02_complex_types.sql"
    "examples/03_vector_support.sql"
    "examples/05_cloud_secret.sql"
    "examples/06_import_schema.sql"
    "examples/07_iceberg_lakehouse.sql"
    "examples/08_ducklake_attach.sql"
    "examples/09_ducklake_import.sql"
    "examples/10_iceberg_direct_scan.sql"
    "examples/11_sf3_analytics.sql"
    "examples/12_s3_tables_direct.sql"
    "examples/13_all_datasets_test.sql"
    "examples/14_arrow_vectorized_types.sql"
    "examples/15_high_performance_insert.sql"
    "examples/16_aggregate_pushdown.sql"
    "examples/17_join_pushdown_test.sql"
    "examples/18_high_performance_v2.sql"
)

# 2.5 加载本地环境变量 (.env)
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
    echo -e "${YELLOW}已加载本地 .env 凭证进行测试。${NC}"
fi

# 3. 运行测试
echo -e "\n${BLUE}[2/3] 开始执行测试脚本...${NC}"
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
echo -e "${BLUE}[3/3] 测试汇总${NC}"
echo "----------------------------------------------------"
if [ $SUCCESS_COUNT -eq $TOTAL_COUNT ]; then
    echo -e "${GREEN}全部测试通过! ($SUCCESS_COUNT/$TOTAL_COUNT)${NC}"
else
    echo -e "${RED}部分测试失败 ($SUCCESS_COUNT/$TOTAL_COUNT)。${NC}"
    exit 1
fi
echo "----------------------------------------------------"
