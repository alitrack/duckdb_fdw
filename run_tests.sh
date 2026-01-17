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
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}

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

make USE_PGXS=1 clean > /dev/null 2>&1
make USE_PGXS=1 -j$NPROC > /dev/null 2>&1
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
    
    # 创建临时测试文件，替换占位符
    TEMP_SQL=$(mktemp)
    cp "$f" "$TEMP_SQL"
    
    # Path replacement: Handle specific hardcoded path in examples
    # Use perl for cross-platform replacement (sed varies on Mac/Linux)
    perl -pi -e "s|/home/coder/workspace/pg_duck|$CURRENT_DIR|g" "$TEMP_SQL"

    if [ ! -z "$S3_ACCESS_KEY" ]; then
        perl -pi -e "s/YOUR_ACCESS_KEY/$S3_ACCESS_KEY/g" "$TEMP_SQL"
        perl -pi -e "s|YOUR_SECRET_KEY|$S3_SECRET_KEY|g" "$TEMP_SQL"
    fi

    CMD="$PSQL_BIN -p $PGPORT -h $PGHOST -d $PGDATABASE -U $PGUSER -f $TEMP_SQL"

    if [ "$VERBOSE" = true ]; then
        # 详细模式
        $CMD
        RET=$?
    else
        # 静默模式
        ERROR_MSG=$($CMD 2>&1 > /dev/null)
        RET=$?
    fi
    
    rm -f "$TEMP_SQL"
    
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
