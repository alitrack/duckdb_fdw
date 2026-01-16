#!/bin/bash

# 🎨 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}====================================================${NC}"
echo -e "${BLUE}      pg_duck (DuckDB FDW) 自动化集成测试脚本        ${NC}"
echo -e "${BLUE}====================================================${NC}"

# 1. 编译
echo -e "\n${BLUE}[1/3] 编译并安装插件...${NC}"
make USE_PGXS=1 clean > /dev/null 2>&1
make USE_PGXS=1 -j$(nproc) > /dev/null 2>&1
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
)

# 3. 运行测试
echo -e "\n${BLUE}[2/3] 开始执行测试脚本...${NC}"
SUCCESS_COUNT=0
TOTAL_COUNT=${#TEST_FILES[@]}

for f in "${TEST_FILES[@]}"; do
    printf "运行 %-40s ... " "$f" 
    # 执行 psql 命令，捕获 stderr 以便分析错误原因
    ERROR_MSG=$(/usr/lib/postgresql/15/bin/psql -p 5433 -h /tmp -d postgres -f "$f" 2>&1 > /dev/null)
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}[OK]${NC}"
        ((SUCCESS_COUNT++))
    else
        # 智能识别错误原因
        if [[ "$ERROR_MSG" == *"Catalog Error"* || "$ERROR_MSG" == *"IO Error"* || "$ERROR_MSG" == *"Network"* ]]; then
            echo -e "${RED}[FAILED]${NC} (可能是网络或凭证问题)"
        else
            echo -e "${RED}[FAILED]${NC} (逻辑错误)"
            echo "错误细节: $ERROR_MSG"
        fi
    fi
done

# 4. 汇总
echo -e "\n${BLUE}[3/3] 测试汇总${NC}"
echo "----------------------------------------------------"
if [ $SUCCESS_COUNT -eq $TOTAL_COUNT ]; then
    echo -e "${GREEN}全部测试通过! ($SUCCESS_COUNT/$TOTAL_COUNT)${NC}"
else
    echo -e "${RED}部分测试失败 ($SUCCESS_COUNT/$TOTAL_COUNT)。${NC}"
    echo -e "提示: 涉及 S3/Iceberg 的测试需要网络环境支持。本地功能 (01,02,03,06) 必须为 OK。"
fi
echo "----------------------------------------------------"
