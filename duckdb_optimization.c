#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/lsyscache.h"

Datum
duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, 
                    duckdb_result *res, int col, uint64_t row,
                    AttInMetadata *attinmeta, int attnum)
{
    if (duckdb_value_is_null(res, col, row))
        return (Datum) 0;

    /* 1. 基础数值直连路径 */
    switch (pgtyp)
    {
        case INT4OID:
            return Int32GetDatum(duckdb_value_int32(res, col, row));
        case INT8OID:
            return Int64GetDatum(duckdb_value_int64(res, col, row));
        case FLOAT8OID:
            return Float8GetDatum(duckdb_value_double(res, col, row));
        case BOOLOID:
            return BoolGetDatum(duckdb_value_boolean(res, col, row));
    }

    /* 2. 文本转换路径 */
    char *raw_val = duckdb_value_varchar(res, col, row);
    if (!raw_val) return (Datum) 0;

    /* 拷贝到内存上下文 */
    char *val = pstrdup(raw_val);
    duckdb_free(raw_val);

    size_t len = strlen(val);

    /* 
     * 3. 关键逻辑区分：
     * - 如果目标是 PG 数组 (LIST -> ARRAY)，转换 [1,2] 为 {1,2}
     * - 如果目标是 pgvector (ARRAY -> vector)，保持 [1,2] 格式
     */
    bool is_pg_native_array = false;
    
    /* 检查常见数组类型的 OID */
    if (pgtyp == 1007 || pgtyp == 1009 || pgtyp == 1021 || pgtyp == 1022)
        is_pg_native_array = true;
    else
    {
        /* 进一步检查类型名称，判断是否以 '_' 开头（PG 数组的内部命名习惯） */
        char *typname = get_type_name(pgtyp);
        if (typname && typname[0] == '_')
            is_pg_native_array = true;
    }

    if (is_pg_native_array && len >= 2 && val[0] == '[' && val[len-1] == ']')
    {
        val[0] = '{';
        val[len-1] = '}';
    }

    /* 4. 安全执行转换 */
    Datum result = (Datum) 0;
    PG_TRY();
    {
        Oid typinput, typioparam;
        getTypeInputInfo(pgtyp, &typinput, &typioparam);
        result = OidInputFunctionCall(typinput, val, typioparam, pgtypmod);
    }
    PG_CATCH();
    {
        pfree(val);
        PG_RE_THROW();
    }
    PG_END_TRY();

    pfree(val);
    return result;
}
