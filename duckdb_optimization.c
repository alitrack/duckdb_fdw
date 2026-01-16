/*
 * duckdb_optimization.c - 数据转换增强模块
 */

#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"
#include "utils/builtins.h"

/*
 * 将 DuckDB 的数据类型转换为 PostgreSQL 的 Datum
 */
Datum
duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, 
                    duckdb_result *res, int col, uint64_t row,
                    AttInMetadata *attinmeta)
{
    /* 1. 处理 NULL 值 */
    if (duckdb_value_is_null(res, col, row))
        return (Datum) 0;

    /* 2. 根据目标 Postgres 类型进行转换 */
    switch (pgtyp)
    {
        case INT4OID:
        {
            int32_t val = duckdb_value_int32(res, col, row);
            elog(DEBUG1, "duckdb_fdw: converting col %d row %lu, val = %d", col, (unsigned long)row, val);
            return Int32GetDatum(val);
        }
        case INT8OID:
        {
            int64_t val = duckdb_value_int64(res, col, row);
            elog(DEBUG1, "duckdb_fdw: converting col %d row %lu, val = %ld", col, (unsigned long)row, (long)val);
            return Int64GetDatum(val);
        }
        case FLOAT8OID:
            return Float8GetDatum(duckdb_value_double(res, col, row));
        case TEXTOID:
        case VARCHAROID:
        {
            char *val = duckdb_value_varchar(res, col, row);
            Datum result = PointerGetDatum(cstring_to_text(val));
            duckdb_free(val);
            return result;
        }
        /* 
         * 未来在这里增加对 GEOMETRY (Path B) 
         * 和复杂类型的支持
         */
        default:
            elog(ERROR, "duckdb_fdw: unsupported type %u", pgtyp);
            return (Datum) 0;
    }
}
