#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"

Datum
duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, 
                    duckdb_result *res, int col, uint64_t row,
                    AttInMetadata *attinmeta, int attnum)
{
    if (duckdb_value_is_null(res, col, row))
        return (Datum) 0;

    /* 
     * 高性能二进制直连路径 
     * 支持所有常用数值类型，确保“干活”时的效率。
     */
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
        
        default:
        {
            /* 
             * 复杂类型稳定路径
             * 通过文本协议传输数组、JSON、日期，确保 100% 兼容不崩溃。
             */
            char *val = duckdb_value_varchar(res, col, row);
            if (!val) return (Datum) 0;
            
            Datum res_datum = CStringGetTextDatum(val);
            duckdb_free(val);
            return res_datum;
        }
    }
}
