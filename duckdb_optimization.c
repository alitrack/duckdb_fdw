#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"

/*
 * Extract DuckDB value as a safe C-string compatible with Postgres input functions.
 * Returns NULL if the value is NULL.
 * Returns a palloc'd string otherwise.
 */
char *
duckdb_extract_as_cstring(duckdb_result *res, int col, uint64_t row, Oid pgtyp)
{
    if (duckdb_value_is_null(res, col, row))
        return NULL;

    /* 
     * Since we force CAST(... AS VARCHAR) in deparse.c for complex types,
     * duckdb_value_varchar should always return a valid string here.
     */
    char *raw_val = duckdb_value_varchar(res, col, row);
    if (!raw_val) 
        return NULL;

    char *val = pstrdup(raw_val);
    duckdb_free(raw_val);

    /* 
     * Format Adaptation:
     * Standard Postgres arrays (e.g. float8[]) require "{}" syntax.
     * DuckDB returns "[]" (JSON style).
     * 
     * pgvector is a base type, so get_element_type() returns InvalidOid,
     * skipping this block and preserving "[]", which is exactly what pgvector wants.
     */
    if (get_element_type(pgtyp) != InvalidOid)
    {
        size_t len = strlen(val);
        if (len >= 2 && val[0] == '[' && val[len-1] == ']')
        {
            val[0] = '{';
            val[len-1] = '}';
        }
    }

    return val;
}

/* Legacy stub */
Datum 
duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, duckdb_result *res, int col, uint64_t row)
{
    switch (pgtyp)
    {
        case INT2OID:
            return Int16GetDatum(duckdb_value_int16(res, col, row));
        case INT4OID:
            return Int32GetDatum(duckdb_value_int32(res, col, row));
        case INT8OID:
            return Int64GetDatum(duckdb_value_int64(res, col, row));
        case FLOAT4OID:
            return Float4GetDatum(duckdb_value_float(res, col, row));
        case FLOAT8OID:
            return Float8GetDatum(duckdb_value_double(res, col, row));
        case BOOLOID:
            return BoolGetDatum(duckdb_value_boolean(res, col, row));
        case DATEOID:
        {
            duckdb_date d = duckdb_value_date(res, col, row);
            duckdb_date_struct date = duckdb_from_date(d);
            char *s = psprintf("%04d-%02d-%02d", date.year, date.month, date.day);
            Oid typinput, typioparam;
            getTypeInputInfo(DATEOID, &typinput, &typioparam);
            return OidInputFunctionCall(typinput, s, typioparam, -1);
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        {
            duckdb_timestamp t = duckdb_value_timestamp(res, col, row);
            duckdb_timestamp_struct ts = duckdb_from_timestamp(t);
            char *s = psprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", 
                               ts.date.year, ts.date.month, ts.date.day,
                               ts.time.hour, ts.time.min, ts.time.sec, ts.time.micros);
            Oid typinput, typioparam;
            getTypeInputInfo(pgtyp, &typinput, &typioparam);
            return OidInputFunctionCall(typinput, s, typioparam, -1);
        }
        case TEXTOID:
        case VARCHAROID:
        case BPCHAROID:
        {
            char *val = duckdb_value_varchar(res, col, row);
            Datum res = PointerGetDatum(cstring_to_text(val));
            duckdb_free(val);
            return res;
        }
        default:
        {
            char *val = duckdb_value_varchar(res, col, row);
            /* Fallback to text input function */
            /* This is slow but safe for rare types */
            return DirectFunctionCall3(textin, CStringGetDatum(val), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
        }
    }
}