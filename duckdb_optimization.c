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
Datum duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, duckdb_result *res, int col, uint64_t row) { return (Datum)0; }