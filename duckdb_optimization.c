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

    /* Get raw string from DuckDB */
    char *raw_val = duckdb_value_varchar(res, col, row);
    if (!raw_val) return NULL;

    /* Make a safe copy in current Postgres memory context */
    char *val = pstrdup(raw_val);
    duckdb_free(raw_val);

    /* 
     * Detect if this is a standard Postgres Array type.
     * Standard arrays (e.g. float8[]) require "{}" syntax.
     * DuckDB returns "[]".
     * 
     * We use get_element_type() to reliably detect arrays.
     * Note: pgvector is a base type (InvalidOid element type), 
     * so it will skip this block and keep "[]", which is exactly what it wants.
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

/* Legacy function stub to satisfy any potential linker references, though unused now */
Datum duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, duckdb_result *res, int col, uint64_t row) { return (Datum)0; }
