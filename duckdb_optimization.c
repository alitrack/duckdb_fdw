#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"

char *
duckdb_extract_as_cstring(duckdb_result *res, int col, uint64_t row, Oid pgtyp)
{
    if (duckdb_value_is_null(res, col, row))
    {
        /* elog(NOTICE, "DEBUG: Row %lu Col %d is NULL", row, col); */
        return NULL;
    }

    char *raw_val = duckdb_value_varchar(res, col, row);
    if (!raw_val) 
    {
        elog(WARNING, "duckdb_fdw: Unexpected NULL varchar for non-NULL value at row %lu col %d", row, col);
        return NULL;
    }

    /* DEBUG: Print raw value */
    /* Only log complex types to avoid spam */
    if (pgtyp == 393219 /* vector OID usually dynamic, but let's just log everything for now */ || pgtyp == 1022 /* float8[] */) 
    {
        elog(NOTICE, "DEBUG: Row %lu Col %d OID %u Raw: '%s'", row, col, pgtyp, raw_val);
    }

    char *val = pstrdup(raw_val);
    duckdb_free(raw_val);

    /* Format Adaptation */
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

Datum duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, duckdb_result *res, int col, uint64_t row) { return (Datum)0; }
