#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "utils/builtins.h"

static char *
duckdb_map_type_name(const char *duck_type)
{
    /* Handle DECIMAL(p,s) */
    if (strncmp(duck_type, "DECIMAL", 7) == 0)
    {
        /* Preserve precision/scale: DECIMAL(10,2) -> numeric(10,2) */
        const char *paren = strchr(duck_type, '(');
        if (paren)
        {
            char *ret = palloc(8 + strlen(paren) + 1);
            sprintf(ret, "numeric%s", paren);
            return ret;
        }
        return "numeric";
    }
    
    if (strcmp(duck_type, "UUID") == 0) return "uuid";
    if (strcmp(duck_type, "BLOB") == 0) return "bytea";
    if (strcmp(duck_type, "BIT") == 0) return "bit";
    
    if (strcmp(duck_type, "BOOLEAN") == 0) return "bool";
    if (strcmp(duck_type, "BIGINT") == 0) return "int8";
    if (strcmp(duck_type, "HUGEINT") == 0) return "numeric";
    if (strcmp(duck_type, "INTEGER") == 0) return "int4";
    if (strcmp(duck_type, "SMALLINT") == 0) return "int2";
    if (strcmp(duck_type, "TINYINT") == 0) return "int2";
    if (strcmp(duck_type, "FLOAT") == 0) return "float4";
    if (strcmp(duck_type, "DOUBLE") == 0) return "float8";
    if (strcmp(duck_type, "DATE") == 0) return "date";
    if (strcmp(duck_type, "TIMESTAMP") == 0) return "timestamp";
    if (strcmp(duck_type, "TIMESTAMPTZ") == 0) return "timestamptz";
    if (strcmp(duck_type, "JSON") == 0) return "jsonb";
    if (strcmp(duck_type, "VARCHAR") == 0) return "text";
    
    /* Handle Arrays: INTEGER[] -> int4[] */
    size_t len = strlen(duck_type);
    if (len > 2 && duck_type[len-2] == '[' && duck_type[len-1] == ']')
    {
        char *base = pstrdup(duck_type);
        base[len-2] = '\0';
        
        char *pg_base = duckdb_map_type_name(base);
        char *ret = psprintf("%s[]", pg_base);
        
        pfree(base);
        return ret;
    }

    return "text";
}

List *
duckdb_import_foreign_schema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    ForeignServer *server = GetForeignServer(serverOid);
    duckdb_connection conn = duckdb_get_connection(server, false);
    duckdb_result res;
    StringInfoData query;
    bool is_file = false;

    if (strstr(stmt->remote_schema, ".parquet") || strstr(stmt->remote_schema, "/"))
        is_file = true;

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    initStringInfo(&query);
    if (is_file)
    {
        StringInfoData ddl;
        char *tablename;
        char *last_slash = strrchr(stmt->remote_schema, '/');
        char *filename = last_slash ? last_slash + 1 : stmt->remote_schema;

        tablename = pstrdup(filename);
        char *dot = strrchr(tablename, '.');
        if (dot) *dot = '\0';

        initStringInfo(&ddl);
        appendStringInfo(&ddl, "CREATE FOREIGN TABLE %s.%s (", 
                         quote_identifier(stmt->local_schema),
                         quote_identifier(tablename));

        appendStringInfo(&query, "DESCRIBE SELECT * FROM read_parquet('%s')", stmt->remote_schema);
        if (duckdb_query(conn, query.data, &res) == DuckDBError)
            elog(ERROR, "DuckDB: %s", duckdb_result_error(&res));

        for (idx_t i = 0; i < duckdb_row_count(&res); i++)
        {
            char *cname = duckdb_value_varchar(&res, 0, i);
            char *ctype = duckdb_value_varchar(&res, 1, i);
            appendStringInfo(&ddl, "%s %s%s", quote_identifier(cname), duckdb_map_type_name(ctype), (i < duckdb_row_count(&res)-1) ? ", " : "");
            duckdb_free(cname); duckdb_free(ctype);
        }
        duckdb_destroy_result(&res);

        appendStringInfo(&ddl, ") SERVER %s OPTIONS (table '%s')", 
                         quote_identifier(server->servername), stmt->remote_schema);

        if (SPI_execute(ddl.data, false, 0) != SPI_OK_UTILITY)
            elog(ERROR, "Failed to create foreign table via SPI: %s", ddl.data);
    }
    else
    {
        /* 
         * 1. Get list of tables first.
         * 2. For each table, use DESCRIBE to get accurate column info.
         */
        duckdb_result tables_res;
        appendStringInfo(&query, 
            "SELECT database_name, schema_name, table_name "
            "FROM duckdb_tables() "
            "WHERE database_name = '%s' OR schema_name = '%s'", 
            stmt->remote_schema, stmt->remote_schema);

        if (duckdb_query(conn, query.data, &tables_res) == DuckDBError)
            elog(ERROR, "DuckDB: %s", duckdb_result_error(&tables_res));

        for (idx_t i = 0; i < duckdb_row_count(&tables_res); i++)
        {
            char *dbname = duckdb_value_varchar(&tables_res, 0, i);
            char *schname = duckdb_value_varchar(&tables_res, 1, i);
            char *tname = duckdb_value_varchar(&tables_res, 2, i);
            
            StringInfoData ddl;
            StringInfoData desc_query;
            duckdb_result col_res;

            initStringInfo(&ddl);
            initStringInfo(&desc_query);
            
            appendStringInfo(&ddl, "CREATE FOREIGN TABLE %s.%s (", 
                             quote_identifier(stmt->local_schema), quote_identifier(tname));
            
            appendStringInfo(&desc_query, "DESCRIBE SELECT * FROM %s.%s.%s", 
                             quote_identifier(dbname), quote_identifier(schname), quote_identifier(tname));
            
            if (duckdb_query(conn, desc_query.data, &col_res) != DuckDBError)
            {
                bool first_col = true;
                for (idx_t j = 0; j < duckdb_row_count(&col_res); j++)
                {
                    char *cname = duckdb_value_varchar(&col_res, 0, j);
                    char *ctype = duckdb_value_varchar(&col_res, 1, j);
                    
                    if (!first_col) appendStringInfoString(&ddl, ", ");
                    appendStringInfo(&ddl, "%s %s", quote_identifier(cname), duckdb_map_type_name(ctype));
                    first_col = false;
                    
                    duckdb_free(cname); duckdb_free(ctype);
                }
                appendStringInfo(&ddl, ") SERVER %s OPTIONS (table '%s.%s.%s')", 
                                 quote_identifier(server->servername), dbname, schname, tname);
                
                if (SPI_execute(ddl.data, false, 0) != SPI_OK_UTILITY)
                    elog(ERROR, "Failed to create foreign table: %s", ddl.data);
                
                duckdb_destroy_result(&col_res);
            }
            
            duckdb_free(dbname); duckdb_free(schname); duckdb_free(tname);
            pfree(ddl.data); pfree(desc_query.data);
        }
        duckdb_destroy_result(&tables_res);
    }

    SPI_finish();
    return NIL;
}
