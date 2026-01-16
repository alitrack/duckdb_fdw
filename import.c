#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "utils/builtins.h"

static const char *
duckdb_map_type_name(const char *duck_type)
{
    if (strcmp(duck_type, "BOOLEAN") == 0) return "bool";
    if (strcmp(duck_type, "BIGINT") == 0) return "int8";
    if (strcmp(duck_type, "HUGEINT") == 0) return "numeric";
    if (strcmp(duck_type, "INTEGER") == 0) return "int4";
    if (strcmp(duck_type, "SMALLINT") == 0) return "int2";
    if (strcmp(duck_type, "FLOAT") == 0) return "float4";
    if (strcmp(duck_type, "DOUBLE") == 0) return "float8";
    if (strcmp(duck_type, "DATE") == 0) return "date";
    if (strcmp(duck_type, "TIMESTAMP") == 0) return "timestamp";
    if (strcmp(duck_type, "JSON") == 0) return "jsonb";
    if (strcmp(duck_type, "VARCHAR") == 0) return "text";
    if (strstr(duck_type, "DECIMAL")) return "numeric";
    if (strstr(duck_type, "[]")) return "text[]";
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
         * Use duckdb_columns() instead of information_schema to support attached catalogs.
         * We match remote_schema against either database_name or schema_name.
         */
        appendStringInfo(&query, 
            "SELECT database_name, schema_name, table_name, column_name, data_type "
            "FROM duckdb_columns() "
            "WHERE database_name = '%s' OR schema_name = '%s' "
            "ORDER BY database_name, schema_name, table_name, column_index", 
            stmt->remote_schema, stmt->remote_schema);

        if (duckdb_query(conn, query.data, &res) == DuckDBError)
            elog(ERROR, "DuckDB: %s", duckdb_result_error(&res));

        char *curr_db = NULL;
        char *curr_sch = NULL;
        char *curr_tab = NULL;
        StringInfoData ddl;
        initStringInfo(&ddl);
        bool first_col = true;

        for (idx_t i = 0; i < duckdb_row_count(&res); i++)
        {
            char *dbname = duckdb_value_varchar(&res, 0, i);
            char *schname = duckdb_value_varchar(&res, 1, i);
            char *tname = duckdb_value_varchar(&res, 2, i);
            char *cname = duckdb_value_varchar(&res, 3, i);
            char *ctype = duckdb_value_varchar(&res, 4, i);

            if (curr_tab == NULL || strcmp(curr_tab, tname) != 0 || strcmp(curr_sch, schname) != 0 || strcmp(curr_db, dbname) != 0)
            {
                if (curr_tab)
                {
                    appendStringInfo(&ddl, ") SERVER %s OPTIONS (table '%s.%s.%s')", 
                        quote_identifier(server->servername), curr_db, curr_sch, curr_tab);
                    if (SPI_execute(ddl.data, false, 0) != SPI_OK_UTILITY)
                        elog(ERROR, "SPI failed: %s", ddl.data);
                }
                resetStringInfo(&ddl);
                appendStringInfo(&ddl, "CREATE FOREIGN TABLE %s.%s (", 
                    quote_identifier(stmt->local_schema), quote_identifier(tname));
                curr_db = pstrdup(dbname);
                curr_sch = pstrdup(schname);
                curr_tab = pstrdup(tname);
                first_col = true;
            }
            
            if (!first_col) appendStringInfoString(&ddl, ", ");
            appendStringInfo(&ddl, "%s %s", quote_identifier(cname), duckdb_map_type_name(ctype));
            first_col = false;

            duckdb_free(dbname); duckdb_free(schname); duckdb_free(tname); 
            duckdb_free(cname); duckdb_free(ctype);
        }

        if (curr_tab)
        {
            appendStringInfo(&ddl, ") SERVER %s OPTIONS (table '%s.%s.%s')", 
                quote_identifier(server->servername), curr_db, curr_sch, curr_tab);
            if (SPI_execute(ddl.data, false, 0) != SPI_OK_UTILITY)
                elog(ERROR, "SPI failed: %s", ddl.data);
        }
        duckdb_destroy_result(&res);
    }

    SPI_finish();
    return NIL;
}
