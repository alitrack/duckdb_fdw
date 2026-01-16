#include "postgres.h"
#include "duckdb_fdw.h"
#include "duckdb.h"

#include "access/xact.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "catalog/pg_type.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_foreign_server.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * Mapping from DuckDB type names to PostgreSQL type OIDs/Names.
 */
static TypeName *
duckdb_map_type(const char *duck_type)
{
    /* Basic Numeric Types */
    if (strcmp(duck_type, "BOOLEAN") == 0) return makeTypeName("bool");
    if (strcmp(duck_type, "BIGINT") == 0) return makeTypeName("int8");
    if (strcmp(duck_type, "HUGEINT") == 0) return makeTypeName("numeric");
    if (strcmp(duck_type, "INTEGER") == 0) return makeTypeName("int4");
    if (strcmp(duck_type, "SMALLINT") == 0) return makeTypeName("int2");
    if (strcmp(duck_type, "TINYINT") == 0) return makeTypeName("int2");
    if (strcmp(duck_type, "FLOAT") == 0) return makeTypeName("float4");
    if (strcmp(duck_type, "DOUBLE") == 0) return makeTypeName("float8");
    
    /* Text/String Types */
    if (strcmp(duck_type, "VARCHAR") == 0) return makeTypeName("text");
    if (strcmp(duck_type, "CHAR") == 0) return makeTypeName("text");
    if (strcmp(duck_type, "BLOB") == 0) return makeTypeName("bytea");
    if (strcmp(duck_type, "UUID") == 0) return makeTypeName("uuid");
    if (strcmp(duck_type, "JSON") == 0) return makeTypeName("jsonb");

    /* Date/Time */
    if (strcmp(duck_type, "DATE") == 0) return makeTypeName("date");
    if (strcmp(duck_type, "TIMESTAMP") == 0) return makeTypeName("timestamp");
    if (strcmp(duck_type, "TIMESTAMPTZ") == 0) return makeTypeName("timestamptz");
    if (strcmp(duck_type, "TIME") == 0) return makeTypeName("time");

    /* Arrays / Lists */
    if (strstr(duck_type, "DOUBLE[]") || strstr(duck_type, "FLOAT[]"))
    {
        TypeName *tn = makeTypeName("float8");
        tn->arrayBounds = list_make1(makeInteger(-1));
        return tn;
    }
    
    if (strstr(duck_type, "[]"))
    {
        TypeName *tn = makeTypeName("text");
        tn->arrayBounds = list_make1(makeInteger(-1));
        return tn;
    }

    /* Fallback */
    return makeTypeName("text");
}

/*
 * ImportForeignSchema handler.
 */
List *
duckdb_import_foreign_schema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List *commands = NIL;
    ForeignServer *server;
    duckdb_connection conn;
    duckdb_result res;
    StringInfoData query;
    bool is_file_import = false;
    char *remote_schema = stmt->remote_schema;

    /* Check if remote_schema looks like a file path */
    if (strstr(remote_schema, ".parquet") || 
        strstr(remote_schema, ".csv") || 
        strstr(remote_schema, ".json") ||
        strstr(remote_schema, "s3://") ||
        strstr(remote_schema, "http://") ||
        strstr(remote_schema, "/"))
    {
        is_file_import = true;
    }

    server = GetForeignServer(serverOid);
    conn = duckdb_get_connection(server, false);

    initStringInfo(&query);

    if (is_file_import)
    {
        char *tablename;
        char *last_slash = strrchr(remote_schema, '/');
        char *filename = last_slash ? last_slash + 1 : remote_schema;
        idx_t row_count;
        List *columns = NIL;
        CreateForeignTableStmt *create;
        char *table_opt_val;

        tablename = pstrdup(filename);
        char *dot = strrchr(tablename, '.');
        if (dot) *dot = '\0';

        if (strstr(remote_schema, ".csv"))
            appendStringInfo(&query, "DESCRIBE SELECT * FROM read_csv_auto('%s')", remote_schema);
        else if (strstr(remote_schema, ".json"))
            appendStringInfo(&query, "DESCRIBE SELECT * FROM read_json_auto('%s')", remote_schema);
        else
            appendStringInfo(&query, "DESCRIBE SELECT * FROM read_parquet('%s')", remote_schema);

        if (duckdb_query(conn, query.data, &res) == DuckDBError)
        {
            const char *err = duckdb_result_error(&res);
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                            errmsg("DuckDB DESCRIBE failed: %s", err ? err : "unknown error")));
        }

        row_count = duckdb_row_count(&res);
        for (idx_t i = 0; i < row_count; i++)
        {
            char *col_name = duckdb_value_varchar(&res, 0, i);
            char *col_type = duckdb_value_varchar(&res, 1, i);
            
            ColumnDef *col = makeNode(ColumnDef);
            col->colname = pstrdup(col_name);
            col->typeName = duckdb_map_type(col_type);
            columns = lappend(columns, col);

            duckdb_free(col_name);
            duckdb_free(col_type);
        }
        duckdb_destroy_result(&res);

        create = makeNode(CreateForeignTableStmt);
        create->base.relation = makeRangeVar(stmt->local_schema, tablename, -1);
        create->base.tableElts = columns;
        create->servername = server->servername;
        
        if (strstr(remote_schema, ".csv"))
            table_opt_val = psprintf("read_csv_auto('%s')", remote_schema);
        else if (strstr(remote_schema, ".json"))
            table_opt_val = psprintf("read_json_auto('%s')", remote_schema);
        else
            table_opt_val = psprintf("read_parquet('%s')", remote_schema);

        create->options = list_make1(makeDefElem("table", (Node *)makeString(table_opt_val), -1));
        commands = lappend(commands, create);
    }
    else
    {
        idx_t row_count;
        char *current_table = NULL;
        List *current_columns = NIL;

        appendStringInfo(&query, 
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = '%s' "
            "ORDER BY table_name, ordinal_position", 
            remote_schema);

        if (duckdb_query(conn, query.data, &res) == DuckDBError)
        {
            const char *err = duckdb_result_error(&res);
            ereport(ERROR, (errmsg("Failed to query DuckDB schema '%s': %s", remote_schema, err ? err : "unknown")));
        }

        row_count = duckdb_row_count(&res);
        for (idx_t i = 0; i < row_count; i++)
        {
            char *table_name = duckdb_value_varchar(&res, 0, i);
            char *col_name = duckdb_value_varchar(&res, 1, i);
            char *col_type = duckdb_value_varchar(&res, 2, i);

            if (current_table == NULL || strcmp(current_table, table_name) != 0)
            {
                if (current_table)
                {
                    CreateForeignTableStmt *create = makeNode(CreateForeignTableStmt);
                    create->base.relation = makeRangeVar(stmt->local_schema, current_table, -1);
                    create->base.tableElts = current_columns;
                    create->servername = server->servername;
                    create->options = list_make1(makeDefElem("table", (Node *)makeString(pstrdup(current_table)), -1));
                    commands = lappend(commands, create);
                }
                current_table = pstrdup(table_name);
                current_columns = NIL;
            }

            ColumnDef *col = makeNode(ColumnDef);
            col->colname = pstrdup(col_name);
            col->typeName = duckdb_map_type(col_type);
            current_columns = lappend(current_columns, col);

            duckdb_free(table_name);
            duckdb_free(col_name);
            duckdb_free(col_type);
        }

        if (current_table)
        {
            CreateForeignTableStmt *create = makeNode(CreateForeignTableStmt);
            create->base.relation = makeRangeVar(stmt->local_schema, current_table, -1);
            create->base.tableElts = current_columns;
            create->servername = server->servername;
            create->options = list_make1(makeDefElem("table", (Node *)makeString(current_table), -1));
            commands = lappend(commands, create);
        }
        duckdb_destroy_result(&res);
    }

    return commands;
}