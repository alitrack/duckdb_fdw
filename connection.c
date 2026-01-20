#include "postgres.h"
#include "duckdb_fdw.h"
#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "catalog/pg_foreign_server.h"
#include "utils/syscache.h"
#include "commands/defrem.h"

typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;
	duckdb_database db;
	duckdb_connection conn;
	int			xact_depth;
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

static void
duckdb_setup_secrets_and_extensions(duckdb_connection conn, ForeignServer *server)
{
    char *s3_region = NULL;
    char *s3_access_key = NULL;
    char *s3_secret_key = NULL;
    char *s3_endpoint = NULL;
    bool s3_use_ssl = true;
    char *extensions = NULL;
    char *attach_catalogs = NULL;
    ListCell *lc;

    /* 1. Parse Options and Detect Requirements */
    foreach(lc, server->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, "s3_region") == 0) s3_region = defGetString(def);
        else if (strcmp(def->defname, "s3_access_key_id") == 0) s3_access_key = defGetString(def);
        else if (strcmp(def->defname, "s3_secret_access_key") == 0) s3_secret_key = defGetString(def);
        else if (strcmp(def->defname, "s3_endpoint") == 0) s3_endpoint = defGetString(def);
        else if (strcmp(def->defname, "s3_use_ssl") == 0) s3_use_ssl = defGetBoolean(def);
        else if (strcmp(def->defname, "extensions") == 0) extensions = defGetString(def);
        else if (strcmp(def->defname, "attach_catalogs") == 0) attach_catalogs = defGetString(def);
    }

    /* 2. Intelligent Extension Autoloading */
    {
        bool need_httpfs = (s3_access_key != NULL);
        bool need_iceberg = false;

        if (attach_catalogs)
        {
            /* DuckLake resource types are handled by the 'iceberg' extension */
            if (strstr(attach_catalogs, "type=iceberg") || strstr(attach_catalogs, "type=ducklake"))
            {
                need_iceberg = true;
                need_httpfs = true;
            }
        }

        /* Use ERROR level to ensure user sees why extension loading fails */
        if (need_httpfs) duckdb_do_sql_command(conn, "INSTALL 'httpfs'; LOAD 'httpfs';", ERROR);
        if (need_iceberg) duckdb_do_sql_command(conn, "INSTALL 'iceberg'; LOAD 'iceberg';", ERROR);

        /* Also load any manually specified extensions */
        if (extensions)
        {
            char *ext_copy = pstrdup(extensions);
            char *token = strtok(ext_copy, ",");
            while (token)
            {
                if (strcmp(token, "httpfs") != 0 && strcmp(token, "iceberg") != 0)
                    duckdb_do_sql_command(conn, psprintf("INSTALL '%s'; LOAD '%s';", token, token), ERROR);
                token = strtok(NULL, ",");
            }
            pfree(ext_copy);
        }
    }

    /* 3. Secrets (Support S3 Tables) */
    if (s3_access_key && s3_secret_key)
    {
        StringInfoData sql;
        initStringInfo(&sql);
        appendStringInfoString(&sql, "CREATE OR REPLACE SECRET pg_duck_s3 ( TYPE S3, ");
        appendStringInfo(&sql, "KEY_ID '%s', ", s3_access_key);
        appendStringInfo(&sql, "SECRET '%s', ", s3_secret_key);
        if (s3_region) appendStringInfo(&sql, "REGION '%s', ", s3_region);
        if (s3_endpoint) appendStringInfo(&sql, "ENDPOINT '%s', ", s3_endpoint);
        appendStringInfo(&sql, "USE_SSL %s );", s3_use_ssl ? "true" : "false");
        duckdb_do_sql_command(conn, sql.data, ERROR);
    }

    /* 4. Catalogs (Auto ATTACH) */
    if (attach_catalogs)
    {
        char *at_copy = pstrdup(attach_catalogs);
        char *token = strtok(at_copy, ",");
        while (token)
        {
            char *name = token;
            char *uri = strchr(token, '=');
            if (uri)
            {
                char *options = NULL;
                *uri = '\0';
                uri++;

                /* Find start of options (first , or ;) */
                char *p = uri;
                while (*p) {
                    if (*p == ',' || *p == ';') {
                        options = p;
                        break;
                    }
                    p++;
                }

                if (options)
                {
                    *options = '\0';
                    options++;
                    /* Normalize options: replace ; with , */
                    for (char *opt_p = options; *opt_p; opt_p++) {
                        if (*opt_p == ';') *opt_p = ',';
                    }

                    if (strncmp(uri, "arn:aws:s3tables", 16) == 0 && 
                        !strstr(options, "authorization_type") && 
                        !strstr(options, "endpoint_type")) {
                        StringInfoData sql;
                        initStringInfo(&sql);
                        appendStringInfo(&sql, "ATTACH '%s' AS %s (%s, AUTHORIZATION_TYPE 'sigv4'", uri, name, options);
                        
                        if (s3_endpoint) {
                            appendStringInfo(&sql, ", ENDPOINT '%s'", s3_endpoint);
                        } else if (s3_region) {
                            appendStringInfo(&sql, ", ENDPOINT 's3tables.%s.amazonaws.com'", s3_region);
                        }
                        
                        appendStringInfoString(&sql, ");");
                        
                        duckdb_do_sql_command(conn, sql.data, ERROR);
                        pfree(sql.data);
                    }
                    else {
                        duckdb_do_sql_command(conn, psprintf("ATTACH '%s' AS %s (%s);", uri, name, options), ERROR);
                    }
                }
                else
                {
                    if (strncmp(uri, "arn:aws:s3tables", 16) == 0) {
                        StringInfoData sql;
                        initStringInfo(&sql);
                        appendStringInfo(&sql, "ATTACH '%s' AS %s (AUTHORIZATION_TYPE 'sigv4'", uri, name);
                        
                        if (s3_endpoint) {
                            appendStringInfo(&sql, ", ENDPOINT '%s'", s3_endpoint);
                        } else if (s3_region) {
                            appendStringInfo(&sql, ", ENDPOINT 's3tables.%s.amazonaws.com'", s3_region);
                        }

                        appendStringInfoString(&sql, ");");

                        duckdb_do_sql_command(conn, sql.data, ERROR);
                        pfree(sql.data);
                    }
                    else {
                        duckdb_do_sql_command(conn, psprintf("ATTACH '%s' AS %s;", uri, name), ERROR);
                    }
                }
            }
            token = strtok(NULL, ",");
        }
        pfree(at_copy);
    }
}

duckdb_connection
duckdb_get_connection(ForeignServer *server, bool truncatable)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("duckdb_fdw connections", 8, &ctl, HASH_ELEM | HASH_BLOBS);
	}

	key = server->serverid;
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	
	if (!found || entry->conn == NULL)
	{
        const char *dbpath = NULL;
        ListCell *lc;
        foreach(lc, server->options)
        {
            DefElem *def = (DefElem *) lfirst(lc);
            if (strcmp(def->defname, "database") == 0)
                dbpath = defGetString(def);
        }

        if (duckdb_open(dbpath, &entry->db) == DuckDBError)
            elog(ERROR, "failed to open DuckDB");
        if (duckdb_connect(entry->db, &entry->conn) == DuckDBError)
            elog(ERROR, "failed to connect to DuckDB");
            
        duckdb_setup_secrets_and_extensions(entry->conn, server);
        entry->xact_depth = 0;
	}
	return entry->conn;
}

void
duckdb_do_sql_command(duckdb_connection conn, const char *sql, int level)
{
	duckdb_result res;
	if (duckdb_query(conn, sql, &res) == DuckDBError)
	{
		const char *err = duckdb_result_error(&res);
		ereport(level, (errcode(ERRCODE_FDW_ERROR), errmsg("DuckDB: %s", err ? err : "error")));
	}
	duckdb_destroy_result(&res);
}
