#include "postgres.h"
#include "duckdb_fdw.h"
#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "catalog/pg_foreign_server.h"
#include "utils/syscache.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"

typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;
	duckdb_database db;
	duckdb_connection conn;
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;
static bool ConnectionXactCallbackRegistered = false;

static void
duckdb_cleanup_connection_cache(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)) != NULL)
	{
		if (entry->conn)
		{
			duckdb_disconnect(&entry->conn);
			entry->conn = NULL;
		}
		if (entry->db)
		{
			duckdb_close(&entry->db);
			entry->db = NULL;
		}
	}
}

static void
duckdb_connection_xact_callback(XactEvent event, void *arg)
{
	(void) arg;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PARALLEL_ABORT:
			duckdb_cleanup_connection_cache();
			break;
		default:
			break;
	}
}

static void
duckdb_connection_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg)
{
	(void) arg;
	(void) mySubid;
	(void) parentSubid;

	/*
	 * On subtransaction abort, clean up cached connections so that any
	 * in-progress DuckDB state from the aborted subtransaction is discarded.
	 * This is conservative (clears all connections, not just those created
	 * in the subtransaction) but avoids leaking DuckDB internal state into
	 * the parent transaction.
	 */
	if (event == SUBXACT_EVENT_ABORT_SUB)
		duckdb_cleanup_connection_cache();
}

static void
append_endpoint_clause(StringInfo sql, const char *s3_endpoint, const char *s3_region)
{
	if (s3_endpoint)
	{
		char *endpoint_lit = duckdb_fdw_quote_literal(s3_endpoint);

		appendStringInfo(sql, ", ENDPOINT %s", endpoint_lit);
		pfree(endpoint_lit);
	}
	else if (s3_region)
	{
		char *endpoint = psprintf("s3tables.%s.amazonaws.com", s3_region);
		char *endpoint_lit = duckdb_fdw_quote_literal(endpoint);

		appendStringInfo(sql, ", ENDPOINT %s", endpoint_lit);
		pfree(endpoint);
		pfree(endpoint_lit);
	}
}

static void
install_extension_if_valid(duckdb_connection conn, const char *ext_name)
{
	char	   *ext_lit;
	char	   *sql;

	if (!duckdb_fdw_is_valid_identifier(ext_name))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid extension name \"%s\"", ext_name)));

	ext_lit = duckdb_fdw_quote_literal(ext_name);
	sql = psprintf("INSTALL %s; LOAD %s;", ext_lit, ext_lit);
	duckdb_do_sql_command(conn, sql, ERROR);
	pfree(ext_lit);
	pfree(sql);
}

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
    char *motherduck_token = NULL;
    ListCell *lc;
    Oid userid = GetUserId();

    /* 0. Check USER MAPPING first (preferred, secure path).
     * S3 credentials in user_mapping are only visible to the mapped user
     * and superusers, unlike pg_foreign_server which is public-readable. */
    {
        UserMapping *um = GetUserMapping(userid, server->serverid);
        if (um && um->options)
        {
            foreach(lc, um->options)
            {
                DefElem *def = (DefElem *) lfirst(lc);
                if (strcmp(def->defname, "s3_access_key_id") == 0)
                    s3_access_key = defGetString(def);
                else if (strcmp(def->defname, "s3_secret_access_key") == 0)
                    s3_secret_key = defGetString(def);
                else if (strcmp(def->defname, "motherduck_token") == 0)
                    motherduck_token = defGetString(def);
            }
        }
    }

    /* 1. Parse Server Options (fallback if user_mapping didn't set creds) */
    foreach(lc, server->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);
        if (strcmp(def->defname, "s3_region") == 0) s3_region = defGetString(def);
        else if (strcmp(def->defname, "s3_access_key_id") == 0 && s3_access_key == NULL) s3_access_key = defGetString(def);
        else if (strcmp(def->defname, "s3_secret_access_key") == 0 && s3_secret_key == NULL) s3_secret_key = defGetString(def);
        else if (strcmp(def->defname, "s3_endpoint") == 0) s3_endpoint = defGetString(def);
        else if (strcmp(def->defname, "s3_use_ssl") == 0) s3_use_ssl = defGetBoolean(def);
        else if (strcmp(def->defname, "extensions") == 0) extensions = defGetString(def);
        else if (strcmp(def->defname, "attach_catalogs") == 0) attach_catalogs = defGetString(def);
        else if (strcmp(def->defname, "motherduck_token") == 0 && motherduck_token == NULL)
            motherduck_token = defGetString(def);
    }

    /* 2. Intelligent Extension Autoloading */
    {
        bool need_httpfs = (s3_access_key != NULL);
        bool need_iceberg = false;
        bool need_motherduck = (motherduck_token != NULL);

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
        if (need_motherduck) duckdb_do_sql_command(conn, "INSTALL 'motherduck'; LOAD 'motherduck';", ERROR);

	        /* Also load any manually specified extensions */
		        if (extensions)
		        {
		            char *ext_copy = pstrdup(extensions);
		            char *saveptr = NULL;
		            char *token = duckdb_fdw_next_token(ext_copy, ",", &saveptr);
		            while (token)
		            {
		                char *trimmed = duckdb_fdw_trim_token(token);
		                if (trimmed[0] == '\0')
		                {
		                    token = duckdb_fdw_next_token(NULL, ",", &saveptr);
		                    continue;
		                }
		                if (strcmp(trimmed, "httpfs") != 0 && strcmp(trimmed, "iceberg") != 0)
		                    install_extension_if_valid(conn, trimmed);
		                token = duckdb_fdw_next_token(NULL, ",", &saveptr);
		            }
		            pfree(ext_copy);
		        }
	    }

    /* 3. Secrets (Support S3 Tables) */
	    if (s3_access_key && s3_secret_key)
	    {
	        StringInfoData sql;
			char *key_lit = duckdb_fdw_quote_literal(s3_access_key);
			char *secret_lit = duckdb_fdw_quote_literal(s3_secret_key);
	        initStringInfo(&sql);
	        appendStringInfoString(&sql, "CREATE OR REPLACE SECRET pg_duck_s3 ( TYPE S3, ");
	        appendStringInfo(&sql, "KEY_ID %s, ", key_lit);
	        appendStringInfo(&sql, "SECRET %s, ", secret_lit);
	        if (s3_region)
			{
				char *region_lit = duckdb_fdw_quote_literal(s3_region);
				appendStringInfo(&sql, "REGION %s, ", region_lit);
				pfree(region_lit);
			}
	        if (s3_endpoint)
			{
				char *endpoint_lit = duckdb_fdw_quote_literal(s3_endpoint);
				appendStringInfo(&sql, "ENDPOINT %s, ", endpoint_lit);
				pfree(endpoint_lit);
			}
	        appendStringInfo(&sql, "USE_SSL %s );", s3_use_ssl ? "true" : "false");
	        duckdb_do_sql_command(conn, sql.data, ERROR);
			pfree(key_lit);
			pfree(secret_lit);
			pfree(sql.data);
	    }

    /* 4. MotherDuck Integration */
    if (motherduck_token)
    {
        StringInfoData sql;
        char *token_lit = duckdb_fdw_quote_literal(motherduck_token);
        initStringInfo(&sql);
        appendStringInfo(&sql, "CREATE OR REPLACE SECRET pg_duck_md "
                         "( TYPE MOTHERDUCK, TOKEN %s );", token_lit);
        duckdb_do_sql_command(conn, sql.data, ERROR);
        pfree(token_lit);
        pfree(sql.data);
    }

    /* 5. Catalogs (Auto ATTACH) */
	    if (attach_catalogs)
	    {
	        char *at_copy = pstrdup(attach_catalogs);
	        char *saveptr = NULL;
	        char *token = duckdb_fdw_next_token(at_copy, ",", &saveptr);
	        while (token)
	        {
	            char *name = duckdb_fdw_trim_token(token);
	            char *uri = strchr(token, '=');
	            if (uri)
	            {
	                char *options = NULL;
	                *uri = '\0';
	                uri = duckdb_fdw_trim_token(uri + 1);

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
	                    options = duckdb_fdw_trim_token(options + 1);
	                    /* Normalize options: replace ; with , */
	                    for (char *opt_p = options; *opt_p; opt_p++) {
	                        if (*opt_p == ';') *opt_p = ',';
	                    }
						if (!duckdb_fdw_is_safe_sql_fragment(options))
							ereport(ERROR,
									(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
									 errmsg("attach_catalogs contains unsafe options fragment")));

	                    if (strncmp(uri, "arn:aws:s3tables", 16) == 0 &&
	                        !strstr(options, "authorization_type") &&
	                        !strstr(options, "endpoint_type")) {
	                        StringInfoData sql;
							char *uri_lit;
							char *name_id;
	                        initStringInfo(&sql);
							if (!duckdb_fdw_is_valid_identifier(name))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("invalid attach alias \"%s\"", name)));
							if (!duckdb_fdw_is_safe_sql_fragment(uri))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("attach_catalogs contains unsafe URI fragment")));
							uri_lit = duckdb_fdw_quote_literal(uri);
							name_id = duckdb_fdw_quote_identifier(name);
	                        appendStringInfo(&sql, "ATTACH %s AS %s (%s, AUTHORIZATION_TYPE 'sigv4'", uri_lit, name_id, options);
	                        append_endpoint_clause(&sql, s3_endpoint, s3_region);

	                        appendStringInfoString(&sql, ");");

	                        duckdb_do_sql_command(conn, sql.data, ERROR);
							pfree(uri_lit);
							pfree(name_id);
	                        pfree(sql.data);
	                    }
	                    else {
							StringInfoData sql;
							char *uri_lit;
							char *name_id;
							if (!duckdb_fdw_is_valid_identifier(name))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("invalid attach alias \"%s\"", name)));
							if (!duckdb_fdw_is_safe_sql_fragment(uri))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("attach_catalogs contains unsafe URI fragment")));
							uri_lit = duckdb_fdw_quote_literal(uri);
							name_id = duckdb_fdw_quote_identifier(name);
							initStringInfo(&sql);
							appendStringInfo(&sql, "ATTACH %s AS %s (%s);", uri_lit, name_id, options);
	                        duckdb_do_sql_command(conn, sql.data, ERROR);
							pfree(uri_lit);
							pfree(name_id);
							pfree(sql.data);
	                    }
	                }
	                else
	                {
	                    if (strncmp(uri, "arn:aws:s3tables", 16) == 0) {
	                        StringInfoData sql;
							char *uri_lit;
							char *name_id;
	                        initStringInfo(&sql);
							if (!duckdb_fdw_is_valid_identifier(name))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("invalid attach alias \"%s\"", name)));
							if (!duckdb_fdw_is_safe_sql_fragment(uri))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("attach_catalogs contains unsafe URI fragment")));
							uri_lit = duckdb_fdw_quote_literal(uri);
							name_id = duckdb_fdw_quote_identifier(name);
	                        appendStringInfo(&sql, "ATTACH %s AS %s (AUTHORIZATION_TYPE 'sigv4'", uri_lit, name_id);
	                        append_endpoint_clause(&sql, s3_endpoint, s3_region);

	                        appendStringInfoString(&sql, ");");

	                        duckdb_do_sql_command(conn, sql.data, ERROR);
							pfree(uri_lit);
							pfree(name_id);
	                        pfree(sql.data);
	                    }
	                    else {
							StringInfoData sql;
							char *uri_lit;
							char *name_id;
							if (!duckdb_fdw_is_valid_identifier(name))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("invalid attach alias \"%s\"", name)));
							if (!duckdb_fdw_is_safe_sql_fragment(uri))
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
										 errmsg("attach_catalogs contains unsafe URI fragment")));
							uri_lit = duckdb_fdw_quote_literal(uri);
							name_id = duckdb_fdw_quote_identifier(name);
							initStringInfo(&sql);
							appendStringInfo(&sql, "ATTACH %s AS %s;", uri_lit, name_id);
	                        duckdb_do_sql_command(conn, sql.data, ERROR);
							pfree(uri_lit);
							pfree(name_id);
							pfree(sql.data);
	                    }
	                }
	            }
	            token = duckdb_fdw_next_token(NULL, ",", &saveptr);
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

	duckdb_runtime_guard_check();

	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("duckdb_fdw connections", 8, &ctl, HASH_ELEM | HASH_BLOBS);
	}
	if (!ConnectionXactCallbackRegistered)
	{
		RegisterXactCallback(duckdb_connection_xact_callback, NULL);
		RegisterSubXactCallback(duckdb_connection_subxact_callback, NULL);
		ConnectionXactCallbackRegistered = true;
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
	}
	return entry->conn;
}

void
duckdb_do_sql_command(duckdb_connection conn, const char *sql, int level)
{
	duckdb_result res;
	MemSet(&res, 0, sizeof(res));
	if (duckdb_query(conn, sql, &res) == DuckDBError)
	{
		const char *err = duckdb_result_error(&res);
		char *safe_err = duckdb_fdw_redact_secret_text(err ? err : "error");

		PG_TRY();
		{
			ereport(level, (errcode(ERRCODE_FDW_ERROR), errmsg("DuckDB: %s", safe_err)));
		}
		PG_FINALLY();
		{
			duckdb_destroy_result(&res);
			pfree(safe_err);
		}
		PG_END_TRY();
		return;
	}
	duckdb_destroy_result(&res);
}
