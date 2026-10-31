#include "postgres.h"
#include "duckdb_fdw.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "catalog/pg_foreign_server.h"
#include "utils/syscache.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "foreign/foreign.h"
#include "catalog/pg_user_mapping.h"
#include "access/reloptions.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"

/*
 * duckdb_get_user_mapping_soft - look up the current user's mapping for
 * `server` without ERRORing when none exists.
 *
 * The core GetUserMapping() ereports "user mapping not found" unless the
 * user (or PUBLIC) has a mapping.  duckdb_fdw treats user mappings as
 * optional (all credentials can live on the FOREIGN SERVER instead), so
 * every lookup here must tolerate a missing mapping.  This mirrors the
 * core implementation (user first, then PUBLIC) but returns NULL instead
 * of raising.  GetUserMappingExtended(elevel) is not available on
 * PG15-18, hence the open-coded syscache search.
 */
static UserMapping *
duckdb_get_user_mapping_soft(Oid userid, Oid serverid)
{
	HeapTuple	tp;
	Form_pg_user_mapping umform;
	UserMapping *um;
	Datum		options;
	bool		isnull;

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(serverid));
	if (!HeapTupleIsValid(tp))
	{
		/* Not found for the specific user -- try PUBLIC */
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverid));
	}
	if (!HeapTupleIsValid(tp))
		return NULL;

	umform = (Form_pg_user_mapping) GETSTRUCT(tp);
	um = (UserMapping *) palloc(sizeof(UserMapping));
	um->umid = umform->oid;
	um->userid = userid;
	um->serverid = serverid;

	/* Extract umoptions (may be NULL for a mapping without options) */
	options = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
							  Anum_pg_user_mapping_umoptions, &isnull);
	um->options = isnull ? NIL : untransformRelOptions(options);

	ReleaseSysCache(tp);
	return um;
}


typedef struct ConnCacheKey
{
	Oid			serverid;
	bool		force_readonly;	/* connections opened for read-only servers
								 * are tracked separately so that a server
								 * re-created without the option (or vice
								 * versa) does not reuse a stale connection */
	/*
	 * User-mapping identity of the user that will use this connection (B2).
	 * Connection setup (S3 credentials, Quack tokens, ATTACHed catalogs) is
	 * driven by the *current user's* user mapping, so two users with
	 * different mappings for the same server must never share a cached
	 * connection: the first opener's secrets/catalogs would leak into the
	 * second user's session (and vice versa).  We key on the user mapping
	 * row's OID; users without an explicit mapping (server owners / super
	 * users get a synthetic mapping with InvalidOid, all other users get
	 * NULL back from GetUserMapping) fall back to the user OID, which is
	 * stable while the session identity is stable (SET ROLE switches both).
	 */
	Oid			umid;
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;
	duckdb_database db;
	duckdb_connection conn;
	/* A whole DuckDB transaction belongs to one PostgreSQL transaction level. */
	bool		in_xact;
	bool		xact_failed;
	SubTransactionId owner;
} ConnCacheEntry;

/* E1: GUC registered in duckdb_fdw.c _PG_init (see there for semantics). */
extern bool duckdb_fdw_keep_connection;

static HTAB *ConnectionHash = NULL;
static bool ConnectionXactCallbackRegistered = false;

/* Resources remain registered until their executor context is reset. */
typedef struct DuckDBResource
{
	dlist_node node;
	SubTransactionId owner;
	void (*cleanup)(void *);
	void (*finish)(void *);
	void *arg;
	MemoryContextCallback reset;
} DuckDBResource;

static dlist_head resources = DLIST_STATIC_INIT(resources);

static void
duckdb_resource_reset(void *arg)
{
	DuckDBResource *resource = arg;

	dlist_delete(&resource->node);
	resource->cleanup(resource->arg);
}

void
duckdb_register_resource(MemoryContext context, void (*cleanup)(void *),
						 void (*finish)(void *), void *arg)
{
	DuckDBResource *resource;
	dlist_iter iter;

	/* Rescans reuse their executor state and its existing context callback. */
	dlist_foreach(iter, &resources)
	{
		resource = dlist_container(DuckDBResource, node, iter.cur);
		if (resource->arg == arg)
		{
			resource->owner = GetCurrentSubTransactionId();
			return;
		}
	}
	resource = MemoryContextAllocZero(context, sizeof(*resource));
	resource->owner = GetCurrentSubTransactionId();
	resource->cleanup = cleanup;
	resource->finish = finish;
	resource->arg = arg;
	resource->reset.func = duckdb_resource_reset;
	resource->reset.arg = resource;
	dlist_push_tail(&resources, &resource->node);
	MemoryContextRegisterResetCallback(context, &resource->reset);
}

static void
duckdb_cleanup_resources(SubTransactionId owner)
{
	dlist_iter iter;

	dlist_foreach(iter, &resources)
	{
		DuckDBResource *resource = dlist_container(DuckDBResource, node, iter.cur);

		if (owner == InvalidSubTransactionId || resource->owner == owner)
			resource->cleanup(resource->arg);
	}
}

static void
duckdb_finish_resources(SubTransactionId owner)
{
	dlist_iter iter;

	dlist_foreach(iter, &resources)
	{
		DuckDBResource *resource = dlist_container(DuckDBResource, node, iter.cur);

		if (resource->finish &&
			(owner == InvalidSubTransactionId || resource->owner == owner))
			resource->finish(resource->arg);
	}
}

static ConnCacheEntry *
duckdb_connection_entry(duckdb_connection conn)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (!ConnectionHash || !conn)
		return NULL;
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = hash_seq_search(&scan)) != NULL)
		if (entry->conn == conn)
		{
			hash_seq_term(&scan);
			return entry;
		}
	return NULL;
}

void
duckdb_check_access(duckdb_connection conn)
{
	ConnCacheEntry *entry = duckdb_connection_entry(conn);

	if (!entry || !entry->in_xact)
		return;
	if (entry->owner != GetCurrentSubTransactionId())
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot access DuckDB from this subtransaction"),
						errdetail("A DuckDB transaction is active in an outer transaction."),
						errhint("Complete the current subtransaction before accessing DuckDB.")));
	if (entry->xact_failed)
		ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
						errmsg("DuckDB transaction requires rollback")));
}

void
duckdb_mark_xact_failed(duckdb_connection conn)
{
	ConnCacheEntry *entry = duckdb_connection_entry(conn);

	if (entry && entry->in_xact)
		entry->xact_failed = true;
}

bool
duckdb_remote_xact_active(duckdb_connection conn)
{
	ConnCacheEntry *entry = duckdb_connection_entry(conn);

	return entry && entry->in_xact;
}

/* Managed transactions propagate even errors from optional metadata probes. */
duckdb_state
duckdb_query_checked(duckdb_connection conn, const char *sql, duckdb_result *res)
{
	duckdb_state status;

	duckdb_check_access(conn);
	status = duckdb_query(conn, sql, res);
	if (status == DuckDBError && duckdb_remote_xact_active(conn))
	{
		char *message = duckdb_fdw_redact_secret_text(duckdb_result_error(res));

		duckdb_mark_xact_failed(conn);
		duckdb_destroy_result(res);
		ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("DuckDB: %s", message)));
	}
	return status;
}

duckdb_state
duckdb_prepare_checked(duckdb_connection conn, const char *sql,
					   duckdb_prepared_statement *stmt)
{
	duckdb_state status;

	duckdb_check_access(conn);
	status = duckdb_prepare(conn, sql, stmt);
	if (status == DuckDBError)
		duckdb_mark_xact_failed(conn);
	return status;
}

duckdb_state
duckdb_execute_checked(duckdb_connection conn, duckdb_prepared_statement stmt,
					   duckdb_result *res)
{
	duckdb_state status;

	duckdb_check_access(conn);
	status = duckdb_execute_prepared(stmt, res);
	if (status == DuckDBError)
		duckdb_mark_xact_failed(conn);
	return status;
}

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
		entry->in_xact = false;
		entry->xact_failed = false;
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
	bool		precommit = event == XACT_EVENT_PRE_COMMIT ||
		event == XACT_EVENT_PARALLEL_PRE_COMMIT;
	bool		commit = event == XACT_EVENT_COMMIT || event == XACT_EVENT_PARALLEL_COMMIT;
	bool		is_abort = event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT;
	bool		prepared = event == XACT_EVENT_PREPARE;
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (!precommit && !commit && !is_abort && !prepared && event != XACT_EVENT_PRE_PREPARE)
		return;
	if (precommit)
		duckdb_finish_resources(InvalidSubTransactionId);
	if (ConnectionHash && (precommit || event == XACT_EVENT_PRE_PREPARE))
	{
		/* Validate all connections before committing any of them. */
		hash_seq_init(&scan, ConnectionHash);
		while ((entry = hash_seq_search(&scan)) != NULL)
		{
			if (!entry->in_xact)
				continue;
			if (entry->xact_failed || event == XACT_EVENT_PRE_PREPARE)
			{
				hash_seq_term(&scan);
				if (entry->xact_failed)
					ereport(ERROR,
							(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
							 errmsg("cannot commit a failed DuckDB transaction"),
							 errhint("Roll back the outer transaction and retry.")));
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot prepare a transaction after a DuckDB table write has started")));
			}
		}
	}
	if (is_abort)
		duckdb_cleanup_resources(InvalidSubTransactionId);
	if (ConnectionHash && (precommit || is_abort))
	{
		hash_seq_init(&scan, ConnectionHash);
		while ((entry = hash_seq_search(&scan)) != NULL)
		{
			if (entry->in_xact && entry->conn)
			{
				/* ERROR is safe at PRE_COMMIT; ABORT cleanup uses WARNING. */
				PG_TRY();
				{
					duckdb_do_sql_command(entry->conn,
										  precommit ? "COMMIT" : "ROLLBACK",
										  precommit ? ERROR : WARNING);
				}
				PG_CATCH();
				{
					hash_seq_term(&scan);
					PG_RE_THROW();
				}
				PG_END_TRY();
			}
			entry->in_xact = false;
			entry->xact_failed = false;
		}
	}
	if (is_abort || prepared || (commit && !duckdb_fdw_keep_connection))
		duckdb_cleanup_connection_cache();
}

static void
duckdb_connection_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
								   SubTransactionId parentSubid, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
	{
		duckdb_finish_resources(mySubid);
		if (ConnectionHash)
		{
			hash_seq_init(&scan, ConnectionHash);
			while ((entry = hash_seq_search(&scan)) != NULL)
				if (entry->in_xact && entry->owner == mySubid && entry->xact_failed)
				{
					hash_seq_term(&scan);
					ereport(ERROR, (errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
								errmsg("DuckDB transaction requires rollback")));
				}
		}
		return;
	}
	if (event != SUBXACT_EVENT_ABORT_SUB && event != SUBXACT_EVENT_COMMIT_SUB)
		return;
	if (event == SUBXACT_EVENT_ABORT_SUB)
		duckdb_cleanup_resources(mySubid);
	else
	{
		dlist_iter iter;

		dlist_foreach(iter, &resources)
		{
			DuckDBResource *resource = dlist_container(DuckDBResource, node, iter.cur);

			if (resource->owner == mySubid)
				resource->owner = parentSubid;
		}
	}
	if (!ConnectionHash)
		return;
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = hash_seq_search(&scan)) != NULL)
	{
		if (!entry->in_xact || entry->owner != mySubid)
			continue;
		if (event == SUBXACT_EVENT_COMMIT_SUB)
			entry->owner = parentSubid;
		else
		{
			duckdb_result res;
			duckdb_state status = duckdb_query(entry->conn, "ROLLBACK", &res);

			if (status == DuckDBError)
			{
				char *message = duckdb_fdw_redact_secret_text(duckdb_result_error(&res));

				duckdb_destroy_result(&res);
				entry->xact_failed = true;
				entry->owner = parentSubid;
				ereport(WARNING, (errmsg("DuckDB rollback failed: %s", message)));
			}
			else
			{
				duckdb_destroy_result(&res);
				entry->in_xact = false;
				entry->xact_failed = false;
				entry->owner = InvalidSubTransactionId;
			}
		}
	}
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
    char *s3_url_style = NULL;
    char *init_sql = NULL;
    char *extensions = NULL;
    char *attach_catalogs = NULL;
    char *motherduck_token = NULL;
    ListCell *lc;
    Oid userid = GetUserId();

    /* 0. Check USER MAPPING first (preferred, secure path).
     * S3 credentials in user_mapping are only visible to the mapped user
     * and superusers, unlike pg_foreign_server which is public-readable. */
    {
        UserMapping *um = duckdb_get_user_mapping_soft(userid, server->serverid);
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
        else if (strcmp(def->defname, "s3_url_style") == 0) s3_url_style = defGetString(def);
        else if (strcmp(def->defname, "init_sql") == 0) init_sql = defGetString(def);
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
	        if (s3_url_style)
			{
				char *style_lit = duckdb_fdw_quote_literal(s3_url_style);
				appendStringInfo(&sql, "URL_STYLE %s, ", style_lit);
				pfree(style_lit);
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

    /* 6. Arbitrary user-supplied initialization SQL (LOAD, SET, ...) */
    if (init_sql)
        duckdb_do_sql_command(conn, init_sql, ERROR);
}

static bool duckdb_lib_version_checked = false;

/*
 * Assert that the loaded libduckdb is inside the tested range: 1.5.x or
 * 2.0.x.  Runs once per backend at first connection (deliberately NOT
 * in _PG_init, so merely LOADing the extension can never fail).
 * The FDW is built against the bundled libduckdb headers; a runtime
 * library outside the tested range means the C API surface the FDW
 * relies on (value accessors, vector layout, TIMESTAMP_TZ semantics)
 * may have changed, so fail loudly instead of silently trusting it.
 * Both 1.5.x and 2.0.x are regression tested (see the 2.0 CI target);
 * anything else (1.4-, 1.6+, 2.1+) is refused until it is tested.
 */
static void
duckdb_assert_library_version(void)
{
	const char *ver;
	int			major = -1;
	int			minor = -1;
	bool		ok;

	if (duckdb_lib_version_checked)
		return;
	duckdb_lib_version_checked = true;

	ver = duckdb_library_version();
	/*
	 * duckdb_library_version() returns strings like "v1.5.1": skip a
	 * leading non-digit prefix (the 'v') before parsing major.minor.
	 */
	{
		const char *p = ver;

		while (*p && (*p < '0' || *p > '9'))
			p++;
		if (sscanf(p, "%d.%d", &major, &minor) < 2)
			elog(ERROR, "duckdb_fdw: unparseable libduckdb version \"%s\", "
				 "tested range 1.5.x / 2.0.x", ver);
	}

	/* supported iff 1.5.x or 2.0.x */
	ok = (major == 1 && minor == 5) || (major == 2 && minor == 0);
	if (!ok)
		elog(ERROR, "duckdb_fdw: unsupported libduckdb %s, "
			 "tested range 1.5.x / 2.0.x", ver);
}

duckdb_connection
duckdb_get_connection(ForeignServer *server, bool truncatable)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	duckdb_runtime_guard_check();
	duckdb_assert_library_version();

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

	MemSet(&key, 0, sizeof(key));
	key.serverid = server->serverid;
	key.force_readonly = duckdb_fdw_server_is_readonly(server);
	{
		UserMapping *um = duckdb_get_user_mapping_soft(GetUserId(), server->serverid);

		/*
		 * No mapping (or synthetic owner/superuser mapping): fall back to
		 * the user OID (see ConnCacheKey comment).  The umid is part of the
		 * hash key, so a connection created under one user/mapping is never
		 * handed to a different user/mapping.
		 */
		key.umid = (um != NULL && OidIsValid(um->umid)) ? um->umid
													 : GetUserId();
	}
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);

	if (!found)
	{
		/*
		 * hash_search initializes a new entry from the key bytes only;
		 * the db/conn fields hold whatever the key happened to contain.
		 * Zero them so that a failed open (e.g. missing user mapping)
		 * aborting mid-creation cannot leave the cache entry holding
		 * garbage handles that the cleanup callback would close.
		 */
		entry->db = NULL;
		entry->conn = NULL;
		entry->in_xact = false;
		entry->xact_failed = false;
	}

	if (!found || entry->conn == NULL)
	{
        const char *dbpath = NULL;
        const char *quack_host = NULL;
        const char *quack_token = NULL;
        const char *disable_ssl = NULL;
        const char *ssl_fingerprint = NULL;
        Oid userid = GetUserId();
        ListCell *lc;

        /* Check user mapping for quack_token first (secure path) */
        {
            UserMapping *um = duckdb_get_user_mapping_soft(userid, server->serverid);
            if (um && um->options)
            {
                ListCell *umlc;
                foreach(umlc, um->options)
                {
                    DefElem *def = (DefElem *) lfirst(umlc);
                    if (strcmp(def->defname, "quack_token") == 0)
                        quack_token = defGetString(def);
                }
            }
        }

        foreach(lc, server->options)
        {
            DefElem *def = (DefElem *) lfirst(lc);
            if (strcmp(def->defname, "database") == 0)
                dbpath = defGetString(def);
            else if (strcmp(def->defname, "quack_host") == 0)
                quack_host = defGetString(def);
            else if (strcmp(def->defname, "quack_token") == 0 && quack_token == NULL)
                quack_token = defGetString(def);
            else if (strcmp(def->defname, "disable_ssl") == 0)
                disable_ssl = defGetBoolean(def) ? "true" : "false";
            else if (strcmp(def->defname, "ssl_fingerprint") == 0)
                ssl_fingerprint = defGetString(def);
        }

        /* Quack mode: use in-memory DuckDB if no database specified */
        if (quack_host && !dbpath)
            dbpath = ":memory:";

        /*
         * force_readonly: open the file database in read-only mode at the
         * DuckDB engine level, so any write statement is rejected by
         * DuckDB itself regardless of the code path that reaches it
         * (foreign table DML, duckdb_execute, batch insert, ...).
         * In-memory databases cannot be opened read-only; for those the
         * FDW-level enforcement (IsForeignRelUpdatable = 0 and the
         * duckdb_execute read-only gate) still applies.
         */
        if (key.force_readonly && dbpath &&
            strcmp(dbpath, ":memory:") != 0)
        {
            duckdb_config cfg = NULL;
            char *err = NULL;

            if (duckdb_create_config(&cfg) != DuckDBSuccess ||
                duckdb_set_config(cfg, "access_mode", "READ_ONLY") != DuckDBSuccess)
                elog(ERROR, "duckdb_fdw: failed to create read-only DuckDB configuration");
            if (duckdb_open_ext(dbpath, &entry->db, cfg, &err) == DuckDBError)
                elog(ERROR, "failed to open DuckDB in read-only mode: %s",
                     err ? err : "unknown error");
        }
        else if (duckdb_open(dbpath, &entry->db) == DuckDBError)
            elog(ERROR, "failed to open DuckDB");
	        if (duckdb_connect(entry->db, &entry->conn) == DuckDBError)
	            elog(ERROR, "failed to connect to DuckDB");

	        duckdb_setup_secrets_and_extensions(entry->conn, server);

        /* Quack proxy mode: load Quack extension and ATTACH remote */
        if (quack_host)
        {
            char *full;
            char *host_lit;
            char *attach_sql;
            StringInfoData attach_opts;

            /*
             * quack is published in the default (core) repository for every
             * supported libduckdb (1.5.x, 2.0.x). core_nightly only carries
             * it for some versions (not 1.5.0/1.5.1/2.0.0), and installing
             * from it fails when quack is already installed from core.
             */
            duckdb_do_sql_command(entry->conn,
                "INSTALL quack; LOAD quack;", ERROR);

            if (quack_token)
            {
                char *token_lit = duckdb_fdw_quote_literal(quack_token);
                char *secret_sql = psprintf(
                    "CREATE SECRET (TYPE quack, TOKEN %s);", token_lit);
                duckdb_do_sql_command(entry->conn, secret_sql, ERROR);
                pfree(token_lit);
                pfree(secret_sql);
            }

            /*
             * The ATTACH path must be a quoted literal: the host may contain
             * quotes or other characters with special meaning in SQL, so a
             * raw psprintf("ATTACH 'quack:%s'") would allow statement injection
             * from a user-supplied FDW option.
             */
            full = psprintf("quack:%s", quack_host);
            host_lit = duckdb_fdw_quote_literal(full);
            /*
             * TLS options are passed only when set, so Quack keeps its own
             * default otherwise (plain HTTP for localhost, TLS elsewhere).
             * SSL_FINGERPRINT pins the server's self-signed certificate.
             */
            initStringInfo(&attach_opts);
            if (disable_ssl)
                appendStringInfo(&attach_opts, "DISABLE_SSL %s", disable_ssl);
            if (ssl_fingerprint)
            {
                char *fp_lit = duckdb_fdw_quote_literal(ssl_fingerprint);

                appendStringInfo(&attach_opts, "%sSSL_FINGERPRINT %s",
                                 attach_opts.len > 0 ? ", " : "", fp_lit);
                pfree(fp_lit);
            }
            if (attach_opts.len > 0)
                attach_sql = psprintf("ATTACH %s AS remote (%s);", host_lit,
                                      attach_opts.data);
            else
                attach_sql = psprintf("ATTACH %s AS remote;", host_lit);
            duckdb_do_sql_command(entry->conn, attach_sql, ERROR);
            pfree(full);
            pfree(host_lit);
            pfree(attach_opts.data);
            pfree(attach_sql);
        }
	}
	duckdb_check_access(entry->conn);
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

		duckdb_mark_xact_failed(conn);

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

/* Start a whole remote transaction at the current PostgreSQL level. */
void
duckdb_ensure_remote_xact(duckdb_connection conn)
{
	ConnCacheEntry *entry = duckdb_connection_entry(conn);

	if (!entry)
		elog(ERROR, "could not access DuckDB connection");
	duckdb_check_access(conn);
	if (entry->in_xact)
		return;
	duckdb_do_sql_command(conn, "BEGIN", ERROR);
	entry->in_xact = true;
	entry->xact_failed = false;
	entry->owner = GetCurrentSubTransactionId();
}
