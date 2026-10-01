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
	/*
	 * B5: true while this connection holds an explicit DuckDB
	 * transaction open (BEGIN issued by the write path, not yet
	 * settled by the xact callback).  DML is therefore atomic with
	 * the top-level PG transaction: the callback COMMITs the remote
	 * transaction on XACT_EVENT_COMMIT and ROLLBACKs it on ABORT.
	 * Read-only work (scans, probes) never triggers a BEGIN and
	 * keeps DuckDB's autocommit semantics.
	 */
	bool			in_xact;
} ConnCacheEntry;

/* E1: GUC registered in duckdb_fdw.c _PG_init (see there for semantics). */
extern bool duckdb_fdw_keep_connection;

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
		entry->in_xact = false;
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
	bool			commit;
	bool			abort;

	(void) arg;

	commit = (event == XACT_EVENT_COMMIT ||
			event == XACT_EVENT_PARALLEL_COMMIT);
	abort = (event == XACT_EVENT_ABORT ||
			event == XACT_EVENT_PARALLEL_ABORT ||
			event == XACT_EVENT_PREPARE);

	if (!commit && !abort)
		return;

	/*
	 * B5: settle the remote transaction BEFORE tearing the
	 * connection down.  Order matters: the COMMIT/ROLLBACK must run
	 * on the still-connected handle, and a failing settle must not
	 * crash the callback, so it is logged as a WARNING only.  (The
	 * XACT_EVENT_PREPARE case is treated as an abort: DuckDB cannot
	 * participate in two-phase commits, so a remote transaction that
	 * outlived this backend would be lost anyway.)
	 */
	if (ConnectionHash != NULL)
	{
		HASH_SEQ_STATUS scan;
		ConnCacheEntry *entry;

		/*
		 * Visit every entry.  The loop always runs to completion, so
		 * dynahash's hash_seq_search() performs the end-of-scan cleanup
		 * itself; an extra hash_seq_term() would error out.
		 */
		hash_seq_init(&scan, ConnectionHash);
		while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)) != NULL)
		{
			if (entry->in_xact && entry->conn)
			{
				duckdb_do_sql_command(entry->conn,
					commit ? "COMMIT" : "ROLLBACK", WARNING);
			}
			entry->in_xact = false;
		}

	}

	/*
	 * E1 (duckdb_fdw.keep_connection, default off): on COMMIT with the GUC
	 * enabled, keep the cached entries alive — the remote transaction has
	 * just been settled (COMMIT/ROLLBACK issued above) and in_xact cleared,
	 * so the next transaction reuses the same handles instead of re-opening
	 * the database, re-installing extensions and re-ATTACHing catalogs.
	 * ABORT (and PREPARE) always tears the cache down: after a remote
	 * ROLLBACK the connection must not be handed to the next transaction,
	 * which would risk a half-open remote state.  Trade-off: for file-backed
	 * DuckDB databases a retained connection keeps the file write lock held
	 * across transactions; quack/remote mode benefits most (see README.md).
	 */
	if (commit && duckdb_fdw_keep_connection)
		elog(DEBUG1,
			 "duckdb_fdw: keep_connection=on: retaining cached connection(s) across commit");
	else
		duckdb_cleanup_connection_cache();
}

static void
duckdb_connection_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
									SubTransactionId parentSubid, void *arg)
{
	(void) arg;
	(void) mySubid;
	(void) parentSubid;

	/*
	 * DELIBERATE NO-OP (B1): cached connections must NOT be torn down on
	 * subtransaction abort/commit.  A cached connection is owned by the
	 * top-level transaction: it is opened lazily by duckdb_get_connection()
	 * and released only by duckdb_connection_xact_callback() on
	 * XACT_EVENT_COMMIT / ABORT / PARALLEL_COMMIT / PARALLEL_ABORT.  Active
	 * plan states (ForeignScan / modify festates) hold the connection handle
	 * in festate->conn for the whole top-level transaction, so aborting any
	 * nested subtransaction used to close connections the parent transaction
	 * was still using -- leaving every such festate->conn dangling.  The next
	 * remote call on the dead handle then crashed the backend (and worse, the
	 * connection is re-fetched from the *new* cache entry while old plan
	 * states keep using the old one).
	 *
	 * Trade-off: DuckDB-side side effects performed inside an aborted
	 * subtransaction are not rolled back individually.  That window is tiny
	 * in this FDW: read-only queries keep no connection state, and DML is
	 * wrapped in one explicit BEGIN/COMMIT/ROLLBACK tied to the *top-level*
	 * transaction (see duckdb_fdw.c / B5), so a subtransaction abort cannot
	 * leave a half-committed remote transaction behind.  Concretely, if the
	 * parent transaction later COMMITs, rows written by the aborted
	 * subtransaction remain on the remote side (the remote transaction is
	 * all-or-nothing); if the parent ABORTs, they are rolled back along
	 * with everything else.  A stale-but-open connection is far less
	 * dangerous than a dangling one.
	 *
	 * The callback is kept registered (and empty) so the top-level xact
	 * callback remains the single owner of connection lifetime.
	 */
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

static bool duckdb_lib_version_checked = false;

/*
 * Assert that the loaded libduckdb is inside the tested range
 * [1.5, 1.6).  Runs once per backend at first connection (deliberately
 * NOT in _PG_init, so merely LOADing the extension can never fail).
 * The FDW is built against the bundled libduckdb headers; a runtime
 * library outside the tested range means the C API surface the FDW
 * relies on (value accessors, vector layout, TIMESTAMP_TZ semantics)
 * may have changed, so fail loudly instead of silently trusting it.
 */
static void
duckdb_assert_library_version(void)
{
	const char *ver;
	int			major = -1;
	int			minor = -1;

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
				 "tested range [1.5, 1.6)", ver);
	}

	/* supported iff 1.5.x: major/minor < 1.5, or >= 1.6 (incl. 2.x) */
	if (major != 1 || minor < 5 || minor > 5)
		elog(ERROR, "duckdb_fdw: unsupported libduckdb %s, "
			 "tested range [1.5, 1.6)", ver);
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
		UserMapping *um = GetUserMapping(GetUserId(), server->serverid);

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
	}

	if (!found || entry->conn == NULL)
	{
        const char *dbpath = NULL;
        const char *quack_host = NULL;
        const char *quack_token = NULL;
        Oid userid = GetUserId();
        ListCell *lc;

        /* Check user mapping for quack_token first (secure path) */
        {
            UserMapping *um = GetUserMapping(userid, server->serverid);
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

            duckdb_do_sql_command(entry->conn,
                "INSTALL quack FROM core_nightly; LOAD quack;", ERROR);

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
            attach_sql = psprintf("ATTACH %s AS remote;", host_lit);
            duckdb_do_sql_command(entry->conn, attach_sql, ERROR);
            pfree(full);
            pfree(host_lit);
            pfree(attach_sql);
        }
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

/*
 * B5: make sure the connection is inside a remote (DuckDB) transaction
 * before DML is sent on it.  The first writer of the top-level PG
 * transaction issues a plain BEGIN (later writers on the same cached
 * connection reuse it); the xact callback settles it (COMMIT/ROLLBACK)
 * when the PG transaction ends, which is what makes INSERTs through the
 * FDW atomic with the surrounding PG transaction.  Connections used only
 * for reads (or duckdb_execute probes) are never started and keep
 * DuckDB's autocommit behavior.
 *
 * If BEGIN itself fails the command is aborted: executing DML in
 * autocommit would break the atomicity guarantee.
 */
void
duckdb_ensure_remote_xact(duckdb_connection conn)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry = NULL;
	bool			found = false;

	if (conn == NULL || ConnectionHash == NULL)
		return;

	/* locate the cache entry that owns this connection */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)) != NULL)
	{
		if (entry->conn == conn)
		{
			found = true;
			break;
		}
	}
	/*
	 * dynahash: hash_seq_search() cleans up by itself once the scan
	 * completes; only a scan abandoned early (found == true) still
	 * needs an explicit hash_seq_term().
	 */
	if (found)
		hash_seq_term(&scan);


	if (!found || entry->in_xact)
		return;

	duckdb_do_sql_command(entry->conn, "BEGIN", ERROR);
	entry->in_xact = true;
}
