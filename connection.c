/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * IDENTIFICATION
 *        connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "duckdb_fdw.h"
#include "access/xact.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "catalog/pg_foreign_server.h"
#include "utils/syscache.h"
#include "utils/inval.h"
#include "commands/defrem.h"

typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;
	duckdb_database db;
	duckdb_connection conn;
	int			xact_depth;
	bool		keep_connections;
	bool		invalidated;
	Oid			serverid;
	uint32		server_hashvalue;
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;
static bool xact_got_connection = false;

static void duckdb_make_new_connection(ConnCacheEntry *entry, ForeignServer *server);
static void duckdbfdw_xact_callback(XactEvent event, void *arg);
static void duckdbfdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

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

		RegisterXactCallback(duckdbfdw_xact_callback, NULL);
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID, duckdbfdw_inval_callback, (Datum) 0);
	}

	xact_got_connection = true;
	key = server->serverid;
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	
	if (!found)
		entry->conn = NULL;

	if (entry->conn == NULL)
		duckdb_make_new_connection(entry, server);

	/* Start a new remote transaction if needed */
	if (entry->xact_depth <= 0)
	{
		duckdb_do_sql_command(entry->conn, "BEGIN TRANSACTION", DEBUG3);
		entry->xact_depth = 1;
	}

	return entry->conn;
}

static void
duckdb_make_new_connection(ConnCacheEntry *entry, ForeignServer *server)
{
	const char *dbpath = NULL;
	ListCell   *lc;

	entry->serverid = server->serverid;
	entry->xact_depth = 0;
	entry->invalidated = false;
	entry->keep_connections = true;
	entry->server_hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID, ObjectIdGetDatum(server->serverid));

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "database") == 0)
			dbpath = defGetString(def);
	}

	if (duckdb_open(dbpath, &entry->db) == DuckDBError)
		ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to open DuckDB database: %s", dbpath ? dbpath : "memory")));

	if (duckdb_connect(entry->db, &entry->conn) == DuckDBError)
	{
		duckdb_close(&entry->db);
		ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to connect to DuckDB")));
	}
}

void
duckdb_cleanup_connection(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn)
		{
			duckdb_disconnect(&entry->conn);
			duckdb_close(&entry->db);
			entry->conn = NULL;
		}
	}
}

void
duckdb_do_sql_command(duckdb_connection conn, const char *sql, int level)
{
	duckdb_result res;
	elog(DEBUG1, "duckdb_fdw: executing sql: %s", sql);
	if (duckdb_query(conn, sql, &res) == DuckDBError)
	{
		const char *err = duckdb_result_error(&res);
		char *err_copy = err ? pstrdup(err) : NULL;
		duckdb_destroy_result(&res);
		ereport(level, (errcode(ERRCODE_FDW_ERROR),
				 errmsg("DuckDB execution failed: %s", err_copy ? err_copy : "unknown error")));
	}
	else
	{
		duckdb_destroy_result(&res);
	}
}

static void
duckdbfdw_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (!xact_got_connection)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		if (entry->xact_depth > 0)
		{
			switch (event)
			{
				case XACT_EVENT_PRE_COMMIT:
					duckdb_do_sql_command(entry->conn, "COMMIT", ERROR);
					break;
				case XACT_EVENT_ABORT:
					duckdb_do_sql_command(entry->conn, "ROLLBACK", WARNING);
					break;
				default:
					break;
			}
		}
		entry->xact_depth = 0;
		if (entry->invalidated || !entry->keep_connections)
		{
			duckdb_disconnect(&entry->conn);
			duckdb_close(&entry->db);
			entry->conn = NULL;
		}
	}
	xact_got_connection = false;
}

static void
duckdbfdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	if (ConnectionHash == NULL) return;
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL) continue;
		if (hashvalue == 0 || (cacheid == FOREIGNSERVEROID && entry->server_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}