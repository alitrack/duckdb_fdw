/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "duckdb_fdw.h"

#include "access/xact.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "commands/defrem.h"

/* Length of host */
#define HOST_LEN 256

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID
 */
typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	sqlite3    *conn;			/* connection to foreign server, or NULL */
	/* Remaining fields are invalid when conn is NULL: */
	int			xact_depth;		/* 0 = no xact open, 1 = main xact open, 2 =
								 * one level of subxact open, etc */
	bool		invalidated;	/* true if reconnect is pending */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

/* tracks whether any work is needed in callback functions */
static bool xact_got_connection = false;


static bool do_sql_command(sqlite3 * conn, const char *sql, int level);
static void begin_remote_xact(ConnCacheEntry *entry);
static void sqlitefdw_xact_callback(XactEvent event, void *arg);
static void sqlitefdw_subxact_callback(SubXactEvent event,
									   SubTransactionId mySubid,
									   SubTransactionId parentSubid,
									   void *arg);
static void
			sqlitefdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

/*
 * sqlite_get_connection:
 * 			Get a connection which can be used to execute queries on
 * the remote Sqlite server with the user's authorization. A new connection
 * is established if we don't already have a suitable one.
 */
sqlite3 *
sqlite_get_connection(ForeignServer *server)
{
	const char *dbpath = NULL;
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;
	ListCell   *lc;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);

		/* allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("duckdb_fdw connections", 8,
									 &ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		RegisterXactCallback(sqlitefdw_xact_callback, NULL);
		RegisterSubXactCallback(sqlitefdw_subxact_callback, NULL);
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  sqlitefdw_inval_callback, (Datum) 0);
	}

	/* Set flag that we did GetConnection during the current transaction */
	xact_got_connection = true;

	/* Loop through the options, and get the server/port */
	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "database") == 0)
			dbpath = defGetString(def);
	}

	Assert(dbpath);
	key = server->serverid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* initialize new hashtable entry (key is already filled in) */
		entry->conn = NULL;
	}

	/*
	 * If the connection needs to be remade due to invalidation, disconnect as
	 * soon as we're out of all transactions.
	 */
	if (entry->conn != NULL && entry->invalidated && entry->xact_depth == 0)
	{
		int			rc = sqlite3_close(entry->conn);

		elog(DEBUG1, "closing connection %p for option changes to take effect. sqlite3_close=%d",
			 entry->conn, rc);
		entry->conn = NULL;
	}

	if (entry->conn == NULL)
	{
		int			rc;
		char	   *err;

		entry->xact_depth = 0;
		entry->invalidated = false;
		entry->server_hashvalue =
			GetSysCacheHashValue1(FOREIGNSERVEROID,
								  ObjectIdGetDatum(server->serverid));

		rc = sqlite3_open(dbpath, &entry->conn);
		if (rc != SQLITE_OK)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to open SQLite DB. rc=%d path=%s", rc, dbpath)));
		/* make 'LIKE' of SQLite case sensitive like PostgreSQL */
		// rc = sqlite3_exec(entry->conn, "pragma case_sensitive_like=1",
		// 				  NULL, NULL, &err);
		// if (rc != SQLITE_OK)
		// {
		// 	char	   *perr = pstrdup(err);

		// 	sqlite3_free(err);
		// 	sqlite3_close(entry->conn);
		// 	entry->conn = NULL;
		// 	ereport(ERROR,
		// 			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
		// 			 errmsg("failed to open SQLite DB. err=%s rc=%d", perr, rc)));
		// }
	}

	/*
	 * Start a new transaction or subtransaction if needed.
	 */
	begin_remote_xact(entry);

	return entry->conn;

}

/*
 * cleanup_connection:
 * Delete all the cache entries on backend exists.
 */
void
sqlite_cleanup_connection(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	int			rc;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		sqlite3_stmt *cur = NULL;

		if (entry->conn == NULL)
			continue;

		// while ((cur = sqlite3_next_stmt(entry->conn, cur)) != NULL)
		// {
		// 	elog(DEBUG1, "finalize %s", sqlite3_sql(cur));
		// 	sqlite3_finalize(cur);
		// }
		elog(DEBUG1, "disconnecting duckdb_fdw connection %p", entry->conn);
		rc = sqlite3_close(entry->conn);
		entry->conn = NULL;
		if (rc != SQLITE_OK)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("close connection failed: %s rc=%d", sqlite3_errmsg(entry->conn), rc)
					 ));
		}
	}
}


/*
 * Convenience subroutine to issue a non-data-returning SQL command to remote
 */
static bool
do_sql_command(sqlite3 * conn, const char *sql, int level)
{
	char	   *err = NULL;

	elog(DEBUG3, "do_sql_commnad %s", sql);

	if (sqlite3_exec(conn, sql, NULL, NULL, &err) != SQLITE_OK)
	{
		char	   *perr = NULL;

		if (err)
		{
			perr = pstrdup(err);
			sqlite3_free(err);
		}
		ereport(level,
				(errcode(ERRCODE_FDW_ERROR),
				 errmsg("failed to execute sql: %s %s", sql, perr)
				 ));
		if (perr)
			pfree(perr);
		return false;
	}
	return true;
}

/*
 * Start remote transaction or subtransaction, if needed.
 */
static void
begin_remote_xact(ConnCacheEntry *entry)
{
	int			curlevel = GetCurrentTransactionNestLevel();

	/* Start main transaction if we haven't yet */
	if (entry->xact_depth <= 0)
	{
		const char *sql;

		elog(DEBUG3, "starting remote transaction on connection %p",
			 entry->conn);

		sql = "BEGIN";

		do_sql_command(entry->conn, sql, ERROR);
		entry->xact_depth = 1;

	}

	/*
	 * If we're in a subtransaction, stack up savepoints to match our level.
	 * This ensures we can rollback just the desired effects when a
	 * subtransaction aborts.
	 */
	while (entry->xact_depth < curlevel)
	{
		char		sql[64];

		snprintf(sql, sizeof(sql), "SAVEPOINT s%d", entry->xact_depth + 1);
		do_sql_command(entry->conn, sql, ERROR);
		entry->xact_depth++;
	}
}


/*
 * Report an sqlite execution error
 */
void
sqlitefdw_report_error(int elevel, sqlite3_stmt * stmt, sqlite3 * conn,
					   const char *sql, int rc)
{
	const char *message = sqlite3_errmsg(conn);
	int			sqlstate = ERRCODE_FDW_ERROR;

	/* copy sql before callling another SQLite API */
	if (message)
		message = pstrdup(message);

	if (!sql && stmt)
	{
		sql = sqlite3_sql(stmt);
		if (sql)
			sql = pstrdup(sqlite3_sql(stmt));
	}

	if (stmt)
		sqlite3_finalize(stmt);

	ereport(ERROR,
			(errcode(sqlstate),
			 errmsg("failed to execute remote SQL: rc=%d %s \n   sql=%s",
					rc, message ? message : "", sql ? sql : "")
			 ));

}


/*
 * pgfdw_xact_callback --- cleanup at main-transaction end.
 */
static void
sqlitefdw_xact_callback(XactEvent event, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	elog(DEBUG1, "xact_callback %d", event);

	/*
	 * Scan all connection cache entries to find open remote transactions, and
	 * close them.
	 */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore cache entry if no open connection right now */
		if (entry->conn == NULL)
			continue;

		/* If it has an open remote transaction, try to close it */
		if (entry->xact_depth > 0)
		{
			elog(DEBUG3, "closing remote transaction on connection %p",
				 entry->conn);

			switch (event)
			{
				case XACT_EVENT_PARALLEL_PRE_COMMIT:
				case XACT_EVENT_PRE_COMMIT:

					/* Commit all remote transactions during pre-commit */
					// if (!sqlite3_get_autocommit(entry->conn))
						do_sql_command(entry->conn, "COMMIT", ERROR);
					break;
				case XACT_EVENT_PRE_PREPARE:

					/*
					 * We disallow remote transactions that modified anything,
					 * since it's not very reasonable to hold them open until
					 * the prepared transaction is committed.  For the moment,
					 * throw error unconditionally; later we might allow
					 * read-only cases.  Note that the error will cause us to
					 * come right back here with event == XACT_EVENT_ABORT, so
					 * we'll clean up the connection state at that point.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot prepare a transaction that modified remote tables")));
					break;
				case XACT_EVENT_PARALLEL_COMMIT:
				case XACT_EVENT_COMMIT:
				case XACT_EVENT_PREPARE:
					/* Pre-commit should have closed the open transaction */
					elog(ERROR, "missed cleaning up connection during pre-commit");
					break;
				case XACT_EVENT_PARALLEL_ABORT:
				case XACT_EVENT_ABORT:
					{
						sqlite3_stmt *cur = NULL;

						elog(DEBUG3, "abort transaction");

						/* Finalize all prepared statements */
						// while ((cur = sqlite3_next_stmt(entry->conn, NULL)) != NULL)
						// {
						// 	sqlite3_finalize(cur);
						// }

						/*
						 * rollback if in transaction because SQLite may
						 * already rollback
						 */
						// if (!sqlite3_get_autocommit(entry->conn))
							do_sql_command(entry->conn, "ROLLBACK", WARNING);

						break;
					}
			}
		}

		/* Reset state to show we're out of a transaction */
		entry->xact_depth = 0;

	}

	/*
	 * Regardless of the event type, we can now mark ourselves as out of the
	 * transaction.  (Note: if we are here during PRE_COMMIT or PRE_PREPARE,
	 * this saves a useless scan of the hashtable during COMMIT or PREPARE.)
	 */
	xact_got_connection = false;
}

/*
 * pgfdw_subxact_callback --- cleanup at subtransaction end.
 */
static void
sqlitefdw_subxact_callback(SubXactEvent event, SubTransactionId mySubid,
						   SubTransactionId parentSubid, void *arg)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;
	int			curlevel;

	/* Nothing to do at subxact start, nor after commit. */
	if (!(event == SUBXACT_EVENT_PRE_COMMIT_SUB ||
		  event == SUBXACT_EVENT_ABORT_SUB))
		return;

	/* Quick exit if no connections were touched in this transaction. */
	if (!xact_got_connection)
		return;

	/*
	 * Scan all connection cache entries to find open remote subtransactions
	 * of the current level, and close them.
	 */
	curlevel = GetCurrentTransactionNestLevel();
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		char		sql[100];

		/*
		 * We only care about connections with open remote subtransactions of
		 * the current level.
		 */
		if (entry->conn == NULL || entry->xact_depth < curlevel)
			continue;

		if (entry->xact_depth > curlevel)
			elog(ERROR, "missed cleaning up remote subtransaction at level %d",
				 entry->xact_depth);

		if (event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		{
			/* Commit all remote subtransactions during pre-commit */
			snprintf(sql, sizeof(sql), "RELEASE SAVEPOINT s%d", curlevel);
			do_sql_command(entry->conn, sql, ERROR);

		}
		else if (in_error_recursion_trouble())
		{
			/*
			 * Don't try to clean up the connection if we're already in error
			 * recursion trouble.
			 */
		}
		else
		{
			/* Rollback all remote subtransactions during abort */
			snprintf(sql, sizeof(sql),
					 "ROLLBACK TO SAVEPOINT s%d; RELEASE SAVEPOINT s%d",
					 curlevel, curlevel);
			// if (!sqlite3_get_autocommit(entry->conn))
				do_sql_command(entry->conn, sql, ERROR);
		}

		/* OK, we're outta that level of subtransaction */
		entry->xact_depth--;
	}
}

/*
 * Connection invalidation callback function
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade.
 * We can't immediately destroy them, since they might be in the midst of
 * a transaction, but we'll remake them at the next opportunity.
 *
 * Although most cache invalidation callbacks blow away all the related stuff
 * regardless of the given hashvalue, connections are expensive enough that
 * it's worth trying to avoid that.
 *
 * NB: We could avoid unnecessary disconnection more strictly by examining
 * individual option values, but it seems too much effort for the gain.
 */
static void
sqlitefdw_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}
