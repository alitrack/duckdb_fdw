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
            elog(ERROR, "failed to open DuckDB: %s", dbpath);
        if (duckdb_connect(entry->db, &entry->conn) == DuckDBError)
            elog(ERROR, "failed to connect to DuckDB");
            
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
