/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        duckdb_fdw.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef duckdb_fdw_H
#define duckdb_fdw_H

#include "sqlite3.h"

#if (PG_VERSION_NUM >= 120000)
#include "nodes/pathnodes.h"
#include "access/table.h"
#include "utils/float.h"
#include "optimizer/optimizer.h"
#else
#include "nodes/relation.h"
#include "optimizer/var.h"
#endif

#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/rel.h"

#define SQLITE_PREFETCH_ROWS	100
#define SQLITE_BLKSIZ		(1024 * 4)
#define SQLITE_PORT			3306
#define MAXDATALEN			1024 * 64

#define WAIT_TIMEOUT		0
#define INTERACTIVE_TIMEOUT 0


#define CR_NO_ERROR 0

#if (PG_VERSION_NUM < 120000)
#define table_close(rel, lock)	heap_close(rel, lock)
#define table_open(rel, lock)	heap_open(rel, lock)
#endif

/*
 * Options structure to store the Sqlite
 * server information
 */
typedef struct sqlite_opt
{
	int			svr_port;		/* SQLite port number */
	char	   *svr_address;	/* SQLite server ip address */

	char	   *svr_database;	/* SQLite database name */
	char	   *svr_table;		/* SQLite table name */
	char	   *svr_init_command;	/* SQLite SQL statement to execute when
									 * connecting to the SQLite server. */
	unsigned long max_blob_size;	/* Max blob size to read without
									 * truncation */
	bool		use_remote_estimate;	/* use remote estimate for rows */
}			sqlite_opt;


/*
 * FDW-specific information for ForeignScanState
 * fdw_state.
 */
typedef struct SQLiteFdwExecState
{
	sqlite3    *conn;			/* SQLite connection handle */
	sqlite3_stmt *stmt;			/* SQLite prepared stament handle */
	char	   *query;			/* Query string */
	Relation	rel;			/* relcache entry for the foreign table */
	List	   *retrieved_attrs;	/* list of target attribute numbers */

	bool		cursor_exists;	/* have we created the cursor? */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	Oid		   *param_types;	/* type of query parameters */

	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */

	sqlite_opt *sqliteFdwOptions;	/* SQLite FDW options */

	List	   *attr_list;		/* query attribute list */
	List	   *column_list;	/* Column list of SQLite Column structures */

	int64		row_nums;		/* number of rows */
	Datum	  **rows;			/* all rows of scan */
	int64		rowidx;			/* current index of rows */
	bool	  **rows_isnull;	/* is null */
	bool		for_update;		/* true if this scan is update target */
	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
	AttrNumber *junk_idx;
}			SqliteFdwExecState;


typedef struct SqliteFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;
	/* Estimated size and cost for a scan or join. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	/* Costs excluding costs for transferring data from the foreign server */
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	bool		use_remote_estimate;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	List	   *shippable_extensions;	/* OIDs of whitelisted extensions */
	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	List	   *joinclauses;

	/* Upper relation information */
	UpperRelationKind stage;

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

	/*
	 * Name of the relation while EXPLAINing ForeignScan. It is used for join
	 * relations but is set for all relations. For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	StringInfo	relation_name;

	/* Grouping information */
	List	   *grouped_tlist;
}			SqliteFdwRelationInfo;

extern bool sqlite_is_foreign_expr(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Expr *expr);

extern Expr *sqlite_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern Expr *sqlite_find_em_expr_for_input_target(PlannerInfo *root,
							  EquivalenceClass *ec,
							  PathTarget *target,
							  RelOptInfo *fallbackRel);

/* in duckdb_fdw.c */
extern int sqlite_set_transmission_modes(void);
extern void sqlite_reset_transmission_modes(int nestlevel);

/* option.c headers */
extern sqlite_opt * sqlite_get_options(Oid foreigntableid);

/* depare.c headers */
extern void sqliteDeparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
										  List *tlist, List *remote_conds, List *pathkeys,
										  bool has_final_sort, bool has_limit, bool is_subquery,
										  List **retrieved_attrs, List **params_list);
extern void sqlite_deparse_insert(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs);
extern void sqlite_deparse_update(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, List *attname);
extern void sqlite_deparse_delete(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *name);
extern void sqlite_append_where_clause(StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, List *exprs,
									   bool is_first, List **params);
extern void sqlite_deparse_analyze(StringInfo buf, char *dbname, char *relname);
extern void sqlite_deparse_string_literal(StringInfo buf, const char *val);
extern List *sqlite_build_tlist_to_deparse(RelOptInfo *foreignrel);
int			sqlite_set_transmission_modes(void);
void		sqlite_reset_transmission_modes(int nestlevel);

/* connection.c headers */
sqlite3    *sqlite_get_connection(ForeignServer *server);
sqlite3    *sqlite_connect(char *svr_address, char *svr_username, char *svr_password, char *svr_database,
						   int svr_port, bool svr_sa, char *svr_init_command,
						   char *ssl_key, char *ssl_cert, char *ssl_ca, char *ssl_capath,
						   char *ssl_cipher);
void		sqlite_cleanup_connection(void);
void		sqlite_rel_connection(sqlite3 * conn);
void		sqlitefdw_report_error(int elevel, sqlite3_stmt * stmt, sqlite3 * conn, const char *sql, int rc);

Datum		sqlite_convert_to_pg(Oid pgtyp, int pgtypmod, sqlite3_stmt * stmt, int attnum);

void		sqlite_bind_sql_var(Oid type, int attnum, Datum value, sqlite3_stmt * stmt, bool *isnull);
#endif							/* duckdb_fdw_H */
