/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        duckdb_fdw.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef duckdb_fdw_H
#define duckdb_fdw_H
// #include "duckdb_shell_wrapper.h"
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

#include "funcapi.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/rel.h"
#include "funcapi.h"

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
#define exec_rt_fetch(rtindex, estate)	rt_fetch(rtindex, estate->es_range_table)
#endif

/* Code version is updated at new release. */
#define CODE_VERSION   10000

#if (PG_VERSION_NUM < 100000)
/*
 * Is the given relation a simple relation i.e a base or "other" member
 * relation?
 */
#define IS_SIMPLE_REL(rel) \
	((rel)->reloptkind == RELOPT_BASEREL || \
	 (rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)

/* Is the given relation a join relation? */
#define IS_JOIN_REL(rel)	\
	((rel)->reloptkind == RELOPT_JOINREL)

/* Is the given relation an upper relation? */
#define IS_UPPER_REL(rel)	\
	((rel)->reloptkind == RELOPT_UPPER_REL)

/* Is the given relation an "other" relation? */
#define IS_OTHER_REL(rel) \
	((rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#endif

#if PG_VERSION_NUM < 130000
#define list_concat(X, Y)  list_concat(X, list_copy(Y))
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

/* Struct for extra information passed to sqlite_estimate_path_cost_size() */
typedef struct SqliteFdwPathExtraData
{
	PathTarget *target;
	bool		has_final_sort;
	bool		has_limit;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
}			SqliteFdwPathExtraData;


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
	TupleDesc	tupdesc;		/* tuple descriptor of scan */
	AttInMetadata *attinmeta;	/* attribute datatype conversion */
	List	   *retrieved_attrs;	/* list of target attribute numbers */

	bool		cursor_exists;	/* have we created the cursor? */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	Oid		   *param_types;	/* type of query parameters */

	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */

	/* batch operation stuff */
	int			num_slots;		/* number of slots to insert */

	char	   *orig_query;		/* original text of INSERT command */
	List	   *target_attrs;	/* list of target attribute numbers */
	int			values_end;		/* length up to the end of VALUES */

	sqlite_opt *sqliteFdwOptions;	/* SQLite FDW options */

	List	   *attr_list;		/* query attribute list */
	List	   *column_list;	/* Column list of SQLite Column structures */

	int64		row_nums;		/* number of rows */
	Datum	  **rows;			/* all rows of scan */
	int64		rowidx;			/* current index of rows */
	bool	  **rows_isnull;	/* is null */
	bool		for_update;		/* true if this scan is update target */
	int			batch_size;		/* value of FDW option "batch_size" */

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

	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List	   *final_remote_exprs;

	/* Estimated size and cost for a scan or join. */
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Costs excluding costs for transferring data from the foreign server */
	double		retrieved_rows;
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

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Selectivity of join conditions */
	Selectivity joinclause_sel;

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
	UserMapping *user;			/* only set in use_remote_estimate mode */

	int			fetch_size;		/* fetch size for this remote table */

	/*
	 * Name of the relation while EXPLAINing ForeignScan. It is used for join
	 * relations but is set for all relations. For join relation, the name
	 * indicates which foreign tables are being joined and the join type used.
	 */
	char	   *relation_name;

	/* Grouping information */
	List	   *grouped_tlist;

	/* Subquery information */
	bool		make_outerrel_subquery; /* do we deparse outerrel as a
										 * subquery? */
	bool		make_innerrel_subquery; /* do we deparse innerrel as a
										 * subquery? */
	Relids		lower_subquery_rels;	/* all relids appearing in lower
										 * subqueries */

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;

	/* Function pushdown surppot in target list */
	bool		is_tlist_func_pushdown;
}			SqliteFdwRelationInfo;

/*
 * Execution state of a foreign scan that modifies a foreign table directly.
 */
typedef struct SqliteFdwDirectModifyState
{
	Relation	rel;			/* relcache entry for the foreign table */
	AttInMetadata *attinmeta;	/* attribute datatype conversion metadata */

	/* extracted fdw_private data */
	char	   *query;			/* text of UPDATE/DELETE command */
	bool		has_returning;	/* is there a RETURNING clause? */
	List	   *retrieved_attrs;	/* attr numbers retrieved by RETURNING */
	bool		set_processed;	/* do we set the command es_processed? */

	/* for remote query execution */
	sqlite3    *conn;			/* connection for the update */
	sqlite3_stmt *stmt;			/* SQLite prepared stament handle */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	Oid		   *param_types;	/* type of query parameters */

	/* for storing result tuples */
	int			num_tuples;		/* # of result tuples */
	int			next_tuple;		/* index of next one to return */
	Relation	resultRel;		/* relcache entry for the target relation */
	AttrNumber *attnoMap;		/* array of attnums of input user columns */
	AttrNumber	ctidAttno;		/* attnum of input ctid column */
	AttrNumber	oidAttno;		/* attnum of input oid column */
	bool		hasSystemCols;	/* are there system columns of resultRel? */

	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
}			SqliteFdwDirectModifyState;

extern bool sqlite_is_foreign_expr(PlannerInfo *root,
								   RelOptInfo *baserel,
								   Expr *expr);
extern bool sqlite_is_foreign_param(PlannerInfo *root,
									RelOptInfo *baserel,
									Expr *expr);
extern bool sqlite_is_foreign_function_tlist(PlannerInfo *root,
											 RelOptInfo *baserel,
											 List *tlist);

extern Expr *sqlite_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern Expr *sqlite_find_em_expr_for_input_target(PlannerInfo *root,
												  EquivalenceClass *ec,
												  PathTarget *target,
												  RelOptInfo *fallbackRel);

/* in duckdb_fdw.c */
extern int	sqlite_set_transmission_modes(void);
extern void sqlite_reset_transmission_modes(int nestlevel);

/* option.c headers */
extern sqlite_opt * sqlite_get_options(Oid foreigntableid);

/* depare.c headers */
extern void sqlite_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
											   List *tlist, List *remote_conds, List *pathkeys,
											   bool has_final_sort, bool has_limit, bool is_subquery,
											   List **retrieved_attrs, List **params_list);
extern void sqlite_deparse_insert(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, bool doNothing, int *values_end_len);
#if PG_VERSION_NUM >= 140000
extern void sqlite_rebuild_insert(StringInfo buf, Relation rel, char *orig_query, List *target_attrs, int values_end_len, int num_params, int num_rows);
extern void sqlite_deparse_truncate(StringInfo buf, List *rels);
#endif
extern void sqlite_deparse_update(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, List *attname);
extern void sqlite_deparse_direct_update_sql(StringInfo buf, PlannerInfo *root,
											 Index rtindex, Relation rel,
											 RelOptInfo *foreignrel,
											 List *targetlist,
											 List *targetAttrs,
											 List *remote_conds,
											 List **params_list,
											 List **retrieved_attrs);
extern void sqlite_deparse_delete(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *name);
extern void sqlite_deparse_direct_delete_sql(StringInfo buf, PlannerInfo *root,
											 Index rtindex, Relation rel,
											 RelOptInfo *foreignrel,
											 List *remote_conds,
											 List **params_list,
											 List **retrieved_attrs);
extern void sqlite_append_where_clause(StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, List *exprs,
									   bool is_first, List **params);
extern void sqlite_deparse_analyze(StringInfo buf, char *dbname, char *relname);
extern void sqlite_deparse_string_literal(StringInfo buf, const char *val);
extern List *sqlite_build_tlist_to_deparse(RelOptInfo *foreignrel);
int			sqlite_set_transmission_modes(void);
void		sqlite_reset_transmission_modes(int nestlevel);
extern const char *sqlite_get_jointype_name(JoinType jointype);
extern void sqlite_classify_conditions(PlannerInfo *root,
									   RelOptInfo *baserel,
									   List *input_conds,
									   List **remote_conds,
									   List **local_conds);

/* connection.c headers */
sqlite3    *sqlite_get_connection(ForeignServer *server, bool truncatable);
sqlite3    *sqlite_connect(char *svr_address, char *svr_username, char *svr_password, char *svr_database,
						   int svr_port, bool svr_sa, char *svr_init_command,
						   char *ssl_key, char *ssl_cert, char *ssl_ca, char *ssl_capath,
						   char *ssl_cipher);
void		sqlite_cleanup_connection(void);
void		sqlite_rel_connection(sqlite3 * conn);
void		sqlitefdw_report_error(int elevel, sqlite3_stmt * stmt, sqlite3 * conn, const char *sql, int rc);
void		sqlite_cache_stmt(ForeignServer *server, sqlite3_stmt * *stmt);

Datum		sqlite_convert_to_pg(Oid pgtyp, int pgtypmod, sqlite3_stmt * stmt, int attnum, AttInMetadata *attinmeta);

void		sqlite_bind_sql_var(Oid type, int attnum, Datum value, sqlite3_stmt * stmt, bool *isnull);
extern void sqlite_do_sql_command(sqlite3 * conn, const char *sql, int level);
#endif							/* duckdb_fdw_H */
