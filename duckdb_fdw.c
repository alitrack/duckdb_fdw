/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        duckdb_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "duckdb_fdw.h"

#include "sqlite3.h"
#include <stdio.h>

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/appendinfo.h"
#endif
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/cost.h"
#if (PG_VERSION_NUM >= 130010 && PG_VERSION_NUM < 140000) || \
	(PG_VERSION_NUM >= 140007 && PG_VERSION_NUM < 150000) || \
	(PG_VERSION_NUM >= 150002)
#include "optimizer/inherit.h"
#endif
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "storage/ipc.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"

extern PGDLLEXPORT void _PG_init(void);

bool sqlite_load_library(void);
static void duckdb_fdw_exit(int code, Datum arg);

PG_MODULE_MAGIC;

/* The number of default estimated rows for table which does not exist in sqlite1_stat1
 * See sqlite3ResultSetOfSelect in select.c of SQLite
 */
#define DEFAULT_ROW_ESTIMATE 1000000
#define DEFAULTE_NUM_ROWS 1000
#define IS_KEY_COLUMN(A) ((strcmp(A->defname, "key") == 0) && \
						  (strcmp(strVal(A->arg), "true") == 0))
/* Default CPU cost to start up a foreign query. */
#define DEFAULT_FDW_STARTUP_COST 100.0

/* Default CPU cost to process 1 row (above and beyond cpu_tuple_cost). */
#define DEFAULT_FDW_TUPLE_COST 0.01

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * This enum describes what's kept in the fdw_private list for a ForeignPath.
 * We store:
 *
 * 1) Boolean flag showing if the remote query has the final sort
 * 2) Boolean flag showing if the remote query has the LIMIT clause
 */
enum FdwPathPrivateIndex
{
	/* has-final-sort flag (as an integer Value node) */
	FdwPathPrivateHasFinalSort,
	/* has-limit flag (as an integer Value node) */
	FdwPathPrivateHasLimit
};

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing UPDATE/DELETE target */
	FdwScanPrivateForUpdate,
#if (PG_VERSION_NUM < 100000)
	/* rtindex */
	FdwScanPrivateRtIndex,
#endif

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a duckdb_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Length till the end of VALUES clause for INSERT
 *	  (-1 for a DELETE/UPDATE)
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* Length till the end of VALUES clause (as an integer Value node) */
	FdwModifyPrivateLen
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ForeignScan node that modifies a foreign table directly.  We store:
 *
 * 1) UPDATE/DELETE statement text to be sent to the remote server
 * 2) Boolean flag showing if the remote query has a RETURNING clause
 * 3) Integer list of attribute numbers retrieved by RETURNING, if any
 * 4) Boolean flag showing if we set the command es_processed
 */
enum FdwDirectModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwDirectModifyPrivateUpdateSql,
	/* has-returning flag (as an integer Value node) */
	FdwDirectModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwDirectModifyPrivateRetrievedAttrs,
	/* set-processed flag (as an integer Value node) */
	FdwDirectModifyPrivateSetProcessed
};

extern PGDLLEXPORT Datum duckdb_fdw_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(duckdb_fdw_handler);
PG_FUNCTION_INFO_V1(duckdb_fdw_version);
PG_FUNCTION_INFO_V1(duckdb_execute);

static void sqliteGetForeignRelSize(PlannerInfo *root,
									RelOptInfo *baserel,
									Oid foreigntableid);

static void sqliteGetForeignPaths(PlannerInfo *root,
								  RelOptInfo *baserel,
								  Oid foreigntableid);

static ForeignScan *sqliteGetForeignPlan(PlannerInfo *root,
										 RelOptInfo *baserel,
										 Oid foreigntableid,
										 ForeignPath *best_path,
										 List *tlist,
										 List *scan_clauses,
										 Plan *outer_plan);

static void sqliteBeginForeignScan(ForeignScanState *node,
								   int eflags);

static TupleTableSlot *sqliteIterateForeignScan(ForeignScanState *node);

static void sqliteReScanForeignScan(ForeignScanState *node);

static void sqliteEndForeignScan(ForeignScanState *node);

static void sqliteAddForeignUpdateTargets(
#if (PG_VERSION_NUM >= 140000)
	PlannerInfo *root,
	Index rtindex,
#else
	Query *parsetree,
#endif
	RangeTblEntry *target_rte,
	Relation target_relation);

static List *sqlitePlanForeignModify(PlannerInfo *root,
									 ModifyTable *plan,
									 Index resultRelation,
									 int subplan_index);

static void sqliteBeginForeignModify(ModifyTableState *mtstate,
									 ResultRelInfo *rinfo,
									 List *fdw_private,
									 int subplan_index,
									 int eflags);

static TupleTableSlot *sqliteExecForeignInsert(EState *estate,
											   ResultRelInfo *rinfo,
											   TupleTableSlot *slot,
											   TupleTableSlot *planSlot);
#if PG_VERSION_NUM >= 140000
static TupleTableSlot **sqliteExecForeignBatchInsert(EState *estate,
													 ResultRelInfo *resultRelInfo,
													 TupleTableSlot **slots,
													 TupleTableSlot **planSlots,
													 int *numSlots);
static int sqliteGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo);
#endif
static TupleTableSlot *sqliteExecForeignUpdate(EState *estate,
											   ResultRelInfo *rinfo,
											   TupleTableSlot *slot,
											   TupleTableSlot *planSlot);

static TupleTableSlot *sqliteExecForeignDelete(EState *estate,
											   ResultRelInfo *rinfo,
											   TupleTableSlot *slot,
											   TupleTableSlot *planSlot);

static void sqliteEndForeignModify(EState *estate,
								   ResultRelInfo *rinfo);

#if (PG_VERSION_NUM >= 110000)
static void sqliteEndForeignInsert(EState *estate,
								   ResultRelInfo *resultRelInfo);
static void sqliteBeginForeignInsert(ModifyTableState *mtstate,
									 ResultRelInfo *resultRelInfo);
#endif

static void sqliteExplainForeignScan(ForeignScanState *node,
									 struct ExplainState *es);

static void sqliteExplainForeignModify(ModifyTableState *mtstate,
									   ResultRelInfo *rinfo,
									   List *fdw_private,
									   int subplan_index,
									   struct ExplainState *es);
static bool sqlitePlanDirectModify(PlannerInfo *root,
								   ModifyTable *plan,
								   Index resultRelation,
								   int subplan_index);
static void sqliteBeginDirectModify(ForeignScanState *node, int eflags);
static TupleTableSlot *sqliteIterateDirectModify(ForeignScanState *node);
static void sqliteEndDirectModify(ForeignScanState *node);
static void sqliteExplainDirectModify(ForeignScanState *node,
									  struct ExplainState *es);
#if PG_VERSION_NUM >= 140000
static void sqliteExecForeignTruncate(List *rels,
									  DropBehavior behavior,
									  bool restart_seqs);
#endif
static bool sqliteAnalyzeForeignTable(Relation relation,
									  AcquireSampleRowsFunc *func,
									  BlockNumber *totalpages);

static List *sqliteImportForeignSchema(ImportForeignSchemaStmt *stmt,
									   Oid serverOid);

static void sqliteGetForeignJoinPaths(PlannerInfo *root,
									  RelOptInfo *joinrel,
									  RelOptInfo *outerrel,
									  RelOptInfo *innerrel,
									  JoinType jointype,
									  JoinPathExtraData *extra);

static void
sqliteGetForeignUpperPaths(PlannerInfo *root,
						   UpperRelationKind stage,
						   RelOptInfo *input_rel,
						   RelOptInfo *output_rel
#if (PG_VERSION_NUM >= 110000)
						   ,
						   void *extra
#endif
);

static void sqlite_prepare_wrapper(ForeignServer *server,
								   sqlite3 *db, char *query,
								   sqlite3_stmt **result,
								   const char **pzTail,
								   bool is_cache);
static void sqlite_to_pg_type(StringInfo str, char *typname);

static TupleTableSlot **sqlite_execute_insert(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  CmdType operation,
											  TupleTableSlot **slots,
											  TupleTableSlot **planSlots,
											  int *numSlots);

static void sqlite_prepare_query_params(PlanState *node,
										List *fdw_exprs,
										int numParams,
										FmgrInfo **param_flinfo,
										List **param_exprs,
										const char ***param_values,
										Oid **param_types);

static void sqlite_process_query_params(ExprContext *econtext,
										FmgrInfo *param_flinfo,
										List *param_exprs,
										const char **param_values,
										sqlite3_stmt **stmt,
										Oid *param_types);

static void sqlite_create_cursor(ForeignScanState *node);
static void sqlite_execute_dml_stmt(ForeignScanState *node);
static void sqlite_merge_fdw_options(SqliteFdwRelationInfo *fpinfo,
									 const SqliteFdwRelationInfo *fpinfo_o,
									 const SqliteFdwRelationInfo *fpinfo_i);
static bool sqlite_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel);
static void sqlite_add_foreign_grouping_paths(PlannerInfo *root,
											  RelOptInfo *input_rel,
											  RelOptInfo *grouped_rel
#if (PG_VERSION_NUM >= 110000)
											  ,
											  GroupPathExtraData *extra
#endif
);
static void sqlite_add_foreign_ordered_paths(PlannerInfo *root,
											 RelOptInfo *input_rel,
											 RelOptInfo *ordered_rel);
static void sqlite_add_foreign_final_paths(PlannerInfo *root,
										   RelOptInfo *input_rel,
										   RelOptInfo *final_rel
#if (PG_VERSION_NUM >= 120000)
										   ,
										   FinalPathExtraData *extra
#endif
);

static void sqlite_get_remote_estimate(const char *sql,
									   sqlite3 *conn,
									   double *rows,
									   int *width,
									   Cost *startup_cost,
									   Cost *total_cost);
static void sqlite_estimate_path_cost_size(PlannerInfo *root,
										   RelOptInfo *foreignrel,
										   List *param_join_conds,
										   List *pathkeys,
										   SqliteFdwPathExtraData *fpextra,
										   double *p_rows, int *p_width,
										   Cost *p_startup_cost, Cost *p_total_cost);
static bool sqlite_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
								   JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel,
								   JoinPathExtraData *extra);
static void sqlite_adjust_foreign_grouping_path_cost(PlannerInfo *root,
													 List *pathkeys,
													 double retrieved_rows,
													 double width,
													 double limit_tuples,
													 Cost *p_startup_cost,
													 Cost *p_run_cost);
static bool sqlite_ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
											 EquivalenceClass *ec, EquivalenceMember *em,
											 void *arg);
static bool sqlite_all_baserels_are_foreign(PlannerInfo *root);

static void sqlite_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, List *fdw_private, Path *epq_path);
static List *sqlite_get_useful_pathkeys_for_relation(PlannerInfo *root,
													 RelOptInfo *rel);
#if PG_VERSION_NUM >= 140000
static int sqlite_get_batch_size_option(Relation rel);
#endif

static const char * get_remote_catalog(ForeignServer *server, sqlite3 *db);

/* Callback argument for sqlite_ec_member_matches_foreign */
typedef struct
{
	Expr *current;		/* current expr, or NULL if not yet found */
	List *already_used; /* expressions already dealt with */
} ec_member_foreign_arg;

/*
 * Library load-time initialization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void _PG_init(void)
{
	on_proc_exit(&duckdb_fdw_exit, PointerGetDatum(NULL));
}

/*
 * duckdb_fdw_exit: Exit callback function.
 */
static void
duckdb_fdw_exit(int code, Datum arg)
{
	sqlite_cleanup_connection();
}

Datum duckdb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	fdwroutine->GetForeignRelSize = sqliteGetForeignRelSize;
	fdwroutine->GetForeignPaths = sqliteGetForeignPaths;
	fdwroutine->GetForeignPlan = sqliteGetForeignPlan;

	fdwroutine->BeginForeignScan = sqliteBeginForeignScan;
	fdwroutine->IterateForeignScan = sqliteIterateForeignScan;
	fdwroutine->ReScanForeignScan = sqliteReScanForeignScan;
	fdwroutine->EndForeignScan = sqliteEndForeignScan;

	fdwroutine->AddForeignUpdateTargets = sqliteAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = sqlitePlanForeignModify;
	fdwroutine->BeginForeignModify = sqliteBeginForeignModify;
	fdwroutine->ExecForeignInsert = sqliteExecForeignInsert;
#if PG_VERSION_NUM >= 140000
	fdwroutine->ExecForeignBatchInsert = sqliteExecForeignBatchInsert;
	fdwroutine->GetForeignModifyBatchSize = sqliteGetForeignModifyBatchSize;
#endif
	fdwroutine->ExecForeignUpdate = sqliteExecForeignUpdate;
	fdwroutine->ExecForeignDelete = sqliteExecForeignDelete;
	fdwroutine->EndForeignModify = sqliteEndForeignModify;
#if (PG_VERSION_NUM >= 110000)
	fdwroutine->BeginForeignInsert = sqliteBeginForeignInsert;
	fdwroutine->EndForeignInsert = sqliteEndForeignInsert;
#endif

	/* Support functions for join push-down */
	fdwroutine->GetForeignJoinPaths = sqliteGetForeignJoinPaths;

	/* support for EXPLAIN */
	fdwroutine->ExplainForeignScan = sqliteExplainForeignScan;
	fdwroutine->ExplainForeignModify = sqliteExplainForeignModify;
	fdwroutine->ExplainDirectModify = sqliteExplainDirectModify;
#if PG_VERSION_NUM >= 140000
	/* Support function for TRUNCATE */
	fdwroutine->ExecForeignTruncate = sqliteExecForeignTruncate;
#endif
	/* suport for Direct Modification */
	fdwroutine->PlanDirectModify = sqlitePlanDirectModify;
	fdwroutine->BeginDirectModify = sqliteBeginDirectModify;
	fdwroutine->IterateDirectModify = sqliteIterateDirectModify;
	fdwroutine->EndDirectModify = sqliteEndDirectModify;

	/* support for ANALYSE */
	fdwroutine->AnalyzeForeignTable = sqliteAnalyzeForeignTable;

	/* support for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = sqliteImportForeignSchema;

	/* Support functions for upper relation push-down */
	fdwroutine->GetForeignUpperPaths = sqliteGetForeignUpperPaths;

	PG_RETURN_POINTER(fdwroutine);
}

Datum duckdb_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(sqlite3_libversion_number());
}

/* Wrapper for sqlite3_prepare */
static void
sqlite_prepare_wrapper(ForeignServer *server, sqlite3 *db, char *query, sqlite3_stmt **stmt,
					   const char **pzTail, bool is_cache)
{
	int rc;
	// db = sqlite_get_connection(server, false);

	// elog(DEBUG1, "duckdb_fdw : %s %s %p %p %p %p\n", __func__, query, server,db,&stmt,stmt);

	rc = sqlite3_prepare_v2(db, query, -1, stmt, pzTail);
	// elog(DEBUG1, "duckdb_fdw : %s %s %d \n", __func__, query, rc);
	if (rc != SQLITE_OK)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("SQL error during prepare: %s %s", sqlite3_errmsg(db), query)));
	}
	/* cache stmt to finalize at last */
	if (is_cache)
		sqlite_cache_stmt(server, stmt);
}

/*
 * sqliteGetForeignRelSize: Create a FdwPlan for a scan on the foreign table
 */
static void
sqliteGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	SqliteFdwRelationInfo *fpinfo;
	ListCell *lc;
#if PG_VERSION_NUM < 160000
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
#endif

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	fpinfo = (SqliteFdwRelationInfo *)palloc0(sizeof(SqliteFdwRelationInfo));
	baserel->fdw_private = (void *)fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;
	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Extract user-settable option values.  Note that per-table setting of
	 * use_remote_estimate overrides per-server setting.
	 */
	fpinfo->use_remote_estimate = false;
	fpinfo->fdw_startup_cost = DEFAULT_FDW_STARTUP_COST;
	fpinfo->fdw_tuple_cost = DEFAULT_FDW_TUPLE_COST;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * identify which user to do remote access as during planning.  This
	 * should match what ExecCheckRTEPerms() does.  If we fail due to lack of
	 * permissions, the query would have failed at runtime anyway.
	 */
	if (fpinfo->use_remote_estimate)
	{
#if (PG_VERSION_NUM < 160000)
		Oid userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
#else
		Oid userid;
		userid = OidIsValid(baserel->userid) ? baserel->userid : GetUserId();
#endif

		fpinfo->user = GetUserMapping(userid, fpinfo->server->serverid);
	}
	else
		fpinfo->user = NULL;

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	sqlite_classify_conditions(root, baserel, baserel->baserestrictinfo,
							   &fpinfo->remote_conds, &fpinfo->local_conds);

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.
	 */
	fpinfo->attrs_used = NULL;
#if PG_VERSION_NUM >= 90600
	pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
#else
	pull_varattnos((Node *)baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
#endif

	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

		pull_varattnos((Node *)rinfo->clause, baserel->relid, &fpinfo->attrs_used);
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path.  The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 baserel->relid,
													 JOIN_INNER,
													 NULL);

	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to
	 * sqlite_estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction clauses, as well as the
	 * average row width.  Otherwise, estimate using whatever statistics we
	 * have locally, in a way similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		/*
		 * Get cost/size estimates with help of remote server.  Save the
		 * values in fpinfo so we don't need to do it again to generate the
		 * basic foreign path.
		 */
		sqlite_estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
									   &fpinfo->rows, &fpinfo->width,
									   &fpinfo->startup_cost, &fpinfo->total_cost);

		/* Report estimated baserel size to planner. */
		baserel->rows = fpinfo->rows;
		baserel->reltarget->width = fpinfo->width;
	}
	else
	{
		/*
		 * If the foreign table has never been ANALYZEd, it will have relpages
		 * and reltuples equal to zero, which most likely has nothing to do
		 * with reality.  We can't do a whole lot about that if we're not
		 * allowed to consult the remote server, but we can use a hack similar
		 * to plancat.c's treatment of empty relations: use a minimum size
		 * estimate of 10 pages, and divide by the column-datatype-based width
		 * estimate to get the corresponding number of tuples.
		 */
#if (PG_VERSION_NUM >= 140000)
		if (baserel->tuples < 0)
#else
		if (baserel->pages == 0 && baserel->tuples == 0)
#endif
		{
			baserel->pages = 10;
			baserel->tuples =
				(10 * BLCKSZ) / (baserel->reltarget->width +
								 MAXALIGN(SizeofHeapTupleHeader));
		}

		/*
		 * Estimate baserel size as best we can with local statistics.
		 */
		set_baserel_size_estimates(root, baserel);

		/* Fill in basically-bogus cost estimates for use later. */
		sqlite_estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
									   &fpinfo->rows, &fpinfo->width,
									   &fpinfo->startup_cost, &fpinfo->total_cost);
	}

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = psprintf("%u", baserel->relid);

	/* No outer and inner relations. */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	fpinfo->lower_subquery_rels = NULL;
	/* Set the relation index. */
	fpinfo->relation_index = baserel->relid;
}

/*
 * sqlite_get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
sqlite_get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List *useful_pathkeys_list = NIL;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *)rel->fdw_private;
	ListCell *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool query_pathkeys_ok = true;

		foreach (lc, root->query_pathkeys)
		{
			PathKey *pathkey = (PathKey *)lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr *em_expr;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * is_foreign_expr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			if (pathkey_ec->ec_has_volatile ||
				!(em_expr = sqlite_find_em_expr_for_rel(pathkey_ec, rel)) ||
				!sqlite_is_foreign_expr(root, rel, em_expr))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
		{
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
			fpinfo->qp_is_pushdown_safe = true;
		}
	}

	return useful_pathkeys_list;
}

static void
sqlite_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, List *fdw_private, Path *epq_path)
{
	List *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell *lc;
	double rows;
	Cost startup_cost;
	Cost total_cost;

	/* Use small cost to avoid calculating real cost size in SQLite */
	rows = startup_cost = total_cost = 10;

	useful_pathkeys_list = sqlite_get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach (lc, useful_pathkeys_list)
	{
		List *useful_pathkeys = lfirst(lc);
		Path *sorted_epq_path;

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys,
								   sorted_epq_path->pathkeys))
			sorted_epq_path = (Path *)
				create_sort_path(root,
								 rel,
								 sorted_epq_path,
								 useful_pathkeys,
								 -1.0);

		if (rel->reloptkind == RELOPT_BASEREL ||
			rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
			add_path(rel, (Path *)
							  create_foreignscan_path(root, rel,
													  NULL,
													  rows,
													  startup_cost,
													  total_cost,
													  useful_pathkeys,
#if (PG_VERSION_NUM >= 120000)
													  rel->lateral_relids,
#else
													  NULL, /* no outer rel either */
#endif
													  sorted_epq_path,
#if PG_VERSION_NUM >= 170000
													  NIL,/* no fdw_restrictinfo list */
#endif
													  fdw_private));
		else
			add_path(rel, (Path *)
#if PG_VERSION_NUM >= 120000
							  create_foreign_join_path(root, rel,
#else
							  create_foreignscan_path(root, rel,
#endif
													   NULL,
													   rows,
													   startup_cost,
													   total_cost,
													   useful_pathkeys,
#if (PG_VERSION_NUM >= 120000)
													   rel->lateral_relids,
#else
													  NULL, /* no outer rel either */
#endif
													   sorted_epq_path,
#if PG_VERSION_NUM >= 170000
													  NIL,/* no fdw_restrictinfo list */
#endif
													   fdw_private));
	}
}

/*
 * Check if any of the tables queried aren't foreign tables.
 * We use this function to add limit pushdownm fallback to sqlite
 * because if theres any non-foreign table, GetForeignUpperPath its not called from planner.c
 */
static bool
sqlite_all_baserels_are_foreign(PlannerInfo *root)
{
	bool allTablesQueriedAreForeign = true;
	ListCell *l;

	/*
	 * If there is no append_rel_list, we assume we're only consulting a
	 * foreign table, so default value it's true and we dont need to do more.
	 */
	foreach (l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, l);
		int childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		if (!(IS_DUMMY_REL(childrel) || childRTE->inh))
		{
			if (!(childrel->rtekind == RTE_RELATION && childRTE->relkind == RELKIND_FOREIGN_TABLE))
			{
				allTablesQueriedAreForeign = false;
				break;
			}
		}
	}

	return allTablesQueriedAreForeign;
}

/*
 * sqliteGetForeignPaths
 *		Create possible scan paths for a scan on the foreign table
 */
static void
sqliteGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *)baserel->fdw_private;
	ForeignPath *path;
	List *ppi_list;
	ListCell *lc;
	Cost startup_cost = 10;
	Cost total_cost = baserel->rows + startup_cost;
	List *fdw_private = NIL;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	/* Estimate costs */
	total_cost = baserel->rows;

	/*
	 * We add fdw_private with has_limit: true if these three conditions are
	 * true because we need to be able to pushdown limit in this case: - Query
	 * has LIMIT - Query don't have OFFSET because if we pusdown OFFSET and
	 * later, we re-applying offset with the "final result", and we would be
	 * "jumping/skipping" child results and losing registries that we wanted
	 * to show. - Some of the baserels are not a foreign table, so PostgreSQL
	 * is not calling GetForeignUpperPaths
	 */
	if (limit_needed(root->parse) && !root->parse->limitOffset && !sqlite_all_baserels_are_foreign(root))
		fdw_private = list_make2(makeInteger(false), makeInteger(true));

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
						  create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
												  NULL, /* default pathtarget */
#endif
												  baserel->rows,
												  startup_cost,
												  total_cost,
												  NIL, /* no pathkeys */
#if (PG_VERSION_NUM >= 120000)
												  baserel->lateral_relids,
#else
												  NULL, /* no outer rel either */
#endif
												  NULL, /* no extra plan */
#if PG_VERSION_NUM >= 170000
												  NIL,/* no fdw_restrictinfo list */
#endif												  
												  fdw_private));

	/* Add paths with pathkeys */
	sqlite_add_paths_with_pathkeys_for_rel(root, baserel, fdw_private, NULL);

	/*
	 * If we're not using remote estimates, stop here.  We have no way to
	 * estimate whether any join clauses would be worth sending across, so
	 * don't bother building parameterized paths.
	 */
	if (!fpinfo->use_remote_estimate)
		return;

	/*
	 * Thumb through all join clauses for the rel to identify which outer
	 * relations could supply one or more safe-to-send-to-remote join clauses.
	 * We'll build a parameterized path for each such outer relation.
	 *
	 * It's convenient to manage this by representing each candidate outer
	 * relation by the ParamPathInfo node for it.  We can then use the
	 * ppi_clauses list in the ParamPathInfo node directly as a list of the
	 * interesting join clauses for that rel.  This takes care of the
	 * possibility that there are multiple safe join clauses for such a rel,
	 * and also ensures that we account for unsafe join clauses that we'll
	 * still have to enforce locally (since the parameterized-path machinery
	 * insists that we handle all movable clauses).
	 */
	ppi_list = NIL;
	foreach (lc, baserel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
		Relids required_outer;
		ParamPathInfo *param_info;

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, baserel))
			continue;

		/* See if it is safe to send to remote */
		if (!sqlite_is_foreign_expr(root, baserel, rinfo->clause))
			continue;

		/* Calculate required outer rels for the resulting path */
		required_outer = bms_union(rinfo->clause_relids,
								   baserel->lateral_relids);

		/*
		 * We do not want the foreign rel itself listed in required_outer
		 */
		required_outer = bms_del_member(required_outer, baserel->relid);

		/*
		 * required_outer probably can't be empty here, but if it were, we
		 * couldn't make a parameterized path.
		 */
		if (bms_is_empty(required_outer))
			continue;

		/* Get the ParamPathInfo */
		param_info = get_baserel_parampathinfo(root, baserel,
											   required_outer);
		Assert(param_info != NULL);

		/*
		 * Add it to list unless we already have it.  Testing pointer equality
		 * is OK since get_baserel_parampathinfo won't make duplicates.
		 */
		ppi_list = list_append_unique_ptr(ppi_list, param_info);
	}

	/*
	 * The above scan examined only "generic" join clauses, not those that
	 * were absorbed into EquivalenceClauses.  See if we can make anything out
	 * of EquivalenceClauses.
	 */
	if (baserel->has_eclass_joins)
	{
		/*
		 * We repeatedly scan the eclass list looking for column references
		 * (or expressions) belonging to the foreign rel. Each time we find
		 * one, we generate a list of equivalence joinclauses for it, and then
		 * see if any are safe to send to the remote.  Repeat till there are
		 * no more candidate EC members.
		 */
		ec_member_foreign_arg arg;

		arg.already_used = NIL;
		for (;;)
		{
			List *clauses;

			/*
			 * Make clauses, skipping any that join to lateral_referencers
			 */
			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root,
															 baserel,
															 sqlite_ec_member_matches_foreign,
															 (void *)&arg,
															 baserel->lateral_referencers);

			/*
			 * Done if there are no more expressions in the foreign rel
			 */
			if (arg.current == NULL)
			{
				Assert(clauses == NIL);
				break;
			}

			/* Scan the extracted join clauses */
			foreach (lc, clauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
				Relids required_outer;
				ParamPathInfo *param_info;

				/* Check if clause can be moved to this rel */
				if (!join_clause_is_movable_to(rinfo, baserel))
					continue;

				/* See if it is safe to send to remote */
				if (!sqlite_is_foreign_expr(root, baserel, rinfo->clause))
					continue;

				/*
				 * Calculate required outer rels for the resulting path
				 */
				required_outer = bms_union(rinfo->clause_relids,
										   baserel->lateral_relids);
				required_outer = bms_del_member(required_outer, baserel->relid);
				if (bms_is_empty(required_outer))
					continue;

				/* Get the ParamPathInfo */
				param_info = get_baserel_parampathinfo(root, baserel,
													   required_outer);
				Assert(param_info != NULL);

				/* Add it to list unless we already have it */
				ppi_list = list_append_unique_ptr(ppi_list, param_info);
			}

			/*
			 * Try again, now ignoring the expression we found this time
			 */
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}

	/*
	 * Now build a path for each useful outer relation.
	 */
	foreach (lc, ppi_list)
	{
		ParamPathInfo *param_info = (ParamPathInfo *)lfirst(lc);
		double rows;
		int width;

		/* Get a cost estimate from the remote */
		sqlite_estimate_path_cost_size(root, baserel,
									   param_info->ppi_clauses, NIL, NULL,
									   &rows, &width,
									   &startup_cost, &total_cost);

		/*
		 * ppi_rows currently won't get looked at by anything, but still we
		 * may as well ensure that it matches our idea of the rowcount.
		 */
		param_info->ppi_rows = rows;

		/* Make the path */
		path = create_foreignscan_path(root, baserel,
									   NULL, /* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, /* no pathkeys */
									   param_info->ppi_req_outer,
									   NULL,
#if PG_VERSION_NUM >= 170000
									  NIL,/* no fdw_restrictinfo list */
#endif	
									   NIL); /* no fdw_private list */
		add_path(baserel, (Path *)path);
	}
}

/*
 * sqliteGetForeignPlan: Get a foreign scan plan node
 */
static ForeignScan *
sqliteGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *)baserel->fdw_private;
	Index scan_relid = baserel->relid;
	List *fdw_private;
	List *local_exprs = NULL;
	List *remote_exprs = NULL;
	List *params_list = NULL;
	List *fdw_scan_tlist = NIL;
	List *remote_conds = NIL;

	StringInfoData sql;
	bool has_final_sort = false;
	bool has_limit = false;
	List *retrieved_attrs;
	ListCell *lc;
	List *fdw_recheck_quals = NIL;
	int for_update;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/* Decide to execute function pushdown support in the target list. */
	fpinfo->is_tlist_func_pushdown = sqlite_is_foreign_function_tlist(root, baserel, tlist);

	/*
	 * Get FDW private data created by sqliteGetForeignUpperPaths(), if any.
	 */
	if (best_path->fdw_private)
	{
		has_final_sort = intVal(list_nth(best_path->fdw_private, FdwPathPrivateHasFinalSort));
		has_limit = intVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */

	/* Build the query */
	initStringInfo(&sql);

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by sqlite_classify_conditions are shown
	 * in fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
	 * scan_clauses list will be a join clause, which we have to check for
	 * remote-safety.
	 *
	 * Note: the join clauses we see here should be the exact same ones
	 * previously examined by sqliteGetForeignPaths.  Possibly it'd be worth
	 * passing forward the classification work done then, rather than
	 * repeating it here.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local execution.
	 * Note however that we only strip the RestrictInfo nodes from the
	 * local_exprs list, since appendWhereClause expects a list of
	 * RestrictInfos.
	 */
	if (IS_SIMPLE_REL(baserel) && fpinfo->is_tlist_func_pushdown == false)
	{
		foreach (lc, scan_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

			Assert(IsA(rinfo, RestrictInfo));

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
			{
				remote_conds = lappend(remote_conds, rinfo);
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			}
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (sqlite_is_foreign_expr(root, baserel, rinfo->clause))
			{
				remote_conds = lappend(remote_conds, rinfo);
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			}
			else
				local_exprs = lappend(local_exprs, rinfo->clause);

			/*
			 * For a base-relation scan, we have to support EPQ recheck, which
			 * should recheck all the remote quals.
			 */
			fdw_recheck_quals = remote_exprs;
		}
	}
	else
	{
		/*
		 * Join relation or upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		if (fpinfo->is_tlist_func_pushdown == false)
		{
			Assert(!scan_clauses);
		}

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see sqliteGetForeignJoinPaths().
		 * If we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the foreign server. */
		if (fpinfo->is_tlist_func_pushdown == true)
		{
			int next_resno = list_length(fdw_scan_tlist) + 1;

			foreach (lc, tlist)
			{
				TargetEntry *tlist_tle = lfirst_node(TargetEntry, lc);

				if (!IsA(tlist_tle->expr, Const))
				{
					TargetEntry *tle;

					tle = makeTargetEntry(copyObject(tlist_tle->expr),
										  next_resno++,
										  NULL,
										  false);
					fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
				}
			}

			foreach (lc, fpinfo->local_conds)
			{
				RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

				fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
												   pull_var_clause((Node *)rinfo->clause,
																   PVC_RECURSE_PLACEHOLDERS));
			}
		}
		else
		{
			fdw_scan_tlist = sqlite_build_tlist_to_deparse(baserel);
		}

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot. This is safe because all scans and
		 * joins support projection, so we never need to insert a Result node.
		 * Also, remove the local conditions from outer plan's quals, lest
		 * they will be evaluated twice, once by the local plan and once by
		 * the scan.
		 */
		if (outer_plan)
		{
			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(baserel->reloptkind != RELOPT_UPPER_REL);
			outer_plan->targetlist = fdw_scan_tlist;

			foreach (lc, local_exprs)
			{
				Join *join_plan = (Join *)outer_plan;
				Node *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.
				 */
				if (join_plan->jointype == JOIN_INNER)
					join_plan->joinqual = list_delete(join_plan->joinqual,
													  qual);
			}
		}
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	sqlite_deparse_select_stmt_for_rel(&sql, root, baserel, fdw_scan_tlist,
									   remote_exprs, best_path->path.pathkeys,
									   has_final_sort, has_limit, false,
									   &retrieved_attrs, &params_list);

	/* Remember remote_exprs for possible use by sqlitePlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	for_update = false;
	if (root->parse->commandType == CMD_UPDATE ||
		root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_INSERT)
	{
		/* Relation is UPDATE/DELETE target, so use FOR UPDATE */
		for_update = true;
	}

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */
	fdw_private = list_make3(makeString(sql.data), retrieved_attrs, makeInteger(for_update));
#if (PG_VERSION_NUM < 100000)
	fdw_private = lappend(fdw_private, makeInteger(root->all_baserels == NULL ? -2 : bms_next_member(root->all_baserels, -1)));
#endif
	if (IS_JOIN_REL(baserel) || IS_UPPER_REL(baserel))
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name));

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private,
							fdw_scan_tlist,
							fdw_recheck_quals,
							outer_plan);
}

#if PG_VERSION_NUM >= 140000
/*
 * Construct a tuple descriptor for the scan tuples handled by a foreign join.
 */
static TupleDesc
sqlite_get_tupdesc_for_join_scan_tuples(ForeignScanState *node)
{
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	EState *estate = node->ss.ps.state;
	TupleDesc tupdesc;
	int i;

	/*
	 * The core code has already set up a scan tuple slot based on
	 * fsplan->fdw_scan_tlist, and this slot's tupdesc is mostly good enough,
	 * but there's one case where it isn't.  If we have any whole-row row
	 * identifier Vars, they may have vartype RECORD, and we need to replace
	 * that with the associated table's actual composite type.  This ensures
	 * that when we read those ROW() expression values from the remote server,
	 * we can convert them to a composite type the local server knows.
	 */
	tupdesc = CreateTupleDescCopy(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor);
	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		Var *var;
		RangeTblEntry *rte;
		Oid reltype;

		/* Nothing to do if it's not a generic RECORD attribute */
		if (att->atttypid != RECORDOID || att->atttypmod >= 0)
			continue;

		/*
		 * If we can't identify the referenced table, do nothing.  This'll
		 * likely lead to failure later, but perhaps we can muddle through.
		 */
		var = (Var *)list_nth_node(TargetEntry, fsplan->fdw_scan_tlist, i)->expr;
		if (!IsA(var, Var) || var->varattno != 0)
			continue;
		rte = list_nth(estate->es_range_table, var->varno - 1);
		if (rte->rtekind != RTE_RELATION)
			continue;
		reltype = get_rel_type_id(rte->relid);
		if (!OidIsValid(reltype))
			continue;
		att->atttypid = reltype;
		/* shouldn't need to change anything else */
	}
	return tupdesc;
}
#endif

/*
 * sqliteBeginForeignScan: Initiate access to the database
 */
static void
sqliteBeginForeignScan(ForeignScanState *node, int eflags)
{
	sqlite3 *conn = NULL;
	SqliteFdwExecState *festate = NULL;
	EState *estate = node->ss.ps.state;
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	int numParams;
	ForeignTable *table;
	ForeignServer *server;
	RangeTblEntry *rte;
	int rtindex;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * Do nothing in EXPLAIN
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	festate = (SqliteFdwExecState *)palloc0(sizeof(SqliteFdwExecState));
	node->fdw_state = (void *)festate;
	festate->rowidx = 0;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
	{
		rtindex = bms_next_member(fsplan->fs_relids, -1);
#if (PG_VERSION_NUM < 100000)
		/* PostgreSQL version 9.6.x need to get rtindex from ForeignPlan */
		if (rtindex == -2)
			rtindex = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateRtIndex));
#endif
	}
	rte = exec_rt_fetch(rtindex, estate);

	/* Get info about foreign table. */
	festate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(rte->relid);
	server = GetForeignServer(table->serverid);

	/*
	 * Get the already connected connection, otherwise connect and get the
	 * connection handle.
	 */
	conn = sqlite_get_connection(server, false);

	/* Stash away the state info we have already */
	festate->query = strVal(list_nth(fsplan->fdw_private, FdwScanPrivateSelectSql));
	festate->retrieved_attrs = list_nth(fsplan->fdw_private, FdwScanPrivateRetrievedAttrs);
	festate->for_update = intVal(list_nth(fsplan->fdw_private, FdwScanPrivateForUpdate)) ? true : false;
	festate->conn = conn;
	festate->cursor_exists = false;

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (fsplan->scan.scanrelid > 0)
	{
		festate->rel = node->ss.ss_currentRelation;
		festate->tupdesc = RelationGetDescr(festate->rel);
	}
	else
	{
		festate->rel = NULL;
#if (PG_VERSION_NUM >= 140000)
		festate->tupdesc = sqlite_get_tupdesc_for_join_scan_tuples(node);
#else
		festate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
#endif
	}

	festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);

	/* Initialize the Sqlite statement */
	festate->stmt = NULL;

	/* Prepare Sqlite statement */
	sqlite_prepare_wrapper(server, festate->conn, festate->query, &festate->stmt, NULL, true);

	/* Prepare for output conversion of parameters used in remote query. */
	numParams = list_length(fsplan->fdw_exprs);
	festate->numParams = numParams;
	if (numParams > 0)
		sqlite_prepare_query_params((PlanState *)node,
									fsplan->fdw_exprs,
									numParams,
									&festate->param_flinfo,
									&festate->param_exprs,
									&festate->param_values,
									&festate->param_types);
}

static void
make_tuple_from_result_row(sqlite3_stmt *stmt,
						   TupleDesc tupleDescriptor,
						   List *retrieved_attrs,
						   Datum *row,
						   bool *is_null,
						   SqliteFdwExecState *festate)
{
	ListCell *lc = NULL;
	int attid = 0;

	memset(row, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset(is_null, true, sizeof(bool) * tupleDescriptor->natts);

	foreach (lc, retrieved_attrs)
	{
		int attnum = lfirst_int(lc) - 1;
		Oid pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
		int32 pgtypmod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;

		if (sqlite3_column_type(stmt, attid) != SQLITE_NULL)
		{
			is_null[attnum] = false;
			row[attnum] = sqlite_convert_to_pg(pgtype, pgtypmod,
											   stmt, attid, festate->attinmeta);
		}
		attid++;
	}
}

/*
 * sqliteIterateForeignScan: Iterate and get the rows one by one from
 * Sqlite and placed in tuple slot
 */
static TupleTableSlot *
sqliteIterateForeignScan(ForeignScanState *node)
{

	SqliteFdwExecState *festate = (SqliteFdwExecState *)node->fdw_state;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	EState *estate = node->ss.ps.state;
	TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	int rc = 0;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * If this is the first call after Begin or ReScan, we need to create the
	 * cursor on the remote side. Binding parameters is done in this function.
	 */
	if (!festate->cursor_exists)
		sqlite_create_cursor(node);

	ExecClearTuple(tupleSlot);

	/*
	 * We get all rows before starting update if this scan is for update
	 * because there is no isolation between update and select on the same
	 * database connections. Please see for details:
	 * https://sqlite.org/isolation.html
	 */
	if (festate->for_update && festate->rowidx == 0)
	{
		int size = 0;

		/* festate->rows need longer context than per tuple */
		MemoryContext oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		festate->row_nums = 0;
		festate->rowidx = 0;
		while (1)
		{
			rc = sqlite3_step(festate->stmt);
			if (rc == SQLITE_ROW)
			{

				if (size == 0)
				{
					size = 1;
					festate->rows = palloc(sizeof(Datum *) * size);
					festate->rows_isnull = palloc(sizeof(bool *) * size);
				}
				else if (festate->row_nums >= size)
				{
					/* expand array */
					size = size * 2;
					festate->rows = repalloc(festate->rows, sizeof(Datum *) * size);
					festate->rows_isnull = repalloc(festate->rows_isnull, sizeof(bool *) * size);
				}
				festate->rows[festate->row_nums] = palloc(sizeof(Datum) * tupleDescriptor->natts);
				festate->rows_isnull[festate->row_nums] = palloc(sizeof(bool) * tupleDescriptor->natts);
				make_tuple_from_result_row(festate->stmt,
										   tupleDescriptor, festate->retrieved_attrs,
										   festate->rows[festate->row_nums],
										   festate->rows_isnull[festate->row_nums],
										   festate);

				festate->row_nums++;
			}
			else if (SQLITE_DONE == rc)
			{
				/* No more rows/data exists */
				break;
			}
			else
			{
				sqlitefdw_report_error(ERROR, festate->stmt, festate->conn, NULL, rc);
			}
		}
		MemoryContextSwitchTo(oldcontext);
	}

	if (festate->for_update)
	{
		if (festate->rowidx < festate->row_nums)
		{
			memcpy(tupleSlot->tts_values, festate->rows[festate->rowidx], sizeof(Datum) * tupleDescriptor->natts);
			memcpy(tupleSlot->tts_isnull, festate->rows_isnull[festate->rowidx], sizeof(bool) * tupleDescriptor->natts);
			ExecStoreVirtualTuple(tupleSlot);
			festate->rowidx++;
		}
	}
	else
	{
		rc = sqlite3_step(festate->stmt);
		if (SQLITE_ROW == rc)
		{
			make_tuple_from_result_row(festate->stmt,
									   tupleDescriptor, festate->retrieved_attrs,
									   tupleSlot->tts_values, tupleSlot->tts_isnull, festate);
			ExecStoreVirtualTuple(tupleSlot);
		}
		else if (SQLITE_DONE == rc)
		{
			/* No more rows/data exists */
		}
		else
		{
			sqlitefdw_report_error(ERROR, festate->stmt, festate->conn, NULL, rc);
		}
	}
	return tupleSlot;
}

/*
 * sqliteEndForeignScan: Finish scanning foreign table and dispose
 * objects used for this scan
 */
static void
sqliteEndForeignScan(ForeignScanState *node)
{
	SqliteFdwExecState *festate = (SqliteFdwExecState *)node->fdw_state;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate == NULL)
		return;

	if (festate->stmt)
	{
		festate->stmt = NULL;
	}
}

/*
 * Restart the scan from the beginning. Note that any parameters the scan
 * depends on may have changed value, so the new scan does not necessarily
 * return exactly the same rows.
 */
static void
sqliteReScanForeignScan(ForeignScanState *node)
{

	SqliteFdwExecState *festate = (SqliteFdwExecState *)node->fdw_state;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	if (festate->stmt)
	{
		sqlite3_reset(festate->stmt);
	}
	festate->cursor_exists = false;
	festate->rowidx = 0;
}

/*
 * sqliteAddForeignUpdateTargets: Add column(s) needed for update/delete on a foreign table,
 * we are using first column as row identification column, so we are adding that into target
 * list.
 */
static void
sqliteAddForeignUpdateTargets(
#if (PG_VERSION_NUM >= 140000)
	PlannerInfo *root,
	Index rtindex,
#else
	Query *parsetree,
#endif
	RangeTblEntry *target_rte,
	Relation target_relation)
{

	Oid relid = RelationGetRelid(target_relation);
	TupleDesc tupdesc = target_relation->rd_att;
	int i;
	bool has_key = false;

	/* loop through all columns of the foreign table */
	for (i = 0; i < tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber attrno = att->attnum;
		List *options;
		ListCell *option;

		/* look for the "key" option on this column */
		options = GetForeignColumnOptions(relid, attrno);
		foreach (option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			/* if "key" is set, add a resjunk for this column */
			if (IS_KEY_COLUMN(def))
			{
				Var *var;
#if PG_VERSION_NUM < 140000
				Index rtindex = parsetree->resultRelation;
				TargetEntry *tle;
#endif
				var = makeVar(rtindex,
							  attrno,
							  att->atttypid,
							  att->atttypmod,
							  att->attcollation,
							  0);
#if (PG_VERSION_NUM >= 140000)
				add_row_identity_var(root, var, rtindex, pstrdup(NameStr(att->attname)));
#else
				/* Wrap it in a resjunk TLE with the right name ... */
				tle = makeTargetEntry((Expr *)var,
									  list_length(parsetree->targetList) + 1,
									  pstrdup(NameStr(att->attname)),
									  true);

				/* ... and add it to the query's targetlist */
				parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
				has_key = true;
			}
			else if (strcmp(def->defname, "key") == 0)
			{
				elog(ERROR, "impossible column option \"%s\"", def->defname);
			}
		}
	}

	if (!has_key)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("no primary key column specified for foreign table"),
				 errdetail("For UPDATE or DELETE, at least one foreign table column must be marked as primary key column."),
				 errhint("Set the option \"%s\" on the columns that belong to the primary key.", "key")));
}

static List *
sqlitePlanForeignModify(PlannerInfo *root,
						ModifyTable *plan,
						Index resultRelation,
						int subplan_index)
{
	CmdType operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation rel;
	List *targetAttrs = NULL;
	StringInfoData sql;
	Oid foreignTableId;
	TupleDesc tupdesc;
	int i;
	List *condAttr = NULL;
	bool doNothing = false;
	int values_end_len = -1;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	foreignTableId = RelationGetRelid(rel);
	tupdesc = RelationGetDescr(rel);

	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		int attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		AttrNumber attno;
#if (PG_VERSION_NUM >= 130010 && PG_VERSION_NUM < 140000) || \
	(PG_VERSION_NUM >= 140007 && PG_VERSION_NUM < 150000) || \
	(PG_VERSION_NUM >= 150002)
		int col;
		RelOptInfo *rel = find_base_rel(root, resultRelation);
		Bitmapset *allUpdatedCols = get_rel_all_updated_cols(root, rel);

		col = -1;
		while ((col = bms_next_member(allUpdatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			attno = col + FirstLowInvalidHeapAttributeNumber;
#else
		Bitmapset *tmpset;
		// tmpset = bms_union(rte->updatedCols, rte->extraUpdatedCols);
		#if PG_VERSION_NUM >= 120000
				tmpset = bms_union(rte->updatedCols, rte->extraUpdatedCols);
		#elif PG_VERSION_NUM >= 90500
				tmpset = rte->updatedCols;
		#else
				tmpset = bms_copy(rte->modifiedCols);
		#endif  /* PG_VERSION_NUM */

		while ((attno = bms_first_member(tmpset)) >= 0)
		{
			attno += FirstLowInvalidHeapAttributeNumber;
#endif
			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	if (plan->returningLists)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("RETURNING clause is not supported")));

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d",
			 (int)plan->onConflictAction);

	/*
	 * Add all primary key attribute names to condAttr used in where clause of
	 * update
	 */
	for (i = 0; i < tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber attrno = att->attnum;
		List *options;
		ListCell *option;

		/* look for the "key" option on this column */
		options = GetForeignColumnOptions(foreignTableId, attrno);
		foreach (option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			if (IS_KEY_COLUMN(def))
			{
				condAttr = lappend_int(condAttr, attrno);
			}
		}
	}

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
	case CMD_INSERT:
		sqlite_deparse_insert(&sql, root, resultRelation, rel, targetAttrs, doNothing, &values_end_len);
		break;
	case CMD_UPDATE:
		sqlite_deparse_update(&sql, root, resultRelation, rel, targetAttrs, condAttr);
		break;
	case CMD_DELETE:
		sqlite_deparse_delete(&sql, root, resultRelation, rel, condAttr);
		break;
	default:
		elog(ERROR, "unexpected operation: %d", (int)operation);
		break;
	}
	table_close(rel, NoLock);
	return list_make3(makeString(sql.data), targetAttrs, makeInteger(values_end_len));
}

static void
sqliteBeginForeignModify(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo,
						 List *fdw_private,
						 int subplan_index,
						 int eflags)
{
	SqliteFdwExecState *fmstate = NULL;
	EState *estate = mtstate->ps.state;
	Relation rel = resultRelInfo->ri_RelationDesc;
	AttrNumber n_params = 0;
	Oid typefnoid = InvalidOid;
	bool isvarlena = false;
	ListCell *lc = NULL;
	Oid foreignTableId = InvalidOid;
	ForeignTable *table;
	ForeignServer *server;
	Plan *subplan;
	int i;

	elog(DEBUG1, " duckdb_fdw : %s", __func__);

	foreignTableId = RelationGetRelid(rel);
#if (PG_VERSION_NUM >= 140000)
	subplan = outerPlanState(mtstate)->plan;
#else
	subplan = mtstate->mt_plans[subplan_index]->plan;
#endif

	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case. resultRelInfon->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	fmstate = (SqliteFdwExecState *)palloc0(sizeof(SqliteFdwExecState));
	fmstate->rel = rel;

	fmstate->conn = sqlite_get_connection(server, false);
	fmstate->query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
	fmstate->target_attrs = (List *)list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
	fmstate->retrieved_attrs = (List *)list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
	fmstate->values_end = intVal(list_nth(fdw_private, FdwModifyPrivateLen));
	fmstate->orig_query = pstrdup(fmstate->query);

	n_params = list_length(fmstate->retrieved_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "duckdb_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);

	/* Set up for remaining transmittable parameters */
	foreach (lc, fmstate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc);
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);

		Assert(!attr->attisdropped);
#if PG_VERSION_NUM >= 140000
		/* Ignore generated columns; */
		if (attr->attgenerated)
		{
			if (list_length(fmstate->retrieved_attrs) >= 1)
				fmstate->p_nums = 1;
			continue;
		}
#endif
		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);

#if (PG_VERSION_NUM >= 140000)
	/* Set batch_size from foreign server/table options. */
	fmstate->batch_size = sqlite_get_batch_size_option(rel);
#endif

	n_params = list_length(fmstate->retrieved_attrs);
	/* Initialize sqlite statment */
	fmstate->stmt = NULL;

	fmstate->num_slots = 1;
	/* Prepare sqlite statment */
	sqlite_prepare_wrapper(server, fmstate->conn, fmstate->query, &fmstate->stmt, NULL, true);

	resultRelInfo->ri_FdwState = fmstate;

	fmstate->junk_idx = palloc0(RelationGetDescr(rel)->natts * sizeof(AttrNumber));
	/* loop through table columns */
	for (i = 0; i < RelationGetDescr(rel)->natts; ++i)
	{
		/*
		 * for primary key columns, get the resjunk attribute number and store
		 * it
		 */
		fmstate->junk_idx[i] =
			ExecFindJunkAttributeInTlist(subplan->targetlist,
										 get_attname(foreignTableId, i + 1
#if (PG_VERSION_NUM >= 110000)
													 ,
													 false
#endif
													 ));
	}
}
#if (PG_VERSION_NUM >= 110000)
static void
sqliteBeginForeignInsert(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
{
	elog(ERROR, "Not support partition insert");
}
static void
sqliteEndForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo)
{
	elog(ERROR, "Not support partition insert");
}
#endif
/*
 * sqliteExecForeignInsert
 *		Insert one row into a foreign table
 */
static TupleTableSlot *
sqliteExecForeignInsert(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot)
{
	TupleTableSlot **rslot;
	int numSlots = 1;

	rslot = sqlite_execute_insert(estate, resultRelInfo, CMD_INSERT,
								  &slot, &planSlot, &numSlots);

	return rslot ? *rslot : NULL;
}

#if PG_VERSION_NUM >= 140000
/*
 * sqliteExecForeignBatchInsert
 *		Insert multiple rows into a foreign table
 */
static TupleTableSlot **
sqliteExecForeignBatchInsert(EState *estate,
							 ResultRelInfo *resultRelInfo,
							 TupleTableSlot **slots,
							 TupleTableSlot **planSlots,
							 int *numSlots)
{
	TupleTableSlot **rslot;

	rslot = sqlite_execute_insert(estate, resultRelInfo, CMD_INSERT,
								  slots, planSlots, numSlots);

	return rslot;
}

/*
 * sqliteGetForeignModifyBatchSize
 *		Determine the maximum number of tuples that can be inserted in bulk
 *
 * Returns the batch size specified for server or table. When batching is not
 * allowed (e.g. for tables with AFTER ROW triggers or with RETURNING clause),
 * returns 1.
 */
static int
sqliteGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
	int batch_size = 1;
	// int			limitVal;
	// #if SQLITE_VERSION_NUMBER < 3008008
	// 	int			limitRow;
	// #endif
	SqliteFdwExecState *fmstate = resultRelInfo->ri_FdwState ? (SqliteFdwExecState *)resultRelInfo->ri_FdwState : NULL;

	/* should be called only once */
	Assert(resultRelInfo->ri_BatchSize == 0);

	/*
	 * In EXPLAIN without ANALYZE, ri_fdwstate is NULL, so we have to lookup
	 * the option directly in server/table options. Otherwise just use the
	 * value we determined earlier.
	 */
	if (fmstate)
	{
		batch_size = fmstate->batch_size;
		// 		limitVal = (sqlite3_limit(fmstate->conn, SQLITE_LIMIT_VARIABLE_NUMBER, -1) / fmstate->p_nums);
		// #if SQLITE_VERSION_NUMBER < 3008008
		// 		limitRow = (sqlite3_limit(fmstate->conn, SQLITE_LIMIT_COMPOUND_SELECT, -1));
		// #endif
	}
	else
		batch_size = sqlite_get_batch_size_option(resultRelInfo->ri_RelationDesc);

	/*
	 * The batch size is used specified for server/table. Make sure we don't
	 * exceed this limit by using the maximum batch_size possible.
	 */
	// if (fmstate && fmstate->p_nums > 0)

	/*
	 * If version of SQLite is less than 3.8.8, Bulk insert into SQLite
	 * database has limit of 500 rows. So need use
	 * SQLITE_MAX_COMPOUND_SELECT to check maximum batch_size.
	 */
	// #if SQLITE_VERSION_NUMBER < 3008008
	// 		batch_size = Min(batch_size, Min(limitVal, limitRow));
	// #else
	// 		batch_size = Min(batch_size, limitVal);
	// #endif

	/* Otherwise use the batch size specified for server/table. */
	return batch_size;
}

/*
 * sqlite_find_modifytable_subplan
 *		Helper routine for sqlitePlanDirectModify to find the
 *		ModifyTable subplan node that scans the specified RTI.
 *
 * Returns NULL if the subplan couldn't be identified.  That's not a fatal
 * error condition, we just abandon trying to do the update directly.
 */
static ForeignScan *
sqlite_find_modifytable_subplan(PlannerInfo *root,
								ModifyTable *plan,
								Index rtindex,
								int subplan_index)
{
	Plan *subplan = outerPlan(plan);

	/*
	 * The cases we support are (1) the desired ForeignScan is the immediate
	 * child of ModifyTable, or (2) it is the subplan_index'th child of an
	 * Append node that is the immediate child of ModifyTable.  There is no
	 * point in looking further down, as that would mean that local joins are
	 * involved, so we can't do the update directly.
	 *
	 * There could be a Result atop the Append too, acting to compute the
	 * UPDATE targetlist values.  We ignore that here; the tlist will be
	 * checked by our caller.
	 *
	 * In principle we could examine all the children of the Append, but it's
	 * currently unlikely that the core planner would generate such a plan
	 * with the children out-of-order.  Moreover, such a search risks costing
	 * O(N^2) time when there are a lot of children.
	 */
	if (IsA(subplan, Append))
	{
		Append *appendplan = (Append *)subplan;

		if (subplan_index < list_length(appendplan->appendplans))
			subplan = (Plan *)list_nth(appendplan->appendplans, subplan_index);
	}
	else if (IsA(subplan, Result) &&
			 outerPlan(subplan) != NULL &&
			 IsA(outerPlan(subplan), Append))
	{
		Append *appendplan = (Append *)outerPlan(subplan);

		if (subplan_index < list_length(appendplan->appendplans))
			subplan = (Plan *)list_nth(appendplan->appendplans, subplan_index);
	}

	/* Now, have we got a ForeignScan on the desired rel? */
	if (IsA(subplan, ForeignScan))
	{
		ForeignScan *fscan = (ForeignScan *)subplan;

		if (bms_is_member(rtindex, fscan->fs_relids))
			return fscan;
	}

	return NULL;
}
#endif

/*
 * sqlitePlanDirectModify
 *		Consider a direct foreign table modification
 *
 * Decide whether it is safe to modify a foreign table directly, and if so,
 * rewrite subplan accordingly.
 */
static bool
sqlitePlanDirectModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{
	CmdType operation = plan->operation;
#if PG_VERSION_NUM < 140000
	Plan *subplan;
#endif
	RelOptInfo *foreignrel;
	RangeTblEntry *rte;
	SqliteFdwRelationInfo *fpinfo;
	Relation rel;
	StringInfoData sql;
	ForeignScan *fscan;
#if PG_VERSION_NUM >= 140000
	List *processed_tlist = NIL;
#endif
	List *targetAttrs = NIL;
	List *remote_exprs;
	List *params_list = NIL;
	List *retrieved_attrs = NIL;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * Decide whether it is safe to modify a foreign table directly.
	 */

	/*
	 * The table modification must be an UPDATE or DELETE.
	 */
	if (operation != CMD_UPDATE && operation != CMD_DELETE)
		return false;

		/*
		 * It's unsafe to modify a foreign table directly if there are any local
		 * joins needed.
		 */
#if (PG_VERSION_NUM >= 140000)
	fscan = sqlite_find_modifytable_subplan(root, plan, resultRelation, subplan_index);
	if (!fscan)
		return false;
#else
	subplan = (Plan *)list_nth(plan->plans, subplan_index);
	if (!IsA(subplan, ForeignScan))
		return false;
	fscan = (ForeignScan *)subplan;
#endif

		/*
		 * It's unsafe to modify a foreign table directly if there are any quals
		 * that should be evaluated locally.
		 */
#if (PG_VERSION_NUM >= 140000)
	if (fscan->scan.plan.qual != NIL)
#else
	if (subplan->qual != NIL)
#endif
		return false;

	/* not supported  RETURNING clause by this FDW */
	if (plan->returningLists)
	{
		return false;
	}

	/* Safe to fetch data about the target foreign rel */
	if (fscan->scan.scanrelid == 0)
	{
		foreignrel = find_join_rel(root, fscan->fs_relids);
		/* We should have a rel for this foreign join. */
		Assert(foreignrel);
	}
	else
		foreignrel = root->simple_rel_array[resultRelation];

	/*
	 * Currently, SQLite 3.33 does support UPDATE-FROM. Because with
	 * UPDATE-FROM we can join the target table against other tables. TODO:
	 * After JOIN pushdown feature is supported, we need to consider the below
	 * code to pushdown UPDATE-FROM.
	 */
	if (IS_JOIN_REL(foreignrel))
		return false;

	rte = root->simple_rte_array[resultRelation];
	fpinfo = (SqliteFdwRelationInfo *)foreignrel->fdw_private;

	/*
	 * It's unsafe to update a foreign table directly, if any expressions to
	 * assign to the target columns are unsafe to evaluate remotely.
	 */
	if (operation == CMD_UPDATE)
	{
#if (PG_VERSION_NUM >= 140000)
		ListCell *lc,
			*lc2;

		/*
		 * The expressions of concern are the first N columns of the processed
		 * targetlist, where N is the length of the rel's update_colnos.
		 */
		get_translated_update_targetlist(root, resultRelation,
										 &processed_tlist, &targetAttrs);
		forboth(lc, processed_tlist, lc2, targetAttrs)
		{
			TargetEntry *tle = lfirst_node(TargetEntry, lc);
			AttrNumber attno = lfirst_int(lc2);

			/* update's new-value expressions shouldn't be resjunk */
			Assert(!tle->resjunk);

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			if (!sqlite_is_foreign_expr(root, foreignrel, (Expr *)tle->expr))
				return false;
		}
#else
		int col;

		/*
		 * We transmit only columns that were explicitly targets of the
		 * UPDATE, so as to avoid unnecessary data transmission.
		 */
		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;
			TargetEntry *tle;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			tle = get_tle_by_resno(subplan->targetlist, attno);

			if (!tle)
				elog(ERROR, "attribute number %d not found in subplan targetlist",
					 attno);

			if (!sqlite_is_foreign_expr(root, foreignrel, (Expr *)tle->expr))
				return false;

			targetAttrs = lappend_int(targetAttrs, attno);
		}
#endif
	}

	/*
	 * Ok, rewrite subplan so as to modify the foreign table directly.
	 */
	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	/*
	 * Recall the qual clauses that must be evaluated remotely.  (These are
	 * bare clauses not RestrictInfos, but deparse.c's appendConditions()
	 * doesn't care.)
	 */
	remote_exprs = fpinfo->final_remote_exprs;

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
	case CMD_UPDATE:
		sqlite_deparse_direct_update_sql(&sql, root, resultRelation, rel,
										 foreignrel,
#if (PG_VERSION_NUM >= 140000)
										 processed_tlist,
#else
										 ((Plan *)fscan)->targetlist,
#endif
										 targetAttrs,
										 remote_exprs, &params_list,
										 &retrieved_attrs);
		break;
	case CMD_DELETE:
		sqlite_deparse_direct_delete_sql(&sql, root, resultRelation, rel,
										 foreignrel,
										 remote_exprs, &params_list,
										 &retrieved_attrs);
		break;
	default:
		elog(ERROR, "unexpected operation: %d", (int)operation);
		break;
	}

	/*
	 * Update the operation info.
	 */
	fscan->operation = operation;
#if PG_VERSION_NUM >= 140000
	fscan->resultRelation = resultRelation;
#endif

	/*
	 * Update the fdw_exprs list that will be available to the executor.
	 */
	fscan->fdw_exprs = params_list;

	/*
	 * Update the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwDirectModifyPrivateIndex, above.
	 */
	fscan->fdw_private = list_make4(makeString(sql.data),
									makeInteger(0),
									retrieved_attrs,
									makeInteger(plan->canSetTag));

	/*
	 * Update the foreign-join-related fields.
	 */
	if (fscan->scan.scanrelid == 0)
	{
		/* No need for the outer subplan. */
		fscan->scan.plan.lefttree = NULL;
	}

	table_close(rel, NoLock);
	return true;
}

/*
 * sqliteBeginDirectModify
 *		Prepare a direct foreign table modification
 */
static void
sqliteBeginDirectModify(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	EState *estate = node->ss.ps.state;
	SqliteFdwDirectModifyState *dmstate;
	Index rtindex;
	ForeignTable *table;
	ForeignServer *server;
	int numParams;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	dmstate = (SqliteFdwDirectModifyState *)palloc0(sizeof(SqliteFdwDirectModifyState));
	node->fdw_state = (void *)dmstate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
#if (PG_VERSION_NUM >= 140000)
	rtindex = node->resultRelInfo->ri_RangeTableIndex;
#else
	rtindex = estate->es_result_relation_info->ri_RangeTableIndex;
#endif

	/* Get info about foreign table. */
	if (fsplan->scan.scanrelid == 0)
		dmstate->rel = ExecOpenScanRelation(estate, rtindex, eflags);
	else
		dmstate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(RelationGetRelid(dmstate->rel));
	server = GetForeignServer(table->serverid);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	dmstate->conn = sqlite_get_connection(server, false);

	/* Update the foreign-join-related fields. */
	if (fsplan->scan.scanrelid == 0)
	{
		/* Save info about foreign table. */
		dmstate->resultRel = dmstate->rel;

		/*
		 * Set dmstate->rel to NULL to teach get_returning_data() and
		 * make_tuple_from_result_row() that columns fetched from the remote
		 * server are described by fdw_scan_tlist of the foreign-scan plan
		 * node, not the tuple descriptor for the target relation.
		 */
		dmstate->rel = NULL;
	}

	/* Initialize state variable */
	dmstate->num_tuples = -1; /* -1 means not set yet */

	/* Get private info created by planner functions. */
	dmstate->query = strVal(list_nth(fsplan->fdw_private,
									 FdwDirectModifyPrivateUpdateSql));
	dmstate->has_returning = intVal(list_nth(fsplan->fdw_private,
											 FdwDirectModifyPrivateHasReturning));
	dmstate->retrieved_attrs = (List *)list_nth(fsplan->fdw_private,
												FdwDirectModifyPrivateRetrievedAttrs);
	dmstate->set_processed = intVal(list_nth(fsplan->fdw_private,
											 FdwDirectModifyPrivateSetProcessed));

	/* Create context for per-tuple temp workspace. */
	dmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "duckdb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Initialize the Sqlite statement */
	dmstate->stmt = NULL;

	/* Prepare Sqlite statement */
	sqlite_prepare_wrapper(server, dmstate->conn, dmstate->query, &dmstate->stmt, NULL, true);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	numParams = list_length(fsplan->fdw_exprs);
	dmstate->numParams = numParams;
	if (numParams > 0)
		sqlite_prepare_query_params((PlanState *)node,
									fsplan->fdw_exprs,
									numParams,
									&dmstate->param_flinfo,
									&dmstate->param_exprs,
									&dmstate->param_values,
									&dmstate->param_types);
}

/*
 * sqliteIterateDirectModify
 *		Execute a direct foreign table modification
 */
static TupleTableSlot *
sqliteIterateDirectModify(ForeignScanState *node)
{
	SqliteFdwDirectModifyState *dmstate = (SqliteFdwDirectModifyState *)node->fdw_state;
	EState *estate = node->ss.ps.state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	Instrumentation *instr = node->ss.ps.instrument;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	Assert(!dmstate->has_returning);

	/*
	 * If this is the first call after Begin, execute the statement.
	 */
	if (dmstate->num_tuples == -1)
		sqlite_execute_dml_stmt(node);

	/* Increment the command es_processed count if necessary. */
	if (dmstate->set_processed)
		estate->es_processed += dmstate->num_tuples;

	/* Increment the tuple count for EXPLAIN ANALYZE if necessary. */
	if (instr)
		instr->tuplecount += dmstate->num_tuples;

	return ExecClearTuple(slot);
}

/*
 * sqliteEndDirectModify
 *		Finish a direct foreign table modification
 */
static void
sqliteEndDirectModify(ForeignScanState *node)
{
	SqliteFdwDirectModifyState *dmstate = (SqliteFdwDirectModifyState *)node->fdw_state;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/* if dmstate is NULL, we are in EXPLAIN; nothing to do */
	if (dmstate == NULL)
		return;

	if (dmstate->stmt)
	{
		dmstate->stmt = NULL;
	}
}

/*
 * sqliteExplainDirectModify
 *		Produce extra output for EXPLAIN of a ForeignScan that modifies a
 *		foreign table directly
 */
static void
sqliteExplainDirectModify(ForeignScanState *node,
						  struct ExplainState *es)
{
	List *fdw_private;
	char *sql;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	if (es->verbose)
	{
		fdw_private = ((ForeignScan *)node->ss.ps.plan)->fdw_private;
		sql = strVal(list_nth(fdw_private, FdwDirectModifyPrivateUpdateSql));
		ExplainPropertyText("SQLite query", sql, es);
	}
}

#if PG_VERSION_NUM >= 140000
/*
 * sqliteExecForeignTruncate
 *		Truncate one or more foreign tables
 */
static void
sqliteExecForeignTruncate(List *rels,
						  DropBehavior behavior,
						  bool restart_seqs)
{
	Oid serverid = InvalidOid;
	sqlite3 *conn = NULL;
	StringInfoData sql;
	ListCell *lc;
	bool server_truncatable = true;

	/*
	 * By default, all duckdb_fdw foreign tables are assumed truncatable. This
	 * can be overridden by a per-server setting, which in turn can be
	 * overridden by a per-table setting.
	 */
	foreach (lc, rels)
	{
		ForeignServer *server = NULL;
		Relation rel = lfirst(lc);
		ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
		ListCell *cell;
		bool truncatable;

		/*
		 * First time through, determine whether the foreign server allows
		 * truncates. Since all specified foreign tables are assumed to belong
		 * to the same foreign server, this result can be used for other
		 * foreign tables.
		 */
		if (!OidIsValid(serverid))
		{
			serverid = table->serverid;
			server = GetForeignServer(serverid);

			foreach (cell, server->options)
			{
				DefElem *defel = (DefElem *)lfirst(cell);

				if (strcmp(defel->defname, "truncatable") == 0)
				{
					server_truncatable = defGetBoolean(defel);
					break;
				}
			}
		}

		/*
		 * Confirm that all specified foreign tables belong to the same
		 * foreign server.
		 */
		Assert(table->serverid == serverid);

		/* Determine whether this foreign table allows truncations */
		truncatable = server_truncatable;
		foreach (cell, table->options)
		{
			DefElem *defel = (DefElem *)lfirst(cell);

			if (strcmp(defel->defname, "truncatable") == 0)
			{
				truncatable = defGetBoolean(defel);
				break;
			}
		}

		if (!truncatable)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("foreign table \"%s\" does not allow truncates",
							RelationGetRelationName(rel))));
	}
	Assert(OidIsValid(serverid));

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	conn = sqlite_get_connection(GetForeignServer(serverid), true);

	/*
	 * TRUNCATE does not support in SQLite, so convert into DELETE FROM to
	 * delete each table.
	 */
	initStringInfo(&sql);
	sqlite_deparse_truncate(&sql, rels);

	/* Issue the DELETE statement without WHERE clause to remote server */
	sqlite_do_sql_command(conn, sql.data, ERROR);

	pfree(sql.data);
}
#endif

static void
bindJunkColumnValue(SqliteFdwExecState *fmstate,
					TupleTableSlot *slot,
					TupleTableSlot *planSlot,
					Oid foreignTableId,
					int bindnum)
{
	int i;
	Datum value;
	Oid typeoid;

	/* Bind where condition using junk column */
	for (i = 0; i < slot->tts_tupleDescriptor->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		AttrNumber attrno = att->attnum;
		List *options;
		ListCell *option;

		/* look for the "key" option on this column */
		if (fmstate->junk_idx[i] == InvalidAttrNumber)
			continue;
		options = GetForeignColumnOptions(foreignTableId, attrno);
		foreach (option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);
			bool is_null = false;

			if (IS_KEY_COLUMN(def))
			{
				/* Get the id that was passed up as a resjunk column */
				value = ExecGetJunkAttribute(planSlot, fmstate->junk_idx[i], &is_null);
				typeoid = att->atttypid;

				/* Bind qual */
				sqlite_bind_sql_var(typeoid, bindnum, value, fmstate->stmt, &is_null);
				bindnum++;
			}
		}
	}
}

/*
 * sqliteExecForeignUpdate
 *		Update one row in a foreign table
 */
static TupleTableSlot *
sqliteExecForeignUpdate(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot)
{

	SqliteFdwExecState *fmstate = (SqliteFdwExecState *)resultRelInfo->ri_FdwState;
	Relation rel = resultRelInfo->ri_RelationDesc;
	Oid foreignTableId = RelationGetRelid(rel);
	ListCell *lc = NULL;
	int bindnum = 0;
	// int			i = 0;
	int rc = 0;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/* Bind the values */
	foreach (lc, fmstate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc);
		Oid type;
		bool is_null;
		Datum value = 0;
#if PG_VERSION_NUM >= 140000
		TupleDesc tupdesc = RelationGetDescr(fmstate->rel);
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		/* Ignore generated columns and skip bind value */
		if (attr->attgenerated)
			continue;
#endif
		/* first attribute cannot be in target list attribute */
		type = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid;

		value = slot_getattr(slot, attnum, &is_null);

		sqlite_bind_sql_var(type, bindnum, value, fmstate->stmt, &is_null);
		bindnum++;
		// i++;
	}

	bindJunkColumnValue(fmstate, slot, planSlot, foreignTableId, bindnum);

	/* Execute the query */
	rc = sqlite3_step(fmstate->stmt);
	if (rc != SQLITE_DONE)
	{
		sqlitefdw_report_error(ERROR, fmstate->stmt, fmstate->conn, NULL, rc);
	}

	sqlite3_reset(fmstate->stmt);

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

static TupleTableSlot *
sqliteExecForeignDelete(EState *estate,
						ResultRelInfo *resultRelInfo,
						TupleTableSlot *slot,
						TupleTableSlot *planSlot)
{
	SqliteFdwExecState *fmstate = (SqliteFdwExecState *)resultRelInfo->ri_FdwState;
	Relation rel = resultRelInfo->ri_RelationDesc;
	Oid foreignTableId = RelationGetRelid(rel);
	int rc = 0;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	bindJunkColumnValue(fmstate, slot, planSlot, foreignTableId, 0);

	/* Execute the query */
	rc = sqlite3_step(fmstate->stmt);
	if (rc != SQLITE_DONE)
	{
		sqlitefdw_report_error(ERROR, fmstate->stmt, fmstate->conn, NULL, rc);
	}
	sqlite3_reset(fmstate->stmt);
	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

static void
sqliteEndForeignModify(EState *estate,
					   ResultRelInfo *resultRelInfo)
{

	SqliteFdwExecState *fmstate = (SqliteFdwExecState *)resultRelInfo->ri_FdwState;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	if (fmstate && fmstate->stmt)
	{
		fmstate->stmt = NULL;
	}
}

static void
sqliteExplainForeignScan(ForeignScanState *node,
						 struct ExplainState *es)
{
	ForeignScan *plan = castNode(ForeignScan, node->ss.ps.plan);
	List *fdw_private = plan->fdw_private;
	char *sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	if (es->verbose)
	{
		ExplainPropertyText("SQLite query", sql, es);
	}
}

static void
sqliteExplainForeignModify(ModifyTableState *mtstate,
						   ResultRelInfo *rinfo,
						   List *fdw_private,
						   int subplan_index,
						   struct ExplainState *es)
{
	elog(DEBUG1, "duckdb_fdw : %s", __func__);

#if PG_VERSION_NUM >= 140000
	if (es->verbose)
	{
		/*
		 * For INSERT we should always have batch size >= 1, but UPDATE and
		 * DELETE don't support batching so don't show the property.
		 */
		if (rinfo->ri_BatchSize > 0)
			ExplainPropertyInteger("Batch Size", NULL, rinfo->ri_BatchSize, es);
	}
#endif
}

static bool
sqliteAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *func,
						  BlockNumber *totalpages)
{
	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	return false;
}

static const char * get_remote_catalog(ForeignServer *server, sqlite3 *volatile db)
{
	sqlite3_stmt *volatile sql_stmt = NULL;
	int rc = 0;
	const char *result_text = NULL;
	const char *catalog = NULL;
	char *query = "SELECT current_catalog() as remote_catalog";

	sqlite_prepare_wrapper(server, db, query, (sqlite3_stmt **)&sql_stmt, NULL, false);
	rc = sqlite3_step(sql_stmt);

	if (rc == SQLITE_DONE)
	{
		sqlite3_reset(sql_stmt);
	}
	else if (rc != SQLITE_ROW)
	{
		/*
		 * Not pass sql_stmt to sqlitefdw_report_error because it is
		 * finalized in PG_CATCH
		 */
		sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(sql_stmt), rc);
	}

	// Retrieve the result as a char*
	result_text = (const char *)sqlite3_column_text(sql_stmt, 0);
	if (result_text)
	{
		// Copy the result to a new memory location
		catalog = pstrdup(result_text);
	}
	elog(DEBUG1, "duckdb_fdw : %s, catalog: %s", __func__, catalog);
	sqlite3_finalize(sql_stmt);
	sql_stmt = NULL;

	return catalog;
}

/*
 * Import a foreign schema
 */
static List *
sqliteImportForeignSchema(ImportForeignSchemaStmt *stmt,
						  Oid serverOid)
{
	sqlite3 *volatile db = NULL;
	sqlite3_stmt *volatile sql_stmt = NULL;
	sqlite3_stmt *volatile pragma_stmt = NULL;
	ForeignServer *server;
	ListCell *lc;
	StringInfoData buf;
	List *commands = NIL;
	bool import_default = false;
	bool import_not_null = true;
	List *tables = NIL;

	char *remote_schema = "main";

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/* Parse statement options */
	foreach (lc, stmt->options)
	{
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "import_default") == 0)
			import_default = defGetBoolean(def);
		else if (strcmp(def->defname, "import_not_null") == 0)
			import_not_null = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
	}

	server = GetForeignServer(serverOid);
	db = sqlite_get_connection(server, true);

	PG_TRY();
	{


		const char *remote_catalog = get_remote_catalog(server, db);
		elog(DEBUG1, "duckdb_fdw remote_catalog: %s", remote_catalog);
		/* You want all tables, except system tables */
		initStringInfo(&buf);
		if (strcmp(stmt->remote_schema, "public") != 0)
		{
			remote_schema = stmt->remote_schema;
		}
		appendStringInfo(&buf, "SELECT table_name as name FROM information_schema.tables WHERE table_catalog=%s AND table_schema = %s", quote_literal_cstr(remote_catalog), quote_literal_cstr(remote_schema));
		/* Apply restrictions for LIMIT TO and EXCEPT */
		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
			stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			bool first_item = true;

			appendStringInfoString(&buf, " AND name ");
			if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
				appendStringInfoString(&buf, "NOT ");
			appendStringInfoString(&buf, "IN (");

			foreach (lc, stmt->table_list)
			{
				RangeVar *rv = (RangeVar *)lfirst(lc);

				if (first_item)
					first_item = false;
				else
					appendStringInfoString(&buf, ", ");

				appendStringInfoString(&buf, quote_literal_cstr(rv->relname));
			}
			appendStringInfoChar(&buf, ')');
		}

		sqlite_prepare_wrapper(server, db, buf.data, (sqlite3_stmt **)&sql_stmt, NULL, false);
		/* Scan all rows for this table */
		for (;;)
		{

			char *table;
			int rc = sqlite3_step(sql_stmt);

			if (rc == SQLITE_DONE)
			{
				sqlite3_reset(sql_stmt);
				break;
			}
			else if (rc != SQLITE_ROW)
			{
				/*
				 * Not pass sql_stmt to sqlitefdw_report_error because it is
				 * finalized in PG_CATCH
				 */

				sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(sql_stmt), rc);
			}
			table = (char *)sqlite3_column_text(sql_stmt, 0);

			tables = lappend(tables, pstrdup(table));
		}

		foreach (lc, tables)
		{
			char *table = (char *)lfirst(lc);
			bool first_item = true;
			char *query = palloc0(strlen(table) + 30);

			sprintf(query, "PRAGMA table_info(%s.%s)", quote_identifier(remote_schema), quote_identifier(table));

			resetStringInfo(&buf);
			appendStringInfo(&buf, "CREATE FOREIGN TABLE %s.%s (\n",
							 quote_identifier(stmt->local_schema), quote_identifier(table));
			sqlite_prepare_wrapper(server, db, query, (sqlite3_stmt **)&pragma_stmt, NULL, false);

			for (;;)
			{
				char *col_name;
				char *type_name;
				bool not_null;
				char *default_val;
				int primary_key;
				int rc = sqlite3_step(pragma_stmt);

				if (rc == SQLITE_DONE)
				{
					sqlite3_reset(pragma_stmt);
					break;
				}
				else if (rc != SQLITE_ROW)
				{
					/* Not pass sql_stmt because it is finalized in PG_CATCH */
					sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
				}
				col_name = (char *)sqlite3_column_text(pragma_stmt, 1);
				type_name = (char *)sqlite3_column_text(pragma_stmt, 2);
				not_null = (sqlite3_column_int(pragma_stmt, 3) == 1);
				default_val = (char *)sqlite3_column_text(pragma_stmt, 4);
				primary_key = sqlite3_column_int(pragma_stmt, 5);

				if (first_item)
					first_item = false;
				else
					appendStringInfoString(&buf, ",\n");

				appendStringInfo(&buf, "%s ", quote_identifier(col_name));

				sqlite_to_pg_type(&buf, type_name);

				/* part of the primary key */
				if (primary_key)
					appendStringInfo(&buf, " OPTIONS (key 'true')");

				if (not_null && import_not_null)
					appendStringInfo(&buf, " NOT NULL");

				if (default_val && import_default)
					appendStringInfo(&buf, " DEFAULT %s", default_val);
			}
			sqlite3_finalize(pragma_stmt);
			pragma_stmt = NULL;

			appendStringInfo(&buf, "\n) SERVER %s\nOPTIONS (table '%s.%s.%s');",
							 quote_identifier(stmt->server_name), quote_identifier(remote_catalog),
							 quote_identifier(remote_schema), quote_identifier(table));

			commands = lappend(commands, pstrdup(buf.data));

			elog(DEBUG1, "duckdb_fdw : %s %s", __func__, pstrdup(buf.data));
		}
	}
	PG_CATCH();
	{
		if (sql_stmt)
			sqlite3_finalize(sql_stmt);
		if (pragma_stmt)
			sqlite3_finalize(pragma_stmt);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (sql_stmt)
		sqlite3_finalize(sql_stmt);
	if (pragma_stmt)
		sqlite3_finalize(pragma_stmt);

	return commands;
}

/*
 * Estimate costs of executing a SQL statement remotely. The given "sql" must
 * be an EXPLAIN command.
 */
static void
sqlite_get_remote_estimate(const char *sql, sqlite3 *conn,
						   double *rows, int *width,
						   Cost *startup_cost, Cost *total_cost)
{
	/*
	 * Disable this funtionality because Sqlite FDW does not use estimate from
	 * remote for planning
	 */
	elog(ERROR, "Not supported to estimate from remote for planning");
}

/*
 * Assess whether the join between inner and outer relations can be pushed
 * down to the foreign server. As a side effect, save information we obtain
 * in this function to SqliteFdwRelationInfo passed in.
 */
static bool
sqlite_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
					   RelOptInfo *outerrel, RelOptInfo *innerrel,
					   JoinPathExtraData *extra)
{
	SqliteFdwRelationInfo *fpinfo;
	SqliteFdwRelationInfo *fpinfo_o;
	SqliteFdwRelationInfo *fpinfo_i;
	ListCell *lc;
	List *joinclauses;

	/*
	 * We support pushing down INNER and LEFT joins. Constructing queries
	 * representing SEMI and ANTI joins is hard, hence not considered right
	 * now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT)
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	fpinfo = (SqliteFdwRelationInfo *)joinrel->fdw_private;
	fpinfo_o = (SqliteFdwRelationInfo *)outerrel->fdw_private;
	fpinfo_i = (SqliteFdwRelationInfo *)innerrel->fdw_private;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * Merge FDW options.  We might be tempted to do this after we have deemed
	 * the foreign join to be OK.  But we must do this beforehand so that we
	 * know which quals can be evaluated on the foreign server, which might
	 * depend on shippable_extensions.
	 */
	fpinfo->server = fpinfo_o->server;
	sqlite_merge_fdw_options(fpinfo, fpinfo_o, fpinfo_i);

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach (lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool is_remote_clause = sqlite_is_foreign_expr(root, joinrel,
													   rinfo->clause);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * sqlite_deparse_explicit_target_list() isn't smart enough to handle
	 * anything other than a Var.  In particular, if there's some
	 * PlaceHolderVar that would need to be evaluated within this join tree
	 * (because there's an upper reference to a quantity that may go to NULL
	 * as a result of an outer join), then we can't try to push the join down
	 * because we'll fail when we get to
	 * sqlite_deparse_explicit_target_list(). However, a PlaceHolderVar that
	 * needs to be evaluated *at the top* of this join tree is OK, because we
	 * can do that locally after fetching the results from the remote side.
	 */
	foreach (lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids relids;

#if (PG_VERSION_NUM >= 100000)
		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ? joinrel->top_parent_relids : joinrel->relids;
#else
		relids = joinrel->relids;
#endif

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/*
	 * By default, both the input relations are not required to be deparsed as
	 * subqueries, but there might be some relations covered by the input
	 * relations that are required to be deparsed as subqueries, so save the
	 * relids of those relations for later use by the deparser.
	 */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	Assert(bms_is_subset(fpinfo_o->lower_subquery_rels, outerrel->relids));
	Assert(bms_is_subset(fpinfo_i->lower_subquery_rels, innerrel->relids));
	fpinfo->lower_subquery_rels = bms_union(fpinfo_o->lower_subquery_rels,
											fpinfo_i->lower_subquery_rels);

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from
	 * the outer side are added to remote_conds since those can be evaluated
	 * after the join is evaluated. The clauses from inner side are added to
	 * the joinclauses, since they need to be evaluated while constructing the
	 * join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
	case JOIN_INNER:
		fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										   fpinfo_i->remote_conds);
		fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										   fpinfo_o->remote_conds);
		break;

	case JOIN_LEFT:
		fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
										  fpinfo_i->remote_conds);
		fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										   fpinfo_o->remote_conds);
		break;

	default:
		/* Should not happen, we have just checked this above */
		elog(ERROR, "unsupported join type %d", jointype);
	}

	/*
	 * For an inner join, all restrictions can be treated alike. Treating the
	 * pushed down conditions as join conditions allows a top level full outer
	 * join to be deparsed without requiring subqueries.
	 */
	if (jointype == JOIN_INNER)
	{
		Assert(!fpinfo->joinclauses);
		fpinfo->joinclauses = fpinfo->remote_conds;
		fpinfo->remote_conds = NIL;
	}

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/* Get user mapping */
	if (fpinfo->use_remote_estimate)
	{
		if (fpinfo_o->use_remote_estimate)
			fpinfo->user = fpinfo_o->user;
		else
			fpinfo->user = fpinfo_i->user;
	}
	else
		fpinfo->user = NULL;

	/*
	 * Set # of retrieved rows and cached relation costs to some negative
	 * value, so that we can detect when they are set to some sensible values,
	 * during one (usually the first) of the calls to
	 * sqlite_estimate_path_cost_size.
	 */
	fpinfo->retrieved_rows = -1;
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation names mustn't include any digits, or it'll confuse
	 * sqliteExplainForeignScan.
	 */
	fpinfo->relation_name = psprintf("(%s) %s JOIN (%s)",
									 fpinfo_o->relation_name,
									 sqlite_get_jointype_name(fpinfo->jointype),
									 fpinfo_i->relation_name);

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	Assert(fpinfo->relation_index == 0); /* shouldn't be set yet */
	fpinfo->relation_index =
		list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

/*
 * Adjust the cost estimates of a foreign grouping path to include the cost
 * of generating properly-sorted output.
 */
static void
sqlite_adjust_foreign_grouping_path_cost(PlannerInfo *root,
										 List *pathkeys,
										 double retrieved_rows,
										 double width,
										 double limit_tuples,
										 Cost *p_startup_cost,
										 Cost *p_run_cost)
{
	/*
	 * If the GROUP BY clause isn't sort-able, the plan chosen by the remote
	 * side is unlikely to generate properly-sorted output, so it would need
	 * an explicit sort; adjust the given costs with cost_sort().  Likewise,
	 * if the GROUP BY clause is sort-able but isn't a superset of the given
	 * pathkeys, adjust the costs with that function.  Otherwise, adjust the
	 * costs by applying the same heuristic as for the scan or join case.
	 */
	if (!grouping_is_sortable(root->parse->groupClause) ||
		!pathkeys_contained_in(pathkeys, root->group_pathkeys))
	{
		Path sort_path; /* dummy for result of cost_sort */

		cost_sort(&sort_path,
				  root,
				  pathkeys,
				  *p_startup_cost + *p_run_cost,
				  retrieved_rows,
				  width,
				  0.0,
				  work_mem,
				  limit_tuples);

		*p_startup_cost = sort_path.startup_cost;
		*p_run_cost = sort_path.total_cost - sort_path.startup_cost;
	}
	else
	{
		/*
		 * The default extra cost seems too large for foreign-grouping cases;
		 * add 1/4th of that default.
		 */
		double sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER - 1.0) * 0.25;

		*p_startup_cost *= sort_multiplier;
		*p_run_cost *= sort_multiplier;
	}
}

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
sqlite_ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel,
								 EquivalenceClass *ec, EquivalenceMember *em,
								 void *arg)
{
	ec_member_foreign_arg *state = (ec_member_foreign_arg *)arg;
	Expr *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}

/*
 * sqliteGetForeignJoinPaths Add possible ForeignPath to joinrel, if
 * join is safe to push down.
 */
static void
sqliteGetForeignJoinPaths(PlannerInfo *root,
						  RelOptInfo *joinrel,
						  RelOptInfo *outerrel,
						  RelOptInfo *innerrel,
						  JoinType jointype,
						  JoinPathExtraData *extra)
{
	SqliteFdwRelationInfo *fpinfo;
	ForeignPath *joinpath;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	Path *epq_path; /* Path to create plan to be executed when
					 * EvalPlanQual gets triggered. */

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/*
	 * This code does not work for joins with lateral references, since those
	 * must have parameterized paths, which we don't generate yet.
	 */
	if (!bms_is_empty(joinrel->lateral_relids))
		return;

	/*
	 * Create unfinished SqliteFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe. Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (SqliteFdwRelationInfo *)palloc0(sizeof(SqliteFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;

	/*
	 * If there is a possibility that EvalPlanQual will be executed, we need
	 * to be able to reconstruct the row using scans of the base relations.
	 * GetExistingLocalJoinPath will find a suitable path for this purpose in
	 * the path list of the joinrel, if one exists.  We must be careful to
	 * call it before adding any ForeignPath, since the ForeignPath might
	 * dominate the only suitable local path available.  We also do it before
	 * calling sqlite_foreign_join_ok(), since that function updates fpinfo
	 * and marks it as pushable if the join is found to be pushable.
	 */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE ||
		root->rowMarks)
	{
		epq_path = GetExistingLocalJoinPath(joinrel);
		if (!epq_path)
		{
			elog(DEBUG1, "could not push down foreign join because a local path suitable for EPQ checks was not found");
			return;
		}
	}
	else
		epq_path = NULL;

	if (!sqlite_foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
	{
		/*
		 * Free path required for EPQ if we copied one; we don't need it now
		 */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	/*
	 * Compute the selectivity and cost of the local_conds, so we don't have
	 * to do it over again for each path. The best we can do for these
	 * conditions is to estimate selectivity on the basis of local statistics.
	 * The local conditions are applied after the join has been computed on
	 * the remote side like quals in WHERE clause, so pass jointype as
	 * JOIN_INNER.
	 */
	fpinfo->local_conds_sel = clauselist_selectivity(root,
													 fpinfo->local_conds,
													 0,
													 JOIN_INNER,
													 NULL);
	cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

	/*
	 * If we are going to estimate costs locally, estimate the join clause
	 * selectivity here while we have special join info.
	 */
	if (!fpinfo->use_remote_estimate)
		fpinfo->joinclause_sel = clauselist_selectivity(root, fpinfo->joinclauses,
														0, fpinfo->jointype,
														extra->sjinfo);

	/* Estimate costs for bare join relation */
	sqlite_estimate_path_cost_size(root, joinrel, NIL, NIL, NULL,
								   &rows, &width, &startup_cost, &total_cost);
	/* Now update this information in the joinrel */
	joinrel->rows = rows;
	joinrel->reltarget->width = width;
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
#if PG_VERSION_NUM >= 120000
	joinpath = create_foreign_join_path(root,
#else
	joinpath = create_foreignscan_path(root,
#endif
										joinrel,
										NULL, /* default pathtarget */
										rows,
										startup_cost,
										total_cost,
										NIL, /* no pathkeys */
										joinrel->lateral_relids,
										epq_path,
#if PG_VERSION_NUM >= 170000
										extra->restrictlist,
#endif
										NIL); /* no fdw_private */

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *)joinpath);

	/* Consider pathkeys for the join relation */
	sqlite_add_paths_with_pathkeys_for_rel(root, joinrel, NULL, epq_path);

	/* XXX Consider parameterized paths for the join relation */
}

static void
sqlite_merge_fdw_options(SqliteFdwRelationInfo *fpinfo,
						 const SqliteFdwRelationInfo *fpinfo_o,
						 const SqliteFdwRelationInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i ||
		   fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate;
	fpinfo->fetch_size = fpinfo_o->fetch_size;

	/* Merge the table level options from either side of the join. */
	if (fpinfo_i)
	{
		/*
		 * We'll prefer to use remote estimates for this join if any table
		 * from either side of the join is using remote estimates.  This is
		 * most likely going to be preferred since they're already willing to
		 * pay the price of a round trip to get the remote EXPLAIN.  In any
		 * case it's not entirely clear how we might otherwise handle this
		 * best.
		 */
		fpinfo->use_remote_estimate = fpinfo_o->use_remote_estimate ||
									  fpinfo_i->use_remote_estimate;

		/*
		 * Set fetch size to maximum of the joining sides, since we are
		 * expecting the rows returned by the join to be proportional to the
		 * relation sizes.
		 */
		fpinfo->fetch_size = Max(fpinfo_o->fetch_size, fpinfo_i->fetch_size);
	}
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to SqliteFdwRelationInfo of the input relation.
 */
static bool
sqlite_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel)
{
	Query *query = root->parse;
	PathTarget *grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *)grouped_rel->fdw_private;
	SqliteFdwRelationInfo *ofpinfo;
	List *aggvars;
	ListCell *lc;
	int i;
	List *tlist = NIL;

	/* Grouping Sets are not pushable */
	if (query->groupingSets)
		return false;

#if (PG_VERSION_NUM < 100000)
	if (root->query_level > 1)
	{
		if (root->all_baserels != NULL)
		{
			Query *query = root->parent_root->parse;
			int rtindex = bms_next_member(root->all_baserels, -1);

			if (rtindex != -2 && list_length(query->rtable) >= rtindex &&
				getrelid(rtindex, query->rtable) == 0)
				return false;
		}
	}
#endif

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (SqliteFdwRelationInfo *)fpinfo->outerrel->fdw_private;

	/*
	 * If underneath input relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	i = 0;
	foreach (lc, grouping_target->exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Index sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any of the GROUP BY expression is not shippable we can not
			 * push down aggregation to the foreign server.
			 */
			if (!sqlite_is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (sqlite_is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/* Check entire expression whether it is pushable or not */
			if (sqlite_is_foreign_expr(root, grouped_rel, expr) &&
				!sqlite_is_foreign_param(root, grouped_rel, expr))
			{
				/* Pushable, add to tlist */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not matched exactly, pull the var with aggregates then */
				aggvars = pull_var_clause((Node *)expr,
										  PVC_INCLUDE_AGGREGATES);

				if (!sqlite_is_foreign_expr(root, grouped_rel, (Expr *)aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain var
				 * nodes should be either same as some GROUP BY expression or
				 * part of some GROUP BY expression. In later case, the query
				 * cannot refer plain var nodes without the surrounding
				 * expression.  In both the cases, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * adding pulled plain var nodes in SELECT clause will cause
				 * an error on the foreign server if they are not same as some
				 * GROUP BY expression.
				 */
				foreach (l, aggvars)
				{
					Expr *aggref = (Expr *)lfirst(l);

					if (IsA(aggref, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(aggref));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable having clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (root->hasHavingQual && query->havingQual)
	{

		foreach (lc, (List *)query->havingQual)
		{
			Expr *expr = (Expr *)lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));

#if (PG_VERSION_NUM >= 100000)
			rinfo = make_restrictinfo(
#if PG_VERSION_NUM >= 140000
				root,
#endif
				expr,
				true,
				false,
				false,
#if (PG_VERSION_NUM >= 160000)
				false,
#endif
				root->qual_security_level,
				grouped_rel->relids,
				NULL,
				NULL);
#else
			rinfo = make_simple_restrictinfo(expr);
#endif
			if (sqlite_is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		aggvars = NIL;

		foreach (lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *)rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach (lc, aggvars)
		{
			Expr *expr = (Expr *)lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.
			 */
			if (IsA(expr, Aggref))
			{
				if (!sqlite_is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * If user is willing to estimate cost for a scan using EXPLAIN, he
	 * intends to estimate scans on that relation more accurately. Then, it
	 * makes sense to estimate the cost of the grouping on that relation more
	 * accurately using EXPLAIN.
	 */
	fpinfo->use_remote_estimate = ofpinfo->use_remote_estimate;

	/* Copy startup and tuple cost as is from underneath input rel's fpinfo */
	fpinfo->fdw_startup_cost = ofpinfo->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = ofpinfo->fdw_tuple_cost;

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs, during one (usually the
	 * first) of the calls to sqlite_estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = NULL;

	return true;
}

/*
 * sqliteGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
static void
sqliteGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						   RelOptInfo *input_rel, RelOptInfo *output_rel
#if (PG_VERSION_NUM >= 110000)
						   ,
						   void *extra
#endif
)
{
	SqliteFdwRelationInfo *fpinfo;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((SqliteFdwRelationInfo *)input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG && stage != UPPERREL_ORDERED && stage != UPPERREL_FINAL) || output_rel->fdw_private)
		return;

	fpinfo = (SqliteFdwRelationInfo *)palloc0(sizeof(SqliteFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
	case UPPERREL_GROUP_AGG:
		sqlite_add_foreign_grouping_paths(root, input_rel, output_rel
#if (PG_VERSION_NUM >= 110000)
										  ,
										  (GroupPathExtraData *)extra
#endif
		);
		break;
	case UPPERREL_ORDERED:
		sqlite_add_foreign_ordered_paths(root, input_rel, output_rel);
		break;
	case UPPERREL_FINAL:
		sqlite_add_foreign_final_paths(root, input_rel, output_rel
#if (PG_VERSION_NUM >= 120000)
									   ,
									   (FinalPathExtraData *)extra
#endif
		);
		break;
	default:
		elog(ERROR, "unexpected upper relation: %d", (int)stage);
		break;
	}
}

/*
 * sqlite_add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
sqlite_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								  RelOptInfo *grouped_rel
#if (PG_VERSION_NUM >= 110000)
								  ,
								  GroupPathExtraData *extra
#endif
)
{
	Query *parse = root->parse;
	SqliteFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	SqliteFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

#if (PG_VERSION_NUM >= 110000)
	Assert(extra->patype == PARTITIONWISE_AGGREGATE_NONE ||
		   extra->patype == PARTITIONWISE_AGGREGATE_FULL);
#endif

	/* SQLite does not allow HAVING without GROUP BY */
	if (root->hasHavingQual && !parse->groupClause)
		return;

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, shippable extensions
	 * etc. details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;

	fpinfo->shippable_extensions = ifpinfo->shippable_extensions;

	/* Assess if it is safe to push down aggregation and grouping. */
	if (!sqlite_foreign_grouping_ok(root, grouped_rel))
		return;

	/* Use small cost to push down aggregate always */
	rows = width = startup_cost = total_cost = 1;
	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add foreign path to the grouping relation. */
#if (PG_VERSION_NUM >= 120000)
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  rows,
										  startup_cost,
										  total_cost,
										  NIL, /* no pathkeys */
										  NULL,
	#if (PG_VERSION_NUM >= 170000)
										  NIL, 
	#endif 										  
										  NIL); /* no fdw_private */
#else
	grouppath = create_foreignscan_path(root,
										grouped_rel,
										root->upper_targets[UPPERREL_GROUP_AGG],
										rows,
										startup_cost,
										total_cost,
										NIL,  /* no pathkeys */
										NULL, /* no required_outer */
										NULL,
										NIL); /* no fdw_private */
#endif

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *)grouppath);
}

/*
 * sqlite_add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
sqlite_add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
								 RelOptInfo *ordered_rel)
{
	Query *parse = root->parse;
	SqliteFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	SqliteFdwRelationInfo *fpinfo = ordered_rel->fdw_private;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	List *fdw_private;
	ForeignPath *ordered_path;
	ListCell *lc;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

#if (PG_VERSION_NUM >= 100000)
	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;
#else

	/*
	 * We don't support cases where there are any SRFs in the targetlist (PG
	 * Version >10)
	 */
	if (expression_returns_set((Node *)parse->targetList))
		return;
#endif

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;

	fpinfo->shippable_extensions = ifpinfo->shippable_extensions;

	/*
	 * If the input_rel is a base or join relation, we would already have
	 * considered pushing down the final sort to the remote server when
	 * creating pre-sorted foreign paths for that relation, because the
	 * query_pathkeys is set to the root->sort_pathkeys in that case (see
	 * standard_qp_callback()).
	 */
	if (input_rel->reloptkind == RELOPT_BASEREL ||
		input_rel->reloptkind == RELOPT_JOINREL)
	{
		Assert(root->query_pathkeys == root->sort_pathkeys);

		/* Safe to push down if the query_pathkeys is safe to push down */
		fpinfo->pushdown_safe = ifpinfo->qp_is_pushdown_safe;

		return;
	}

	/* The input_rel should be a grouping relation */
	Assert(input_rel->reloptkind == RELOPT_UPPER_REL &&
		   ifpinfo->stage == UPPERREL_GROUP_AGG);

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying grouping relation to perform the final sort remotely,
	 * which is stored into the fdw_private list of the resulting path.
	 */

	/* Assess if it is safe to push down the final sort */
	foreach (lc, root->sort_pathkeys)
	{
		PathKey *pathkey = (PathKey *)lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		Expr *sort_expr;
		RelOptInfo *fallback_rel = (input_rel->reloptkind == RELOPT_UPPER_REL) ? ifpinfo->outerrel : input_rel;

		/*
		 * is_foreign_expr would detect volatile expressions as well, but
		 * checking ec_has_volatile here saves some cycles.
		 */
		if (pathkey_ec->ec_has_volatile)
			return;

		/* Get the sort expression for the pathkey_ec */
		sort_expr = sqlite_find_em_expr_for_input_target(root,
														 pathkey_ec,
														 input_rel->reltarget,
														 fallback_rel);

		/* If it's unsafe to remote, we cannot push down the final sort */
		if (!sqlite_is_foreign_expr(root, input_rel, sort_expr))
			return;
	}

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Use small cost to push down aggregate always */
	rows = width = startup_cost = total_cost = 1;
	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/*
	 * Build the fdw_private list that will be used by sqliteGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
	fdw_private = list_make2(makeInteger(true), makeInteger(false));

#if (PG_VERSION_NUM >= 120000)
	/* Create foreign ordering path */
	ordered_path = create_foreign_upper_path(root,
											 input_rel,
											 root->upper_targets[UPPERREL_ORDERED],
											 rows,
											 startup_cost,
											 total_cost,
											 root->sort_pathkeys,
											 NULL, /* no extra plan */
#if (PG_VERSION_NUM >= 170000)
											NIL, 
#endif 
											 fdw_private);
#else

	/*
	 * We use root->upper_targets[UPERREL_FINAL] because until PG12,
	 * upper_targets[UPPERREL_ORDERED] is not filled. Anyways, in PG12
	 * root->upper_targets[UPPERREL_FINAL] and
	 * root->upper_targets[UPPERREL_ORDERED] have the same value. More info:
	 * backend/optimizer/plan/planner.c (Line 2189)
	 */

	/* Create foreign ordering path */
	ordered_path = create_foreignscan_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   root->sort_pathkeys,
										   NULL,
										   NULL, /* no extra plan */
										   fdw_private);
#endif

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *)ordered_path);
}

/*
 * sqlite_add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
sqlite_add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
							   RelOptInfo *final_rel
#if (PG_VERSION_NUM >= 120000)
							   ,
							   FinalPathExtraData *extra
#endif
)
{
	Query *parse = root->parse;
	SqliteFdwRelationInfo *ifpinfo = (SqliteFdwRelationInfo *)input_rel->fdw_private;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *)final_rel->fdw_private;
	bool has_final_sort = false;
	List *pathkeys = NIL;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	List *fdw_private;
	ForeignPath *final_path;
#if (PG_VERSION_NUM < 120000)
	bool has_limit = limit_needed(parse);
#endif

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is FOR UPDATE/SHARE clause and if there is no need to
	 * add a LIMIT node. We DONT support FOR UPDATE pushdown because SQLITE
	 * has no implemented yet, that's why we dont do nothing.
	 */
	if (parse->rowMarks
#if (PG_VERSION_NUM >= 120000)
		|| !extra->limit_needed
#else
		|| !has_limit
#endif
	)
		return;

#if (PG_VERSION_NUM >= 100000)
	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;
#else

	/*
	 * We don't support cases where there are any SRFs in the targetlist (PG
	 * Version >10)
	 */
	if (expression_returns_set((Node *)parse->targetList))
		return;
#endif

	/* Save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, foreign server, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;

	fpinfo->shippable_extensions = ifpinfo->shippable_extensions;

#if (PG_VERSION_NUM >= 120000)
	Assert(extra->limit_needed);
#else
	Assert(has_limit);
#endif

	/*
	 * If the input_rel is an ordered relation, replace the input_rel with its
	 * input relation
	 */
	if (input_rel->reloptkind == RELOPT_UPPER_REL &&
		ifpinfo->stage == UPPERREL_ORDERED)
	{
		input_rel = ifpinfo->outerrel;
		ifpinfo = (SqliteFdwRelationInfo *)input_rel->fdw_private;
		has_final_sort = true;
		pathkeys = root->sort_pathkeys;
	}

	/* The input_rel should be a base, join, or grouping relation */
	Assert(input_rel->reloptkind == RELOPT_BASEREL ||
		   input_rel->reloptkind == RELOPT_JOINREL ||
		   (input_rel->reloptkind == RELOPT_UPPER_REL &&
			ifpinfo->stage == UPPERREL_GROUP_AGG));

	/*
	 * We try to create a path below by extending a simple foreign path for
	 * the underlying base, join, or grouping relation to perform the final
	 * sort (if has_final_sort) and the LIMIT restriction remotely, which is
	 * stored into the fdw_private list of the resulting path.  (We
	 * re-estimate the costs of sorting the underlying relation, if
	 * has_final_sort.)
	 */

	/*
	 * Assess if it is safe to push down the LIMIT and OFFSET to the remote
	 * server
	 */

	/*
	 * If the underlying relation has any local conditions, the LIMIT/OFFSET
	 * cannot be pushed down.
	 */
	if (ifpinfo->local_conds)
		return;

#if PG_VERSION_NUM >= 130000
	/* Don't pushdown FETCH ... WITH TIES option */
	if (parse->limitCount && parse->limitOption == LIMIT_OPTION_WITH_TIES)
		return;
#endif

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!sqlite_is_foreign_expr(root, input_rel, (Expr *)parse->limitOffset) ||
		!sqlite_is_foreign_expr(root, input_rel, (Expr *)parse->limitCount))
		return;

	/* Safe to push down */
	fpinfo->pushdown_safe = true;

	/* Use small cost to push down limit always */
	rows = width = startup_cost = total_cost = 1;
	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/*
	 * Build the fdw_private list that will be used by sqliteGetForeignPlan.
	 * Items in the list must match order in enum FdwPathPrivateIndex.
	 */
#if (PG_VERSION_NUM >= 120000)
	fdw_private = list_make2(makeInteger(has_final_sort),
							 makeInteger(extra->limit_needed));
#else
	fdw_private = list_make2(makeInteger(has_final_sort),
							 makeInteger(has_limit));
#endif

	/*
	 * Create foreign final path; this gets rid of a no-longer-needed outer
	 * plan (if any), which makes the EXPLAIN output look cleaner
	 */
#if (PG_VERSION_NUM >= 120000)
	final_path = create_foreign_upper_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL, /* no extra plan */
#if PG_VERSION_NUM >= 170000
										  NIL,/* no fdw_restrictinfo list */
#endif											   
										   fdw_private);
#else
	final_path = create_foreignscan_path(root,
										 input_rel,
										 root->upper_targets[UPPERREL_FINAL],
										 rows,
										 startup_cost,
										 total_cost,
										 pathkeys,
										 NULL, /* no required_outer */
										 NULL, /* no extra plan */
										 fdw_private);
#endif
	/* and add it to the final_rel */
	add_path(final_rel, (Path *)final_path);
}

/*
 * sqlite_estimate_path_cost_size Get cost and size estimates for a foreign scan on
 * given foreign relation either a base relation or a join between foreign
 * relations or an upper relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being
 * costed. fpextra specifies additional post-scan/join-processing steps such
 * as the final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
static void
sqlite_estimate_path_cost_size(PlannerInfo *root,
							   RelOptInfo *foreignrel,
							   List *param_join_conds,
							   List *pathkeys,
							   SqliteFdwPathExtraData *fpextra,
							   double *p_rows, int *p_width,
							   Cost *p_startup_cost, Cost *p_total_cost)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *)foreignrel->fdw_private;
	double rows;
	double retrieved_rows;
	int width;
	Cost startup_cost;
	Cost total_cost;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses. Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		List *remote_param_join_conds;
		List *local_param_join_conds;
		StringInfoData sql;
		sqlite3 *conn;
		Selectivity local_sel;
		QualCost local_cost;
		List *fdw_scan_tlist = NIL;
		List *remote_conds;

		/* Required only to be passed to deparseSelectStmtForRel */
		List *retrieved_attrs;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		sqlite_classify_conditions(root, foreignrel, param_join_conds,
								   &remote_param_join_conds, &local_param_join_conds);

		/*
		 * Build the list of columns to be fetched from the foreign server.
		 */
		if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
			fdw_scan_tlist = sqlite_build_tlist_to_deparse(foreignrel);
		else
			fdw_scan_tlist = NIL;

		/*
		 * The complete list of remote conditions includes everything from
		 * baserestrictinfo plus any extra join_conds relevant to this
		 * particular path.
		 */
		remote_conds = list_concat(list_copy(remote_param_join_conds),
								   fpinfo->remote_conds);

		/*
		 * Construct EXPLAIN query including the desired SELECT, FROM, and
		 * WHERE clauses. Params and other-relation Vars are replaced by dummy
		 * values, so don't request params_list.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "EXPLAIN ");
		sqlite_deparse_select_stmt_for_rel(&sql, root, foreignrel, fdw_scan_tlist,
										   remote_conds, pathkeys,
										   fpextra ? fpextra->has_final_sort : false,
										   fpextra ? fpextra->has_limit : false,
										   false, &retrieved_attrs, NULL);

		/* Get the remote estimate */
		conn = sqlite_get_connection(fpinfo->server, false);

		sqlite_get_remote_estimate(sql.data, conn, &rows, &width,
								   &startup_cost, &total_cost);

		retrieved_rows = rows;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel = clauselist_selectivity(root,
										   local_param_join_conds,
										   foreignrel->relid,
										   JOIN_INNER,
										   NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;

		/*
		 * Add in tlist eval cost for each output row.  In case of an
		 * aggregate, some of the tlist expressions such as grouping
		 * expressions will be evaluated remotely, so adjust the costs.
		 */
		startup_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.startup;
		total_cost += foreignrel->reltarget->cost.per_tuple * rows;
		if (IS_UPPER_REL(foreignrel))
		{
			QualCost tlist_cost;

			cost_qual_eval(&tlist_cost, fdw_scan_tlist, root);
			startup_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.startup;
			total_cost -= tlist_cost.per_tuple * rows;
		}
	}
	else
	{
		Cost run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * We will come here again and again with different set of pathkeys or
		 * additional post-scan/join-processing steps that caller wants to
		 * cost.  We don't need to calculate the cost/size estimates for the
		 * underlying scan, join, or grouping each time.  Instead, use those
		 * estimates if we have cached them already.
		 */
		if (fpinfo->rel_startup_cost >= 0 && fpinfo->rel_total_cost >= 0)
		{
			Assert(fpinfo->retrieved_rows >= 1);

			rows = fpinfo->rows;
			retrieved_rows = fpinfo->retrieved_rows;
			width = fpinfo->width;
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;

			/*
			 * If we estimate the costs of a foreign scan or a foreign join
			 * with additional post-scan/join-processing steps, the scan or
			 * join costs obtained from the cache wouldn't yet contain the
			 * eval costs for the final scan/join target, which would've been
			 * updated by apply_scanjoin_target_to_paths(); add the eval costs
			 * now.
			 */
			if (fpextra && !IS_UPPER_REL(foreignrel))
			{
				/* Shouldn't get here unless we have LIMIT */
				Assert(fpextra->has_limit);
				Assert(foreignrel->reloptkind == RELOPT_BASEREL ||
					   foreignrel->reloptkind == RELOPT_JOINREL);
				startup_cost += foreignrel->reltarget->cost.startup;
				run_cost += foreignrel->reltarget->cost.per_tuple * rows;
			}
		}
		else if (IS_JOIN_REL(foreignrel))
		{
			SqliteFdwRelationInfo *fpinfo_i;
			SqliteFdwRelationInfo *fpinfo_o;
			QualCost join_cost;
			QualCost remote_conds_cost;
			double nrows;

			/* Use rows/width estimates made by the core code. */
			rows = foreignrel->rows;
			width = foreignrel->reltarget->width;

			/* For join we expect inner and outer relations set */
			Assert(fpinfo->innerrel && fpinfo->outerrel);

			fpinfo_i = (SqliteFdwRelationInfo *)fpinfo->innerrel->fdw_private;
			fpinfo_o = (SqliteFdwRelationInfo *)fpinfo->outerrel->fdw_private;

			/* Estimate of number of rows in cross product */
			nrows = fpinfo_i->rows * fpinfo_o->rows;

			/*
			 * Back into an estimate of the number of retrieved rows.  Just in
			 * case this is nuts, clamp to at most nrows.
			 */
			retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
			retrieved_rows = Min(retrieved_rows, nrows);

			/*
			 * The cost of foreign join is estimated as cost of generating
			 * rows for the joining relations + cost for applying quals on the
			 * rows.
			 */

			/*
			 * Calculate the cost of clauses pushed down to the foreign server
			 */
			cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
			/* Calculate the cost of applying join clauses */
			cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

			/*
			 * Startup cost includes startup cost of joining relations and the
			 * startup cost for join and other clauses. We do not include the
			 * startup cost specific to join strategy (e.g. setting up hash
			 * tables) since we do not know what strategy the foreign server
			 * is going to use.
			 */
			startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
			startup_cost += join_cost.startup;
			startup_cost += remote_conds_cost.startup;
			startup_cost += fpinfo->local_conds_cost.startup;

			/*
			 * Run time cost includes:
			 *
			 * 1. Run time cost (total_cost - startup_cost) of relations being
			 * joined
			 *
			 * 2. Run time cost of applying join clauses on the cross product
			 * of the joining relations.
			 *
			 * 3. Run time cost of applying pushed down other clauses on the
			 * result of join
			 *
			 * 4. Run time cost of applying nonpushable other clauses locally
			 * on the result fetched from the foreign server.
			 */
			run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
			run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
			run_cost += nrows * join_cost.per_tuple;
			nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
			run_cost += nrows * remote_conds_cost.per_tuple;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}
		else if (IS_UPPER_REL(foreignrel))
		{
			RelOptInfo *outerrel = fpinfo->outerrel;
			SqliteFdwRelationInfo *ofpinfo;
			AggClauseCosts aggcosts;
			double input_rows;
			int numGroupCols;
			double numGroups = 1;

			/*
			 * The upper relation should have its outer relation set
			 */
			Assert(outerrel);

			/*
			 * and that outer relation should have its reltarget set
			 */
			Assert(outerrel->reltarget);

			/*
			 * This cost model is mixture of costing done for sorted and
			 * hashed aggregates in cost_agg().  We are not sure which
			 * strategy will be considered at remote side, thus for
			 * simplicity, we put all startup related costs in startup_cost
			 * and all finalization and run cost are added in total_cost.
			 */

			ofpinfo = (SqliteFdwRelationInfo *)outerrel->fdw_private;

			/* Get rows from input rel */
			input_rows = ofpinfo->rows;

			/*
			 * Collect statistics about aggregates for estimating costs.
			 */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs)
			{
#if PG_VERSION_NUM >= 140000
				get_agg_clause_costs(root, AGGSPLIT_SIMPLE, &aggcosts);
#else
				get_agg_clause_costs(root, (Node *)fpinfo->grouped_tlist,
									 AGGSPLIT_SIMPLE, &aggcosts);

				/*
				 * The cost of aggregates in the HAVING qual will be the same
				 * for each child as it is for the parent, so there's no need
				 * to use a translated version of havingQual.
				 */
				get_agg_clause_costs(root, (Node *)root->parse->havingQual,
									 AGGSPLIT_SIMPLE, &aggcosts);
#endif
			}

			/*
			 * Get number of grouping columns and possible number of groups
			 */
			numGroupCols = list_length(root->parse->groupClause);
			numGroups = estimate_num_groups(root,
											get_sortgrouplist_exprs(root->parse->groupClause,
																	fpinfo->grouped_tlist),
											input_rows, NULL
#if PG_VERSION_NUM >= 140000
											,
											NULL
#endif
			);

			/*
			 * Get the retrieved_rows and rows estimates.  If there are HAVING
			 * quals, account for their selectivity.
			 */
			if (root->parse->havingQual)
			{
				/*
				 * Factor in the selectivity of the remotely-checked quals
				 */
				retrieved_rows =
					clamp_row_est(numGroups *
								  clauselist_selectivity(root,
														 fpinfo->remote_conds,
														 0,
														 JOIN_INNER,
														 NULL));

				/*
				 * Factor in the selectivity of the locally-checked quals
				 */
				rows = clamp_row_est(retrieved_rows * fpinfo->local_conds_sel);
			}
			else
			{
				rows = retrieved_rows = numGroups;
			}

			/* Use width estimate made by the core code. */
			width = foreignrel->reltarget->width;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input relation, adjusted for
			 *	     tlist replacement by apply_scanjoin_target_to_paths()
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += outerrel->reltarget->cost.startup;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
#if PG_VERSION_NUM >= 120000
			startup_cost += aggcosts.finalCost.startup;
#else
			startup_cost += aggcosts.finalCost;
#endif
			startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation, adjusted for
			 *	     tlist replacement by apply_scanjoin_target_to_paths()
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += outerrel->reltarget->cost.per_tuple * input_rows;
#if PG_VERSION_NUM >= 120000
			run_cost += aggcosts.finalCost.per_tuple * numGroups;
#else
			run_cost += aggcosts.finalCost * numGroups;
#endif
			run_cost += cpu_tuple_cost * numGroups;

			/* Account for the eval cost of HAVING quals, if any */
			if (root->parse->havingQual)
			{
				QualCost remote_cost;

				/*
				 * Add in the eval cost of the remotely-checked quals
				 */
				cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
				startup_cost += remote_cost.startup;
				run_cost += remote_cost.per_tuple * numGroups;

				/*
				 * Add in the eval cost of the locally-checked quals
				 */
				startup_cost += fpinfo->local_conds_cost.startup;
				run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
			}

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}
		else
		{
			Cost cpu_per_tuple;

			/*
			 * Use rows/width estimates made by set_baserel_size_estimates.
			 */
			rows = foreignrel->rows;
			width = foreignrel->reltarget->width;

			/*
			 * Back into an estimate of the number of retrieved rows.  Just in
			 * case this is nuts, clamp to at most foreignrel->tuples.
			 */
			retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
			retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * foreignrel->pages;

			startup_cost += foreignrel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * foreignrel->tuples;

			/* Add in tlist eval cost for each output row */
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}

		/*
		 * Without remote estimates, we have no real way to estimate the cost
		 * of generating sorted output.  It could be free if the query plan
		 * the remote side would have chosen generates properly-sorted output
		 * anyway, but in most cases it will cost something.  Estimate a value
		 * high enough that we won't pick the sorted path when the ordering
		 * isn't locally useful, but low enough that we'll err on the side of
		 * pushing down the ORDER BY clause when it's useful to do so.
		 */
		if (pathkeys != NIL)
		{
			if (IS_UPPER_REL(foreignrel))
			{
				Assert(foreignrel->reloptkind == RELOPT_UPPER_REL &&
					   fpinfo->stage == UPPERREL_GROUP_AGG);
				sqlite_adjust_foreign_grouping_path_cost(root, pathkeys,
														 retrieved_rows, width,
														 fpextra->limit_tuples,
														 &startup_cost, &run_cost);
			}
			else
			{
				startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
				run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			}
		}

		total_cost = startup_cost + run_cost;

#if PG_VERSION_NUM >= 120000
		/* Adjust the cost estimates if we have LIMIT */
		if (fpextra && fpextra->has_limit)
		{
			adjust_limit_rows_costs(&rows, &startup_cost, &total_cost,
									fpextra->offset_est, fpextra->count_est);
			retrieved_rows = rows;
		}
#endif
	}

	/*
	 * If this includes the final sort step, the given target, which will be
	 * applied to the resulting path, might have different expressions from
	 * the foreignrel's reltarget (see make_sort_input_target()); adjust tlist
	 * eval costs.
	 */
	if (fpextra && fpextra->has_final_sort &&
		fpextra->target != foreignrel->reltarget)
	{
		QualCost oldcost = foreignrel->reltarget->cost;
		QualCost newcost = fpextra->target->cost;

		startup_cost += newcost.startup - oldcost.startup;
		total_cost += newcost.startup - oldcost.startup;
		total_cost += (newcost.per_tuple - oldcost.per_tuple) * rows;
	}

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL)
	{
		fpinfo->retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/*
	 * If we have LIMIT, we should prefer performing the restriction remotely
	 * rather than locally, as the former avoids extra row fetches from the
	 * remote that the latter might cause.  But since the core code doesn't
	 * account for such fetches when estimating the costs of the local
	 * restriction (see create_limit_path()), there would be no difference
	 * between the costs of the local restriction and the costs of the remote
	 * restriction estimated above if we don't use remote estimates (except
	 * for the case where the foreignrel is a grouping relation, the given
	 * pathkeys is not NIL, and the effects of a bounded sort for that rel is
	 * accounted for in costing the remote restriction).  Tweak the costs of
	 * the remote restriction to ensure we'll prefer it if LIMIT is a useful
	 * one.
	 */
	if (!fpinfo->use_remote_estimate &&
		fpextra && fpextra->has_limit &&
		fpextra->limit_tuples > 0 &&
		fpextra->limit_tuples < fpinfo->rows)
	{
		Assert(fpinfo->rows > 0);
		total_cost -= (total_cost - startup_cost) * 0.05 *
					  (fpinfo->rows - fpextra->limit_tuples) / fpinfo->rows;
	}

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}

static void
sqlite_to_pg_type(StringInfo str, char *type)
{
	int i;

	/*
	 * type conversion based on SQLite affiniy
	 * https://www.sqlite.org/datatype3.html
	 */
	static const char *affinity[][2] = {
		{"int", "bigint"},
		{"char", "text"},
		{"clob", "text"},
		{"text", "text"},
		{"blob", "bytea"},
		{"real", "double precision"},
		{"floa", "double precision"},
		{"doub", "double precision"},
		{NULL, NULL}};

	static const char *pg_type[][2] = {
		{"datetime", "timestamp"},
		{"time"},
		{"date"},
		{"bit"}, /* bit(n) and bit varying(n) */
		{"boolean"},
		{"varchar"},
		{"char"},
		{NULL}};

	if (type == NULL || type[0] == '\0')
	{
		/* If no type, use blob affinity */
		appendStringInfoString(str, "bytea");
		return;
	}

	type = str_tolower(type, strlen(type), C_COLLATION_OID);

	for (i = 0; pg_type[i][0] != NULL; i++)
	{
		if (strncmp(type, pg_type[i][0], strlen(pg_type[i][0])) == 0)
		{
			/* Pass type to PostgreSQL as it is */
			if (pg_type[i][1] == NULL)
				appendStringInfoString(str, type);
			else
				appendStringInfoString(str, pg_type[i][1]);
			pfree(type);
			return;
		}
	}

	for (i = 0; affinity[i][0] != NULL; i++)
	{
		if (strstr(type, affinity[i][0]) != 0)
		{
			appendStringInfoString(str, affinity[i][1]);
			pfree(type);
			return;
		}
	}
	/* decimal for numeric affinity */
	appendStringInfoString(str, "decimal");
	pfree(type);
}

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the remote server.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int sqlite_set_transmission_modes(void)
{
	int nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void)set_config_option("datestyle", "ISO",
								PGC_USERSET, PGC_S_SESSION,
								GUC_ACTION_SAVE, true, 0, false);

	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void)set_config_option("intervalstyle", "postgres",
								PGC_USERSET, PGC_S_SESSION,
								GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void)set_config_option("extra_float_digits", "3",
								PGC_USERSET, PGC_S_SESSION,
								GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void sqlite_reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * sqlite_execute_insert
 *		Perform execute sqliteExecForeignInsert, sqliteExecForeignBatchInsert
 */
static TupleTableSlot **
sqlite_execute_insert(EState *estate,
					  ResultRelInfo *resultRelInfo,
					  CmdType operation,
					  TupleTableSlot **slots,
					  TupleTableSlot **planSlots,
					  int *numSlots)
{
	SqliteFdwExecState *fmstate = (SqliteFdwExecState *)resultRelInfo->ri_FdwState;
	ListCell *lc;
	Datum value = 0;
	MemoryContext oldcontext;
#if PG_VERSION_NUM >= 140000
	Relation rel = resultRelInfo->ri_RelationDesc;
	TupleDesc tupdesc = RelationGetDescr(rel);
#endif
	int rc = SQLITE_OK;
	int nestlevel;
	int bindnum = 0;
	int i;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	nestlevel = sqlite_set_transmission_modes();

	Assert(operation == CMD_INSERT);

#if PG_VERSION_NUM >= 140000
	if (fmstate->num_slots != *numSlots)
	{
		StringInfoData sql;
		ForeignTable *table;
		ForeignServer *server;

		table = GetForeignTable(RelationGetRelid(fmstate->rel));
		server = GetForeignServer(table->serverid);
		fmstate->stmt = NULL;

		initStringInfo(&sql);
		sqlite_rebuild_insert(&sql, fmstate->rel, fmstate->orig_query,
							  fmstate->target_attrs, fmstate->values_end,
							  fmstate->p_nums, *numSlots - 1);
		fmstate->query = sql.data;
		fmstate->num_slots = *numSlots;

		sqlite_prepare_wrapper(server, fmstate->conn, fmstate->query, &fmstate->stmt, NULL, true);
	}
#endif

	for (i = 0; i < *numSlots; i++)
	{
		foreach (lc, fmstate->retrieved_attrs)
		{
			int attnum = lfirst_int(lc) - 1;
			Oid type = TupleDescAttr(slots[i]->tts_tupleDescriptor, attnum)->atttypid;
			bool isnull;
#if PG_VERSION_NUM >= 140000
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum);

			/* Ignore generated columns and skip bind value */
			if (attr->attgenerated)
				continue;
#endif

			value = slot_getattr(slots[i], attnum + 1, &isnull);
			sqlite_bind_sql_var(type, bindnum, value, fmstate->stmt, &isnull);
			bindnum++;
		}
	}
	sqlite_reset_transmission_modes(nestlevel);

	/* Execute the query */
	rc = sqlite3_step(fmstate->stmt);
	if (rc != SQLITE_DONE)
	{
		sqlitefdw_report_error(ERROR, fmstate->stmt, fmstate->conn, NULL, rc);
	}
	sqlite3_reset(fmstate->stmt);
	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(fmstate->temp_cxt);

	return slots;
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
sqlite_prepare_query_params(PlanState *node,
							List *fdw_exprs,
							int numParams,
							FmgrInfo **param_flinfo,
							List **param_exprs,
							const char ***param_values,
							Oid **param_types)
{
	int i;
	ListCell *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * numParams);
	*param_types = (Oid *)palloc0(sizeof(Oid) * numParams);
	i = 0;
	foreach (lc, fdw_exprs)
	{
		Node *param_expr = (Node *)lfirst(lc);
		Oid typefnoid;
		bool isvarlena;

		(*param_types)[i] = exprType(param_expr);
		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require duckdb_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
#if PG_VERSION_NUM >= 100000
	*param_exprs = (List *)ExecInitExprList(fdw_exprs, node);
#else
	*param_exprs = (List *)ExecInitExpr((Expr *)fdw_exprs, node);
#endif
	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **)palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values and bind parameters
 *
 */
static void
sqlite_process_query_params(ExprContext *econtext,
							FmgrInfo *param_flinfo,
							List *param_exprs,
							const char **param_values,
							sqlite3_stmt **stmt,
							Oid *param_types)
{
	int i;
	ListCell *lc;
	int nestlevel;

	nestlevel = sqlite_set_transmission_modes();
	i = 0;
	foreach (lc, param_exprs)
	{
		ExprState *expr_state = (ExprState *)lfirst(lc);
		Datum expr_value;
		bool isNull;

		/* Evaluate the parameter expression */
#if PG_VERSION_NUM >= 100000
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);
#else
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);
#endif
		/* Bind parameters */
		sqlite_bind_sql_var(param_types[i], i, expr_value, *stmt, &isNull);

		/*
		 * Get string sentation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (isNull)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);
		i++;
	}
	sqlite_reset_transmission_modes(nestlevel);
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
sqlite_create_cursor(ForeignScanState *node)
{
	SqliteFdwExecState *festate = (SqliteFdwExecState *)node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int numParams = festate->numParams;
	const char **values = festate->param_values;

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (numParams > 0)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		sqlite_process_query_params(econtext,
									festate->param_flinfo,
									festate->param_exprs,
									values,
									&festate->stmt,
									festate->param_types);

		MemoryContextSwitchTo(oldcontext);
	}

	/* Mark the cursor as created, and show no tuples have been retrieved */
	festate->cursor_exists = true;
}

/*
 * Execute a direct UPDATE/DELETE statement.
 */
static void
sqlite_execute_dml_stmt(ForeignScanState *node)
{
	SqliteFdwDirectModifyState *dmstate = (SqliteFdwDirectModifyState *)node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int numParams = dmstate->numParams;
	const char **values = dmstate->param_values;
	int rc;

	/*
	 * Construct array of query parameter values in text format.
	 */
	if (numParams > 0)
		sqlite_process_query_params(econtext,
									dmstate->param_flinfo,
									dmstate->param_exprs,
									values,
									&dmstate->stmt,
									dmstate->param_types);

	/*
	 * Notice that we pass NULL for paramTypes, thus forcing the remote server
	 * to infer types for all parameters.  Since we explicitly cast every
	 * parameter (see deparse.c), the "inference" is trivial and will produce
	 * the desired result.  This allows us to avoid assuming that the remote
	 * server has the same OIDs we do for the parameters' types.
	 */
	rc = sqlite3_step(dmstate->stmt);
	if (rc != SQLITE_DONE)
	{
		sqlitefdw_report_error(ERROR, dmstate->stmt, dmstate->conn, NULL, rc);
	}

	/* Get the number of rows affected. */
	dmstate->num_tuples = sqlite3_changes(dmstate->conn);
}

/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
Expr *
sqlite_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell *lc_em;

	foreach (lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em);

		if (bms_is_subset(em->em_relids, rel->relids) &&
			!bms_is_empty(em->em_relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}

/*
 * Find an equivalence class member expression to be computed as a sort column
 * in the given target.
 */
Expr *
sqlite_find_em_expr_for_input_target(PlannerInfo *root,
									 EquivalenceClass *ec,
									 PathTarget *target,
									 RelOptInfo *fallbackRel)
{
	ListCell *lc1;
	int i;
	Expr *fallback_expr;

	i = 0;
	foreach (lc1, target->exprs)
	{
		Expr *expr = (Expr *)lfirst(lc1);
		Index sgref = get_pathtarget_sortgroupref(target, i);
		ListCell *lc2;

		/* Ignore non-sort expressions */
		if (sgref == 0 ||
			get_sortgroupref_clause_noerr(sgref,
										  root->parse->sortClause) == NULL)
		{
			i++;
			continue;
		}

		/* We ignore binary-compatible relabeling on both ends */
		while (expr && IsA(expr, RelabelType))
			expr = ((RelabelType *)expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach (lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *)lfirst(lc2);
			Expr *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *)em_expr)->arg;

			if (equal(em_expr, expr))
				return em->em_expr;
		}

		i++;
	}

	/*
	 * We add this method as fallback in versions prior to PG11/12 because
	 * target->sortgrouprefs its not filled and this function always fails
	 * because cannot find sort expression.
	 */
	fallback_expr = sqlite_find_em_expr_for_rel(ec, fallbackRel);

	if (fallback_expr)
		return fallback_expr;

	elog(ERROR, "could not find pathkey item to sort");
	return NULL; /* keep compiler quiet */
}

#if PG_VERSION_NUM >= 140000
/*
 * Determine batch size for a given foreign table. The option specified for
 * a table has precedence.
 */
static int
sqlite_get_batch_size_option(Relation rel)
{
	Oid foreigntableid = RelationGetRelid(rel);
	ForeignTable *table;
	ForeignServer *server;
	List *options;
	ListCell *lc;

	/* we use 1 by default, which means "no batching" */
	int batch_size = 1;

	/*
	 * Load options for table and server. We append server options after table
	 * options, because table options take precedence.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);

	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);

	/* See if either table or server specifies batch_size. */
	foreach (lc, options)
	{
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "batch_size") == 0)
		{
			(void)parse_int(defGetString(def), &batch_size, 0, NULL);
			break;
		}
	}

	return batch_size;
}
#endif

/*
 * duckdb_execute
 * 		Execute a statement that returns no result values  on a foreign server.
 */
Datum duckdb_execute(PG_FUNCTION_ARGS)
{
	sqlite3 *volatile db = NULL;
	sqlite3_stmt *volatile sql_stmt = NULL;
	ForeignServer *server;
	Name srvname = PG_GETARG_NAME(0);
	char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));

	elog(DEBUG1, "duckdb_fdw : %s  %s", __func__, query);

	server = GetForeignServerByName(srvname->data, false);
	db = sqlite_get_connection(server, false);

	PG_TRY();
	{
		sqlite_prepare_wrapper(server, db, query, (sqlite3_stmt **)&sql_stmt, NULL, false);
		/* Scan all rows for this table */
		for (;;)
		{
			int rc = sqlite3_step(sql_stmt);

			if (rc == SQLITE_DONE)
				break;
			else if (rc != SQLITE_ROW)
			{
				/*
				 * Not pass sql_stmt to sqlitefdw_report_error because it is
				 * finalized in PG_CATCH
				 */
				sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(sql_stmt), rc);
			}
		}
	}
	PG_CATCH();
	{
		if (sql_stmt)
			sqlite3_finalize(sql_stmt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (sql_stmt)
		sqlite3_finalize(sql_stmt);
	PG_RETURN_VOID();
}