/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        duckdb_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "duckdb_fdw.h"

#include <sqlite3.h>
#include <stdio.h>

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/cost.h"
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

bool		sqlite_load_library(void);
static void duckdb_fdw_exit(int code, Datum arg);

PG_MODULE_MAGIC;


/* The number of default estimated rows for table which does not exist in sqlite1_stat1
 * See sqlite3ResultSetOfSelect in select.c of SQLite
 */
#define DEFAULT_ROW_ESTIMATE 1000000
#define DEFAULTE_NUM_ROWS    1000
#define IS_KEY_COLUMN(A)	((strcmp(A->defname, "key") == 0) && \
							 (strcmp(((Value *)(A->arg))->val.str, "true") == 0))

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

extern Datum duckdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum duckdb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(duckdb_fdw_handler);


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


static void sqliteAddForeignUpdateTargets(Query *parsetree,
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



static bool sqliteAnalyzeForeignTable(Relation relation,
									  AcquireSampleRowsFunc *func,
									  BlockNumber *totalpages);



static List *sqliteImportForeignSchema(ImportForeignSchemaStmt *stmt,
									   Oid serverOid);

static void
			sqliteGetForeignUpperPaths(PlannerInfo *root,
									   UpperRelationKind stage,
									   RelOptInfo *input_rel,
									   RelOptInfo *output_rel
#if (PG_VERSION_NUM >= 110000)
									   ,void *extra
#endif
);

static void sqlite_prepare_wrapper(sqlite3 * db, char *query, sqlite3_stmt * *result, const char **pzTail);
static int	get_estimate(Oid foreigntableid);
static void sqlite_to_pg_type(StringInfo str, char *typname);

static void prepare_query_params(PlanState *node,
								 List *fdw_exprs,
								 int numParams,
								 FmgrInfo **param_flinfo,
								 List **param_exprs,
								 const char ***param_values,
								 Oid **param_types);

static void process_query_params(ExprContext *econtext,
								 FmgrInfo *param_flinfo,
								 List *param_exprs,
								 const char **param_values,
								 sqlite3_stmt * *stmt,
								 Oid *param_types);

static void create_cursor(ForeignScanState *node);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel);
static void add_foreign_grouping_paths(PlannerInfo *root,
									   RelOptInfo *input_rel,
									   RelOptInfo *grouped_rel
#if (PG_VERSION_NUM >= 110000)
									   ,GroupPathExtraData *extra
#endif
);
static void add_foreign_ordered_paths(PlannerInfo *root,
						  RelOptInfo *input_rel,
						  RelOptInfo *ordered_rel);
static void add_foreign_final_paths(PlannerInfo *root,
						RelOptInfo *input_rel,
						RelOptInfo *final_rel
#if (PG_VERSION_NUM >= 120000)
						,FinalPathExtraData *extra
#endif
);

static bool all_baserels_are_foreign(PlannerInfo *root);

static void add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, List *fdw_private);
static List *get_useful_pathkeys_for_relation(PlannerInfo *root,
								 RelOptInfo *rel);

/*
 * Library load-time initialization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void
_PG_init(void)
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


Datum
duckdb_fdw_handler(PG_FUNCTION_ARGS)
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
	fdwroutine->ExecForeignUpdate = sqliteExecForeignUpdate;
	fdwroutine->ExecForeignDelete = sqliteExecForeignDelete;
	fdwroutine->EndForeignModify = sqliteEndForeignModify;
#if (PG_VERSION_NUM >= 110000)
	fdwroutine->BeginForeignInsert = sqliteBeginForeignInsert;
	fdwroutine->EndForeignInsert = sqliteEndForeignInsert;
#endif

	/* support for EXPLAIN */
	fdwroutine->ExplainForeignScan = sqliteExplainForeignScan;
	fdwroutine->ExplainForeignModify = sqliteExplainForeignModify;

	/* support for ANALYSE */
	fdwroutine->AnalyzeForeignTable = sqliteAnalyzeForeignTable;

	/* support for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = sqliteImportForeignSchema;

	/* Support functions for upper relation push-down */
	fdwroutine->GetForeignUpperPaths = sqliteGetForeignUpperPaths;

	PG_RETURN_POINTER(fdwroutine);
}

/* Wrapper for sqlite3_prepare */
static void
sqlite_prepare_wrapper(sqlite3 * db, char *query, sqlite3_stmt * *stmt,
					   const char **pzTail)
{
	int			rc;

	elog(DEBUG1, "duckdb_fdw : %s %s\n", __func__, query);
	rc = sqlite3_prepare_v2(db, query, -1, stmt, pzTail);
	if (rc != SQLITE_OK)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("SQL error during prepare: %s %s", sqlite3_errmsg(db), query)
				 ));
	}
}


/*
 * sqliteGetForeignRelSize: Create a FdwPlan for a scan on the foreign table
 */
static void
sqliteGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	double		rows = 0;
	SqliteFdwRelationInfo *fpinfo;
	ListCell   *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *namespace;
	const char *relname;
	const char *refname;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	fpinfo = (SqliteFdwRelationInfo *) palloc0(sizeof(SqliteFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be pushed down always. */
	fpinfo->pushdown_safe = true;
	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	/*
	 * Identify which baserestrictinfo clauses can be sent to the remote
	 * server and which can't.
	 */
	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (sqlite_is_foreign_expr(root, baserel, ri->clause))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
		else
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
	}

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.
	 */
#if PG_VERSION_NUM >= 90600
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
#endif

	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
	}

	rows = get_estimate(foreigntableid);

	baserel->rows = rows;
	baserel->tuples = rows;


	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output. We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	namespace = get_namespace_name(get_rel_namespace(foreigntableid));
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(namespace),
					 quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));
}

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List	   *useful_pathkeys_list = NIL;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) rel->fdw_private;
	ListCell   *lc;

	/*
	 * Pushing the query_pathkeys to the remote server is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	fpinfo->qp_is_pushdown_safe = false;
	if (root->query_pathkeys)
	{
		bool		query_pathkeys_ok = true;

		foreach(lc, root->query_pathkeys)
		{
			PathKey    *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr	   *em_expr;

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
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, List *fdw_private)
{
	List	   *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell   *lc;
	double 		rows;
	Cost        startup_cost;
	Cost        total_cost;

	/* Use small cost to avoid calculating real cost size in SQLite */
	rows = startup_cost = total_cost = 10;

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach(lc, useful_pathkeys_list)
	{
		List	   *useful_pathkeys = lfirst(lc);

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
											 NULL,	/* no outer rel either */
#endif
											 NULL,
											 fdw_private));
		else
			elog(ERROR, "Join clauses not supported for Order..");
	}
}

/*
 * Check if any of the tables queried aren't foreign tables.
 * We use this function to add limit pushdownm fallback to sqlite
 * because if theres any non-foreign table, GetForeignUpperPath its not called from planner.c
 */
static bool
all_baserels_are_foreign(PlannerInfo *root)
{
	bool		allTablesQueriedAreForeign = true;
	ListCell	*l;

	/* If there is no append_rel_list, we assume we're only consulting a foreign table, so default value it's true and we dont need to do more. */
	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst_node(AppendRelInfo, l);
		int				childRTindex;
		RangeTblEntry	*childRTE;
		RelOptInfo		*childrel;

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
	Cost		startup_cost = 10;
	Cost		total_cost = baserel->rows + startup_cost;
	List	   *fdw_private = NIL;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	/* Estimate costs */
	total_cost = baserel->rows;

	/*
	 * We add fdw_private with has_limit: true if these three conditions are true because we need to be able to pushdown
	 * limit in this case:
	 *   - Query has LIMIT
	 *   - Query don't have OFFSET because if we pusdown OFFSET and later, we re-applying offset with the "final result", and we would be "jumping/skipping" child results
	 *   and losing registries that we wanted to show.
	 *   - Some of the baserels are not a foreign table, so PostgreSQL is not calling GetForeignUpperPaths
	 */
	if (limit_needed(root->parse) && !root->parse->limitOffset && !all_baserels_are_foreign(root))
		fdw_private = list_make2(makeInteger(false), makeInteger(true));

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
#if PG_VERSION_NUM >= 90600
									 NULL,	/* default pathtarget */
#endif
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
#if (PG_VERSION_NUM >= 120000)
									 baserel->lateral_relids,
#else
									 NULL,	/* no outer rel either */
#endif
									 NULL,	/* no extra plan */
									 fdw_private));

	/* Add paths with pathkeys */
	add_paths_with_pathkeys_for_rel(root, baserel, fdw_private);
}

/*
 * sqliteGetForeignPlan: Get a foreign scan plan node
 */
static ForeignScan *
sqliteGetForeignPlan(
					 PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) baserel->fdw_private;
	Index		scan_relid = baserel->relid;
	List	   *fdw_private;
	List	   *local_exprs = NULL;
	List	   *remote_exprs = NULL;
	List	   *params_list = NULL;
	List	   *fdw_scan_tlist = NIL;
	List	   *remote_conds = NIL;

	StringInfoData sql;
	bool	   has_final_sort = false;
	bool	   has_limit = false;
	List	   *retrieved_attrs;
	ListCell   *lc;
	List	   *fdw_recheck_quals = NIL;
	int			for_update;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

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
	 * determined to be safe or unsafe by classifyConditions are shown in
	 * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
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
	if (baserel->reloptkind == RELOPT_BASEREL ||
		baserel->reloptkind == RELOPT_OTHER_MEMBER_REL)
	{
		foreach(lc, scan_clauses)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

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
		Assert(!scan_clauses);

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
		fdw_scan_tlist = sqlite_build_tlist_to_deparse(baserel);

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
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins. Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(baserel->reloptkind != RELOPT_UPPER_REL);
			outer_plan->targetlist = fdw_scan_tlist;

			foreach(lc, local_exprs)
			{
				Join	   *join_plan = (Join *) outer_plan;
				Node	   *qual = lfirst(lc);

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
	sqliteDeparseSelectStmtForRel(&sql, root, baserel, fdw_scan_tlist,
								  remote_exprs, best_path->path.pathkeys,
								  has_final_sort, has_limit, false,
								  &retrieved_attrs, &params_list);

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
	fdw_private = lappend(fdw_private, makeInteger(root->all_baserels == NULL ? -2 : bms_next_member(root->all_baserels, -1)));
	if (baserel->reloptkind == RELOPT_JOINREL ||
		baserel->reloptkind == RELOPT_UPPER_REL)
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private,
							fdw_scan_tlist, fdw_recheck_quals, outer_plan

		);
}

/*
 * sqliteBeginForeignScan: Initiate access to the database
 */
static void
sqliteBeginForeignScan(ForeignScanState *node, int eflags)
{
	sqlite3    *conn = NULL;
	SqliteFdwExecState *festate = NULL;
	EState	   *estate = node->ss.ps.state;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	int			numParams;
	ForeignTable *table;
	ForeignServer *server;
	RangeTblEntry *rte;
	int			rtindex;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * We'll save private state in node->fdw_state.
	 */
	festate = (SqliteFdwExecState *) palloc(sizeof(SqliteFdwExecState));
	node->fdw_state = (void *) festate;
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
		rtindex = intVal(list_nth(fsplan->fdw_private, 3));
		if (rtindex == -2)
		{
			/* root->all_baserels at GetForeignPlan is empty */
			rtindex = bms_next_member(fsplan->fs_relids, -1);
		}
	}
	rte = rt_fetch(rtindex, estate->es_range_table);

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	server = GetForeignServer(table->serverid);

	/*
	 * Get the already connected connection, otherwise connect and get the
	 * connection handle.
	 */
	conn = sqlite_get_connection(server);

	/* Stash away the state info we have already */
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));
	festate->retrieved_attrs = list_nth(fsplan->fdw_private, 1);
	festate->for_update = intVal(list_nth(fsplan->fdw_private, 2)) ? true : false;
	festate->conn = conn;
	festate->cursor_exists = false;

	festate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "duckdb_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);

	/* Initialize the Sqlite statement */
	festate->stmt = NULL;

	/* Prepare Sqlite statement */
	sqlite_prepare_wrapper(festate->conn, festate->query, &festate->stmt, NULL);

	/* Prepare for output conversion of parameters used in remote query. */
	numParams = list_length(fsplan->fdw_exprs);
	festate->numParams = numParams;
	if (numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams,
							 &festate->param_flinfo,
							 &festate->param_exprs,
							 &festate->param_values,
							 &festate->param_types);
}

static void
make_tuple_from_result_row(sqlite3_stmt * stmt,
						   TupleDesc tupleDescriptor,
						   List *retrieved_attrs,
						   Datum *row,
						   bool *is_null)
{
	ListCell   *lc = NULL;
	int			attid = 0;

	memset(row, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset(is_null, true, sizeof(bool) * tupleDescriptor->natts);

	foreach(lc, retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
		int32		pgtypmod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;

		if (sqlite3_column_type(stmt, attid) != SQLITE_NULL)
		{
			is_null[attnum] = false;
			row[attnum] = sqlite_convert_to_pg(pgtype, pgtypmod,
											   stmt, attid);
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

	SqliteFdwExecState *festate = (SqliteFdwExecState *) node->fdw_state;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	EState	   *estate = node->ss.ps.state;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	int			rc = 0;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/*
	 * If this is the first call after Begin or ReScan, we need to create the
	 * cursor on the remote side. Binding parameters is done in this function.
	 */
	if (!festate->cursor_exists)
		create_cursor(node);


	ExecClearTuple(tupleSlot);

	/*
	 * We get all rows before starting update if this scan is for update
	 * because there is no isolation between update and select on the same
	 * database connections. Please see for details:
	 * https://sqlite.org/isolation.html
	 */
	if (festate->for_update && festate->rowidx == 0)
	{
		int			size = 0;

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
										   festate->rows_isnull[festate->row_nums]);

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
									   tupleSlot->tts_values, tupleSlot->tts_isnull);
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
	SqliteFdwExecState *festate = (SqliteFdwExecState *) node->fdw_state;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	if (festate->stmt)
	{
		sqlite3_finalize(festate->stmt);
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

	SqliteFdwExecState *festate = (SqliteFdwExecState *) node->fdw_state;

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
sqliteAddForeignUpdateTargets(Query *parsetree,
							  RangeTblEntry *target_rte,
							  Relation target_relation)
{
	Oid			relid = RelationGetRelid(target_relation);
	TupleDesc	tupdesc = target_relation->rd_att;
	int			i;
	bool		has_key = false;

	/* loop through all columns of the foreign table */
	for (i = 0; i < tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber	attrno = att->attnum;
		List	   *options;
		ListCell   *option;

		/* look for the "key" option on this column */
		options = GetForeignColumnOptions(relid, attrno);	

		foreach(option, options)
		{
			DefElem    *def = (DefElem *) lfirst(option);
			/* if "key" is set, add a resjunk for this column */
			if (IS_KEY_COLUMN(def))
			{
				Var		   *var;
				TargetEntry *tle;

				/* Make a Var representing the desired value */
				var = makeVar(parsetree->resultRelation,
							  attrno,
							  att->atttypid,
							  att->atttypmod,
							  att->attcollation,
							  0);

				/* Wrap it in a resjunk TLE with the right name ... */
				tle = makeTargetEntry((Expr *) var,
									  list_length(parsetree->targetList) + 1,
									  pstrdup(NameStr(att->attname)),
									  true);

				/* ... and add it to the query's targetlist */
				parsetree->targetList = lappend(parsetree->targetList, tle);
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
	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NULL;
	StringInfoData sql;
	Oid			foreignTableId;
	TupleDesc	tupdesc;
	int			i;
	List	   *condAttr = NULL;

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
		int			attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{

		Bitmapset  *tmpset;

		AttrNumber	col;
#if (PG_VERSION_NUM >= 120000)
                tmpset = bms_union(rte->updatedCols, rte->extraUpdatedCols);
#else
                tmpset = bms_copy(rte->updatedCols);
#endif
		while ((col = bms_first_member(tmpset)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber)	/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");

			targetAttrs = lappend_int(targetAttrs, col);
		}
	}

	if (plan->returningLists)
		elog(ERROR, "RETURNING is not supported by this FDW");

	if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "not suport ON CONFLICT: %d",
			 (int) plan->onConflictAction);

	/*
	 * Add all primary key attribute names to condAttr used in where clause of
	 * update
	 */
	for (i = 0; i < tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber	attrno = att->attnum;
		List	   *options;
		ListCell   *option;

		/* look for the "key" option on this column */
		options = GetForeignColumnOptions(foreignTableId, attrno);
		foreach(option, options)
		{
			DefElem    *def = (DefElem *) lfirst(option);

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
			sqlite_deparse_insert(&sql, root, resultRelation, rel, targetAttrs);
			break;
		case CMD_UPDATE:
			sqlite_deparse_update(&sql, root, resultRelation, rel, targetAttrs, condAttr);
			break;
		case CMD_DELETE:
			sqlite_deparse_delete(&sql, root, resultRelation, rel, condAttr);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}
	table_close(rel, NoLock);
	return list_make2(makeString(sql.data), targetAttrs);
}


static void
sqliteBeginForeignModify(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo,
						 List *fdw_private,
						 int subplan_index,
						 int eflags)
{
	SqliteFdwExecState *fmstate = NULL;
	EState	   *estate = mtstate->ps.state;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	AttrNumber	n_params = 0;
	Oid			typefnoid = InvalidOid;
	bool		isvarlena = false;
	ListCell   *lc = NULL;
	Oid			foreignTableId = InvalidOid;
	ForeignTable *table;
	ForeignServer *server;
	Plan	   *subplan = mtstate->mt_plans[subplan_index]->plan;
	int			i;

	elog(DEBUG1, " duckdb_fdw : %s", __func__);

	foreignTableId = RelationGetRelid(rel);

	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case. resultRelInfon->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	fmstate = (SqliteFdwExecState *) palloc0(sizeof(SqliteFdwExecState));
	fmstate->rel = rel;

	fmstate->conn = sqlite_get_connection(server);
	fmstate->query = strVal(list_nth(fdw_private, 0));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private, 1);

	n_params = list_length(fmstate->retrieved_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "duckdb_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);

	/* Set up for remaining transmittable parameters */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc);
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1);

		Assert(!attr->attisdropped);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);
	n_params = list_length(fmstate->retrieved_attrs);
	/* Initialize sqlite statment */
	fmstate->stmt = NULL;
	/* Prepare sqlite statment */
	sqlite_prepare_wrapper(fmstate->conn, fmstate->query, &fmstate->stmt, NULL);
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
													 ,false
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
	SqliteFdwExecState *fmstate = (SqliteFdwExecState *) resultRelInfo->ri_FdwState;
	ListCell   *lc;
	Datum		value = 0;
	MemoryContext oldcontext;
	int			rc = SQLITE_OK;
	int			nestlevel;
	int			bindnum = 0;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);


	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	nestlevel = sqlite_set_transmission_modes();
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			type = TupleDescAttr(slot->tts_tupleDescriptor, attnum)->atttypid;
		bool		isnull;

		value = slot_getattr(slot, attnum + 1, &isnull);
		sqlite_bind_sql_var(type, bindnum, value, fmstate->stmt, &isnull);
		bindnum++;
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

	return slot;
}

static void
bindJunkColumnValue(SqliteFdwExecState * fmstate,
					TupleTableSlot *slot,
					TupleTableSlot *planSlot,
					Oid foreignTableId,
					int bindnum)
{
	int			i;
	Datum		value;
	Oid			typeoid;
	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	/* Bind where condition using junk column */
	for (i = 0; i < slot->tts_tupleDescriptor->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(slot->tts_tupleDescriptor, i);
		AttrNumber	attrno = att->attnum;
		List	   *options;
		ListCell   *option;

		/* look for the "key" option on this column */
		if (fmstate->junk_idx[i] == InvalidAttrNumber)
			continue;
		options = GetForeignColumnOptions(foreignTableId, attrno);
		foreach(option, options)
		{
			DefElem    *def = (DefElem *) lfirst(option);
			bool		is_null = false;

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

	SqliteFdwExecState *fmstate = (SqliteFdwExecState *) resultRelInfo->ri_FdwState;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	Oid			foreignTableId = RelationGetRelid(rel);
	ListCell   *lc = NULL;
	int			bindnum = 0;
	int			i = 0;
	int			rc = 0;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	bindJunkColumnValue(fmstate, slot, planSlot, foreignTableId, bindnum);
	bindnum++;
	/* Bind the values */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc);
		Oid			type;
		bool		is_null;
		Datum		value = 0;

		/* first attribute cannot be in target list attribute */
		type = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid;

		value = slot_getattr(slot, attnum, &is_null);
		
		sqlite_bind_sql_var(type, bindnum, value, fmstate->stmt, &is_null);
		bindnum++;
		i++;
	}
	
	// bindJunkColumnValue(fmstate, slot, planSlot, foreignTableId, bindnum);

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
	SqliteFdwExecState *fmstate = (SqliteFdwExecState *) resultRelInfo->ri_FdwState;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	Oid			foreignTableId = RelationGetRelid(rel);
	int			rc = 0;

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

	SqliteFdwExecState *fmstate = (SqliteFdwExecState *) resultRelInfo->ri_FdwState;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	if (fmstate && fmstate->stmt)
	{
		sqlite3_finalize(fmstate->stmt);
		fmstate->stmt = NULL;
	}
}


static void
sqliteExplainForeignScan(ForeignScanState *node,
						 struct ExplainState *es)
{

	SqliteFdwExecState *festate = (SqliteFdwExecState *) node->fdw_state;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	if (es->verbose)
	{
		ExplainPropertyText("SQLite query", festate->query, es);
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
}



static bool
sqliteAnalyzeForeignTable(Relation relation,
						  AcquireSampleRowsFunc *func,
						  BlockNumber *totalpages)
{
	elog(DEBUG1, "duckdb_fdw : %s", __func__);
	return false;
}


/*
 * Import a foreign schema
 */
static List *
sqliteImportForeignSchema(ImportForeignSchemaStmt *stmt,
						  Oid serverOid)
{
	sqlite3    *volatile db = NULL;
	sqlite3_stmt *volatile sql_stmt = NULL;
	sqlite3_stmt *volatile pragma_stmt = NULL;
	ForeignServer *server;
	ListCell   *lc;
	StringInfoData buf;
	List	   *commands = NIL;
	bool		import_default = false;
	bool		import_not_null = true;

	elog(DEBUG1, "duckdb_fdw : %s", __func__);

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "import_default") == 0)
			import_default = defGetBoolean(def);
		else if (strcmp(def->defname, "import_not_null") == 0)
			import_not_null = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
	}

	server = GetForeignServerByName(stmt->server_name, false);
	db = sqlite_get_connection(server);

	PG_TRY();
	{
		/* You want all tables, except system tables */
		initStringInfo(&buf);
		appendStringInfo(&buf, "SELECT name FROM sqlite_master() WHERE type in('table', 'view') AND name NOT LIKE 'sqlite_%%'");

		/* Apply restrictions for LIMIT TO and EXCEPT */
		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
			stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			bool		first_item = true;

			appendStringInfoString(&buf, " AND name ");
			if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
				appendStringInfoString(&buf, "NOT ");
			appendStringInfoString(&buf, "IN (");

			foreach(lc, stmt->table_list)
			{
				RangeVar   *rv = (RangeVar *) lfirst(lc);

				if (first_item)
					first_item = false;
				else
					appendStringInfoString(&buf, ", ");

				appendStringInfoString(&buf, quote_literal_cstr(rv->relname));
			}
			appendStringInfoChar(&buf, ')');
		}

		sqlite_prepare_wrapper(db, buf.data, (sqlite3_stmt * *) & sql_stmt, NULL);

		/* Scan all rows for this table */
		for (;;)
		{

			char	   *table;
			char	   *query;
			bool		first_item = true;
			int			rc = sqlite3_step(sql_stmt);

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
			table = (char *) sqlite3_column_text(sql_stmt, 0);

			resetStringInfo(&buf);
			appendStringInfo(&buf, "CREATE FOREIGN TABLE %s.%s (\n",
							 quote_identifier(stmt->local_schema), quote_identifier(table));

			query = palloc0(strlen(table) + 30);
			sprintf(query, "PRAGMA table_info('%s')", quote_identifier(table));

			sqlite_prepare_wrapper(db, query, (sqlite3_stmt * *) & pragma_stmt, NULL);

			for (;;)
			{
				char	   *col_name;
				char	   *type_name;
				bool		not_null;
				char	   *default_val;
				int			primary_key;
				int			rc = sqlite3_step(pragma_stmt);

				if (rc == SQLITE_DONE)
					break;
				else if (rc != SQLITE_ROW)
				{
					/* Not pass sql_stmt because it is finalized in PG_CATCH */
					sqlitefdw_report_error(ERROR, NULL, db, sqlite3_sql(pragma_stmt), rc);
				}
				col_name = (char *) sqlite3_column_text(pragma_stmt, 1);
				type_name = (char *) sqlite3_column_text(pragma_stmt, 2);
				not_null = (sqlite3_column_int(pragma_stmt, 3) == 1);
				default_val = (char *) sqlite3_column_text(pragma_stmt, 4);
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

			appendStringInfo(&buf, "\n) SERVER %s\nOPTIONS (table ",
							 quote_identifier(stmt->server_name));
			sqlite_deparse_string_literal(&buf, table);
			appendStringInfoString(&buf, ");");
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
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the foreign server.  As a side effect, save information we obtain in
 * this function to SqliteFdwRelationInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel)
{
	Query	   *query = root->parse;
	PathTarget *grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) grouped_rel->fdw_private;
	SqliteFdwRelationInfo *ofpinfo;
	List	   *aggvars;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* Grouping Sets are not pushable */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (SqliteFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underneath input relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;


	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

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
			if (sqlite_is_foreign_expr(root, grouped_rel, expr))
			{
				/* Pushable, add to tlist */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{


				/* Not matched exactly, pull the var with aggregates then */
				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				if (!sqlite_is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
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
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
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
		ListCell   *lc;

		foreach(lc, (List *) query->havingQual)
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));

#if (PG_VERSION_NUM >= 100000)
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
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
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

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
	 * first) of the calls to estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;


	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();

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
						   ,void *extra
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
		!((SqliteFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if ((stage != UPPERREL_GROUP_AGG && stage != UPPERREL_ORDERED && stage != UPPERREL_FINAL) || output_rel->fdw_private)
		return;

	fpinfo = (SqliteFdwRelationInfo *) palloc0(sizeof(SqliteFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	fpinfo->stage = stage;
	output_rel->fdw_private = fpinfo;

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			add_foreign_grouping_paths(root, input_rel, output_rel
#if (PG_VERSION_NUM >= 110000)
									   , (GroupPathExtraData *) extra
#endif
									   );
			break;
		case UPPERREL_ORDERED:
			add_foreign_ordered_paths(root, input_rel, output_rel);
			break;
		case UPPERREL_FINAL:
			add_foreign_final_paths(root, input_rel, output_rel
#if (PG_VERSION_NUM >= 120000)
									   , (FinalPathExtraData *) extra
#endif
									   );
			break;
		default:
			elog(ERROR, "unexpected upper relation: %d", (int) stage);
			break;
	}
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
						   RelOptInfo *grouped_rel
#if (PG_VERSION_NUM >= 110000)
						   , GroupPathExtraData *extra
#endif
						   )
{
	Query	   *parse = root->parse;
	SqliteFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	SqliteFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

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
	if (!foreign_grouping_ok(root, grouped_rel))
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
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */
#else
	grouppath = create_foreignscan_path(root,
										grouped_rel,
										root->upper_targets[UPPERREL_GROUP_AGG],
										rows,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										NULL,	/* no required_outer */
										NULL,
										NIL);	/* no fdw_private */
#endif

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}


/*
 * add_foreign_ordered_paths
 *		Add foreign paths for performing the final sort remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given ordered_rel.
 */
static void
add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
						  RelOptInfo *ordered_rel)
{
	Query	   *parse = root->parse;
	SqliteFdwRelationInfo *ifpinfo = input_rel->fdw_private;
	SqliteFdwRelationInfo *fpinfo = ordered_rel->fdw_private;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *ordered_path;
	ListCell   *lc;

	/* Shouldn't get here unless the query has ORDER BY */
	Assert(parse->sortClause);

#if (PG_VERSION_NUM >= 100000)
	/* We don't support cases where there are any SRFs in the targetlist */
	if (parse->hasTargetSRFs)
		return;
#else
	/* We don't support cases where there are any SRFs in the targetlist (PG Version >10) */
	if (expression_returns_set((Node *) parse->targetList))
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
	foreach(lc, root->sort_pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		Expr	   *sort_expr;
		RelOptInfo* fallback_rel = (input_rel->reloptkind == RELOPT_UPPER_REL) ?
			ifpinfo->outerrel : input_rel;

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
											 NULL,	/* no extra plan */
											 fdw_private);
#else
	/*
	 * We use root->upper_targets[UPERREL_FINAL] because until PG12, upper_targets[UPPERREL_ORDERED] is not filled.
	 * Anyways, in PG12 root->upper_targets[UPPERREL_FINAL] and root->upper_targets[UPPERREL_ORDERED] have the same value.
	 * More info: backend/optimizer/plan/planner.c (Line 2189)
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
											 NULL,	/* no extra plan */
											 fdw_private);
#endif

	/* and add it to the ordered_rel */
	add_path(ordered_rel, (Path *) ordered_path);
}

/*
 * add_foreign_final_paths
 *		Add foreign paths for performing the final processing remotely.
 *
 * Given input_rel contains the source-data Paths.  The paths are added to the
 * given final_rel.
 */
static void
add_foreign_final_paths(PlannerInfo *root, RelOptInfo *input_rel,
						RelOptInfo *final_rel
#if (PG_VERSION_NUM >= 120000)
						, FinalPathExtraData *extra
#endif
)
{
	Query	   *parse = root->parse;
	SqliteFdwRelationInfo *ifpinfo = (SqliteFdwRelationInfo *) input_rel->fdw_private;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) final_rel->fdw_private;
	bool		has_final_sort = false;
	List	   *pathkeys = NIL;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	List	   *fdw_private;
	ForeignPath *final_path;
#if (PG_VERSION_NUM < 120000)
	bool 		has_limit = limit_needed(parse);
#endif

	/*
	 * Currently, we only support this for SELECT commands
	 */
	if (parse->commandType != CMD_SELECT)
		return;

	/*
	 * No work if there is FOR UPDATE/SHARE clause and if there is no need
	 * to add a LIMIT node. We DONT support FOR UPDATE pushdown because SQLITE has no implemented yet,
	 * that's why we dont do nothing.
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
	/* We don't support cases where there are any SRFs in the targetlist (PG Version >10) */
	if (expression_returns_set((Node *) parse->targetList))
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
		ifpinfo = (SqliteFdwRelationInfo *) input_rel->fdw_private;
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

	/*
	 * Also, the LIMIT/OFFSET cannot be pushed down, if their expressions are
	 * not safe to remote.
	 */
	if (!sqlite_is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset) ||
		!sqlite_is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
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
										   NULL,	/* no extra plan */
										   fdw_private);
#else
	final_path = create_foreignscan_path(root,
										   input_rel,
										   root->upper_targets[UPPERREL_FINAL],
										   rows,
										   startup_cost,
										   total_cost,
										   pathkeys,
										   NULL, 	/* no required_outer */
										   NULL,	/* no extra plan */
										   fdw_private);
#endif
	/* and add it to the final_rel */
	add_path(final_rel, (Path *) final_path);
}

//todo:
static int
get_estimate(Oid foreigntableid)
{
	return DEFAULT_ROW_ESTIMATE;
	// sqlite3    *db;
	// sqlite3_stmt *stmt;
	// char	   *query;
	// size_t		len;
	// sqlite_opt *opt;
	// int			rows = DEFAULT_ROW_ESTIMATE;
	// ForeignTable *table;
	// int			rc;

	// opt = sqlite_get_options(foreigntableid);
	// table = GetForeignTable(foreigntableid);

	// db = sqlite_get_connection(GetForeignServer(table->serverid));

	// len = strlen(opt->svr_table) + 60;
	// query = (char *) palloc(len);
	// snprintf(query, len, "SELECT stat FROM sqlite_stat1 WHERE tbl='%s' AND idx IS NULL", opt->svr_table);
	// rc = sqlite3_prepare_v2(db, query, -1, &stmt, NULL);
	// if (rc != SQLITE_OK)
	// {
	// 	const char *err = sqlite3_errmsg(db);

	// 	if (strcmp(err, "no such table: sqlite_stat1") != 0)
	// 		elog(ERROR, "prepare failed with rc=%d msg=%s", rc, err);
	// 	return DEFAULT_ROW_ESTIMATE;
	// }

	// rc = sqlite3_step(stmt);
	// if (rc == SQLITE_ROW)
	// 	rows = sqlite3_column_int(stmt, 0);
	// else if (rc != SQLITE_DONE)
	// 	sqlitefdw_report_error(ERROR, stmt, db, query, rc);

	// sqlite3_finalize(stmt);
	// return rows;
}



static void
sqlite_to_pg_type(StringInfo str, char *type)
{
	int			i;

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
		{"bit"},				/* bit(n) and bit varying(n) */
		{"boolean"},
		{"varchar"},
		{"char"},
		{NULL}
	};

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
int
sqlite_set_transmission_modes(void)
{
	int			nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle", "ISO",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle", "postgres",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits", "3",
								 PGC_USERSET, PGC_S_SESSION,
								 GUC_ACTION_SAVE, true, 0, false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
sqlite_reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
					 List *fdw_exprs,
					 int numParams,
					 FmgrInfo **param_flinfo,
					 List **param_exprs,
					 const char ***param_values,
					 Oid **param_types)
{
	int			i;
	ListCell   *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * numParams);
	*param_types = (Oid *) palloc0(sizeof(Oid) * numParams);
	i = 0;
	foreach(lc, fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

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
	*param_exprs = (List *) ExecInitExprList(fdw_exprs, node);
#else
	*param_exprs = (List *) ExecInitExpr((Expr *) fdw_exprs, node);
#endif
	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values and bind parameters
 *
 */
static void
process_query_params(ExprContext *econtext,
					 FmgrInfo *param_flinfo,
					 List *param_exprs,
					 const char **param_values,
					 sqlite3_stmt * *stmt,
					 Oid *param_types)
{
	int			i;
	ListCell   *lc;
	int			nestlevel;

	nestlevel = sqlite_set_transmission_modes();
	i = 0;
	foreach(lc, param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

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
create_cursor(ForeignScanState *node)
{
	SqliteFdwExecState *festate = (SqliteFdwExecState *) node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int			numParams = festate->numParams;
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

		process_query_params(econtext,
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
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 */
Expr *
sqlite_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell   *lc_em;

	foreach(lc_em, ec->ec_members)
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
	ListCell   *lc1;
	int			i;
	Expr	   *fallback_expr;

	i = 0;
	foreach(lc1, target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc1);
		Index		sgref = get_pathtarget_sortgroupref(target, i);
		ListCell   *lc2;

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
			expr = ((RelabelType *) expr)->arg;

		/* Locate an EquivalenceClass member matching this expr, if any */
		foreach(lc2, ec->ec_members)
		{
			EquivalenceMember *em = (EquivalenceMember *) lfirst(lc2);
			Expr	   *em_expr;

			/* Don't match constants */
			if (em->em_is_const)
				continue;

			/* Ignore child members */
			if (em->em_is_child)
				continue;

			/* Match if same expression (after stripping relabel) */
			em_expr = em->em_expr;
			while (em_expr && IsA(em_expr, RelabelType))
				em_expr = ((RelabelType *) em_expr)->arg;

			if (equal(em_expr, expr))
				return em->em_expr;
		}

		i++;
	}

	/* 
	 * We add this method as fallback in versions prior to PG11/12 because target->sortgrouprefs its not filled and
	 * this function always fails because cannot find sort expression.
	 */
	fallback_expr = sqlite_find_em_expr_for_rel(ec, fallbackRel);

	if (fallback_expr)
		return fallback_expr;

	elog(ERROR, "could not find pathkey item to sort");
	return NULL;				/* keep compiler quiet */
}
