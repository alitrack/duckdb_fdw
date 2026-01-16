#include "postgres.h"
#include "duckdb_fdw.h"
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"

PG_MODULE_MAGIC;

static void
duckdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *)palloc0(sizeof(DuckDBFdwRelationInfo));
	baserel->fdw_private = (void *)fpinfo;
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
	baserel->rows = 1000;
}

static void
duckdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel, NULL, baserel->rows, 10, 20, NIL, baserel->lateral_relids, NULL, NIL));
}

static ForeignScan *
duckdbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
					 ForeignPath *best_path, List *tlist, List *scan_clauses,
					 Plan *outer_plan)
{
	StringInfoData sql;
	List *fdw_private;
	List *retrieved_attrs = NIL;
    List *params_list = NIL;
    List *deparse_tlist;

	initStringInfo(&sql);
    deparse_tlist = duckdb_build_tlist_to_deparse(baserel);
	duckdb_deparse_select_stmt_for_rel(&sql, root, baserel, deparse_tlist, NIL, NIL, false, false, false, &retrieved_attrs, &params_list);

	fdw_private = list_make2(makeString(sql.data), retrieved_attrs);
	return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, fdw_private, NIL, NIL, outer_plan);
}

static void
duckdbBeginForeignScan(ForeignScanState *node, int eflags)
{
	DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	EState *estate = node->ss.ps.state;
    RangeTblEntry *rte;
    ForeignTable *table;
	
	node->fdw_state = (void *)festate;
	festate->tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
	festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);

    rte = exec_rt_fetch(fsplan->scan.scanrelid, estate);
    table = GetForeignTable(rte->relid);
    festate->conn = duckdb_get_connection(GetForeignServer(table->serverid), false);
    
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));
	festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
    
    if (duckdb_query(festate->conn, festate->query, &festate->res) == DuckDBError)
        elog(ERROR, "duckdb_fdw: query failed: %s", duckdb_result_error(&festate->res));
        
    festate->rowidx = 0;
    festate->row_count = duckdb_row_count(&festate->res);
}

static TupleTableSlot *
duckdbIterateForeignScan(ForeignScanState *node)
{
	DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    int i;
    ListCell *lc;

	ExecClearTuple(slot);

    if (festate->rowidx >= festate->row_count)
        return slot;

    i = 0;
    foreach(lc, festate->retrieved_attrs)
    {
        int attnum = lfirst_int(lc) - 1;
        Oid pgtype = festate->tupdesc->attrs[attnum].atttypid;
        int32 pgtypmod = festate->tupdesc->attrs[attnum].atttypmod;
        
        slot->tts_values[attnum] = duckdb_convert_to_pg(pgtype, pgtypmod, &festate->res, i, festate->rowidx, festate->attinmeta, attnum);
        
        if (duckdb_value_is_null(&festate->res, i, festate->rowidx))
             slot->tts_isnull[attnum] = true;
        else
             slot->tts_isnull[attnum] = false;
        i++;
    }

    ExecStoreVirtualTuple(slot);
    festate->rowidx++;
	return slot;
}

static void
duckdbEndForeignScan(ForeignScanState *node)
{
	DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
	if (festate)
        duckdb_destroy_result(&festate->res);
}

static void
duckdbReScanForeignScan(ForeignScanState *node)
{
    duckdbEndForeignScan(node);
    duckdbBeginForeignScan(node, 0);
}

PG_FUNCTION_INFO_V1(duckdb_fdw_handler);
Datum duckdb_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	fdwroutine->GetForeignRelSize = duckdbGetForeignRelSize;
	fdwroutine->GetForeignPaths = duckdbGetForeignPaths;
	fdwroutine->GetForeignPlan = duckdbGetForeignPlan;
	fdwroutine->BeginForeignScan = duckdbBeginForeignScan;
	fdwroutine->IterateForeignScan = duckdbIterateForeignScan;
	fdwroutine->ReScanForeignScan = duckdbReScanForeignScan;
	fdwroutine->EndForeignScan = duckdbEndForeignScan;
	PG_RETURN_POINTER(fdwroutine);
}

PG_FUNCTION_INFO_V1(duckdb_fdw_version);
Datum duckdb_fdw_version(PG_FUNCTION_ARGS) { PG_RETURN_TEXT_P(cstring_to_text(duckdb_library_version())); }

PG_FUNCTION_INFO_V1(duckdb_execute);
Datum duckdb_execute(PG_FUNCTION_ARGS) {
    char *servername = NameStr(*PG_GETARG_NAME(0));
    char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));
    duckdb_connection conn = duckdb_get_connection(GetForeignServerByName(servername, false), false);
    duckdb_do_sql_command(conn, query, NOTICE);
    PG_RETURN_VOID();
}

void _PG_init(void) {}
void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, void *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost) {}
int duckdb_set_transmission_modes(void) { return 0; }
void duckdb_reset_transmission_modes(int nestlevel) {}
Expr * duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel) { return NULL; }
Expr * duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel) { return NULL; }
