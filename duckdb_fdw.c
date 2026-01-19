#include "postgres.h"
#include "duckdb_fdw.h"
#include "utils/uuid.h"
#include "utils/numeric.h"
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_user_mapping.h"
#include "miscadmin.h"
#include "executor/executor.h"

PG_MODULE_MAGIC;

static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
                RelOptInfo *outerrel, RelOptInfo *innerrel,
                void *extra)
{
    DuckDBFdwRelationInfo *fpinfo;
    DuckDBFdwRelationInfo *ofpinfo;
    DuckDBFdwRelationInfo *ifpinfo;
    ListCell   *lc;

    /*
     * We can only push down joins between two foreign tables from the same
     * server.
     */
    ofpinfo = (DuckDBFdwRelationInfo *) outerrel->fdw_private;
    ifpinfo = (DuckDBFdwRelationInfo *) innerrel->fdw_private;

    if (ofpinfo == NULL || ifpinfo == NULL)
    {
        return false;
    }

    if (ofpinfo->server == NULL || ifpinfo->server == NULL)
    {
        return false;
    }

    if (ofpinfo->server->serverid != ifpinfo->server->serverid)
    {
        return false;
    }

    /*
     * If they have different user mappings, they might have different 
     * permissions or connection settings. But if both are NULL (local DuckDB), 
     * it is fine.
     */
    if (ofpinfo->user != ifpinfo->user &&
        (ofpinfo->user == NULL || ifpinfo->user == NULL ||
         ofpinfo->user->umid != ifpinfo->user->umid))
    {
        return false;
    }

    /*
     * If either of the input relations is not pushable, the join is not
     * pushable either.
     */
    if (!ofpinfo->pushdown_safe || !ifpinfo->pushdown_safe)
    {
        return false;
    }

    /*
     * Create a DuckDBFdwRelationInfo for the join relation.
     */
    fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
    fpinfo->pushdown_safe = true;
    fpinfo->server = ofpinfo->server;
    fpinfo->user = ofpinfo->user;
    fpinfo->outerrel = outerrel;
    fpinfo->innerrel = innerrel;
    fpinfo->jointype = jointype;

    /*
     * Estimate rows and width.
     */
    fpinfo->rows = (ofpinfo->rows > ifpinfo->rows) ? ofpinfo->rows : ifpinfo->rows; 
    fpinfo->width = ofpinfo->width + ifpinfo->width;
    joinrel->rows = fpinfo->rows;

    /*
     * Identify pushable join clauses.
     */
    duckdb_classify_conditions(root, joinrel, ((JoinPathExtraData *) extra)->restrictlist,
                                &fpinfo->joinclauses, &fpinfo->local_conds);

    /*
     * Identify pushable other quals.
     */
    duckdb_classify_conditions(root, joinrel, joinrel->baserestrictinfo,
                                &fpinfo->remote_conds, &fpinfo->local_conds);

    /*
     * Set up glob_cxt for checking pushability of the join relation.
     */
    {
        foreign_glob_cxt glob_cxt;
        glob_cxt.root = root;
        glob_cxt.foreignrel = joinrel;
        glob_cxt.relids = joinrel->relids;

        /*
         * Check if the join's target list is pushable.
         */
        foreach(lc, joinrel->reltarget->exprs)
        {
            Node *n = (Node *) lfirst(lc);
            if (!duckdb_is_foreign_expr_full(root, joinrel, (Expr *) n, &glob_cxt))
                return false;
        }

        /*
         * Check if the join clauses are pushable.
         */
        foreach(lc, fpinfo->joinclauses)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            if (!duckdb_is_foreign_expr_full(root, joinrel, ri->clause, &glob_cxt))
                return false;
        }
    }

    joinrel->fdw_private = (void *) fpinfo;
    return true;
}

static void
duckdbGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel,
                          RelOptInfo *outerrel, RelOptInfo *innerrel,
                          JoinType jointype, JoinPathExtraData *extra)
{
    /*
     * If the join is pushable, add a foreign join path.
     */
    if (foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
    {
        double      rows = joinrel->rows;
        Cost        startup_cost = 0;
        Cost        total_cost = rows;

        add_path(joinrel, (Path *)
                 create_foreignscan_path(root, joinrel,
                                          joinrel->reltarget,
                                          rows,
                                          startup_cost,
                                          total_cost,
                                          NIL,
                                          joinrel->lateral_relids,
                                          NULL,
                                          NIL));
    }
}

static void
duckdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
    baserel->fdw_private = (void *) fpinfo;
    fpinfo->foreigntableid = foreigntableid;
    fpinfo->table = GetForeignTable(foreigntableid);
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);
    
    HeapTuple tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                                   ObjectIdGetDatum(GetUserId()),
                                   ObjectIdGetDatum(fpinfo->server->serverid));
    if (HeapTupleIsValid(tp))
    {
        fpinfo->user = GetUserMapping(GetUserId(), fpinfo->server->serverid);
        ReleaseSysCache(tp);
    }
    
    fpinfo->pushdown_safe = true;
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
    baserel->rows = 1000;
    fpinfo->rows = baserel->rows;
    fpinfo->width = baserel->reltarget->width;
}

static void
duckdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    /*
     * Create a ForeignPath node and add it as only possible path.  We use the
     * same cost as for a regular scan, but without any cpu_tuple_cost.
     */
    add_path(baserel, (Path *)
             create_foreignscan_path(root, baserel,
                                     NULL,  /* default target */
                                     baserel->rows,
                                     10,
                                     baserel->rows + 10,
                                     NIL,   /* no pathkeys */
                                     NULL,  /* no required_outer */
                                     NULL,  /* no fdw_outerpath */
                                     NIL)); /* no fdw_private */
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
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) baserel->fdw_private;
    Index       scanrelid;
    Oid         rel_oid = foreigntableid;

    initStringInfo(&sql);

    if (IS_UPPER_REL(baserel))
    {
        /* Aggregation pushdown */
        scanrelid = 0;
        deparse_tlist = tlist;
        rel_oid = fpinfo->foreigntableid;
    }
    else if (IS_JOIN_REL(baserel))
    {
        /* Join pushdown */
        scanrelid = 0;
        deparse_tlist = tlist;
        /* Use the OID of the first foreign table involved in the join as a dummy */
        rel_oid = fpinfo->server->serverid; 
    }
    else
    {
        /* Base relation scan */
        scanrelid = baserel->relid;
        deparse_tlist = duckdb_build_tlist_to_deparse(baserel);
    }

    duckdb_deparse_select_stmt_for_rel(&sql, root, baserel, deparse_tlist, fpinfo->remote_conds, NIL, false, false, false, &retrieved_attrs, &params_list);

    fdw_private = list_make4(makeString(sql.data), 
                             retrieved_attrs, 
                             makeInteger(rel_oid),
                             makeInteger(fpinfo->server->serverid));
    
    return make_foreignscan(tlist, extract_actual_clauses(fpinfo->local_conds, false), scanrelid, NIL, fdw_private, (IS_UPPER_REL(baserel) || IS_JOIN_REL(baserel) ? tlist : NIL), NIL, outer_plan);
}

static void
duckdbBeginForeignScan(ForeignScanState *node, int eflags)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    ForeignTable *table;
    Oid foreigntableid;
    
    node->fdw_state = (void *)festate;

    if (fsplan->scan.scanrelid > 0)
    {
        festate->tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
        foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
        table = GetForeignTable(foreigntableid);
        festate->conn = duckdb_get_connection(GetForeignServer(table->serverid), false);
    }
    else
    {
        if (node->ss.ss_ScanTupleSlot)
            festate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
        Oid serverid = intVal(list_nth(fsplan->fdw_private, 3));
        festate->conn = duckdb_get_connection(GetForeignServer(serverid), false);
    }

    festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);
    festate->query = strVal(list_nth(fsplan->fdw_private, 0));
    festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
    
    if (duckdb_query(festate->conn, festate->query, &festate->res) == DuckDBError)
    {
        elog(ERROR, "duckdb_fdw: query failed: %s", duckdb_result_error(&festate->res));
    }

    festate->current_chunk_idx = 0;
    festate->current_chunk_row_idx = 0;
    festate->current_chunk_row_count = duckdb_row_count(&festate->res);
    festate->is_started = true;
}

static Datum
duckdb_value_to_pg(DuckDBFdwExecState *festate, int col_idx, uint64_t global_row, Oid pgtype)
{
    if (duckdb_value_is_null(&festate->res, col_idx, global_row))
        return (Datum)0;

    switch (pgtype)
    {
        case BOOLOID:
            return BoolGetDatum(duckdb_value_boolean(&festate->res, col_idx, global_row));
        case INT2OID:
            return Int16GetDatum(duckdb_value_int16(&festate->res, col_idx, global_row));
        case INT4OID:
            return Int32GetDatum(duckdb_value_int32(&festate->res, col_idx, global_row));
        case INT8OID:
            return Int64GetDatum(duckdb_value_int64(&festate->res, col_idx, global_row));
        case FLOAT4OID:
            return Float4GetDatum(duckdb_value_float(&festate->res, col_idx, global_row));
        case FLOAT8OID:
            return Float8GetDatum(duckdb_value_double(&festate->res, col_idx, global_row));
        case DATEOID:
            return Int32GetDatum(duckdb_value_date(&festate->res, col_idx, global_row).days - 10957);
        case UUIDOID: {
            char *s = duckdb_value_varchar(&festate->res, col_idx, global_row);
            Datum res = DirectFunctionCall1(uuid_in, CStringGetDatum(s));
            duckdb_free(s);
            return res;
        }
        default: {
            char *s = duckdb_value_varchar(&festate->res, col_idx, global_row);
            Datum res;
            if (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID)
                res = PointerGetDatum(cstring_to_text(s));
            else {
                Oid typinput, typioparam;
                getTypeInputInfo(pgtype, &typinput, &typioparam);
                res = OidInputFunctionCall(typinput, s, typioparam, -1);
            }
            duckdb_free(s);
            return res;
        }
    }
}

static TupleTableSlot *
duckdbIterateForeignScan(ForeignScanState *node)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    Datum      *values = slot->tts_values;
    bool       *nulls = slot->tts_isnull;
    ListCell   *lc;
    int         i;

    ExecClearTuple(slot);

    if (festate->current_chunk_row_idx >= festate->current_chunk_row_count)
        return slot;

    i = 0;
    foreach(lc, festate->retrieved_attrs)
    {
        int attnum_pg = lfirst_int(lc);
        if (attnum_pg <= 0) { i++; continue; } 

        int attnum_idx = attnum_pg - 1;
        Oid pgtype = festate->tupdesc->attrs[attnum_idx].atttypid;
        
        values[attnum_idx] = duckdb_value_to_pg(festate, i, festate->current_chunk_row_idx, pgtype);
        nulls[attnum_idx] = duckdb_value_is_null(&festate->res, i, festate->current_chunk_row_idx);
        i++;
    }

    ExecStoreVirtualTuple(slot);
    festate->current_chunk_row_idx++;
    return slot;
}

static void
duckdbEndForeignScan(ForeignScanState *node)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    if (festate)
    {
        duckdb_destroy_result(&festate->res);
    }
}

static void
duckdbReScanForeignScan(ForeignScanState *node)
{
    duckdbEndForeignScan(node);
    duckdbBeginForeignScan(node, 0);
}

static void
duckdbGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
                            RelOptInfo *input_rel, RelOptInfo *output_rel,
                            void *extra)
{
    DuckDBFdwRelationInfo *fpinfo;

    if (input_rel == NULL || input_rel->fdw_private == NULL ||
        !((DuckDBFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
        return;

    if (stage != UPPERREL_GROUP_AGG)
        return;

    fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
    fpinfo->pushdown_safe = false;
    fpinfo->outerrel = input_rel;
    fpinfo->foreigntableid = ((DuckDBFdwRelationInfo *) input_rel->fdw_private)->foreigntableid;
    fpinfo->server = ((DuckDBFdwRelationInfo *) input_rel->fdw_private)->server;
    fpinfo->user = ((DuckDBFdwRelationInfo *) input_rel->fdw_private)->user;
    output_rel->fdw_private = fpinfo;

    if (duckdb_is_foreign_expr(root, output_rel, (Expr *) output_rel->reltarget->exprs))
    {
        fpinfo->pushdown_safe = true;
        add_path(output_rel, (Path *)
                 create_foreignscan_path(root, output_rel,
                                          output_rel->reltarget,
                                          output_rel->rows,
                                          10, 20, NIL,
                                          NULL, NULL, NIL));
    }
}

static void
duckdbAddForeignUpdateTargets(PlannerInfo *root,
                              Index rtindex,
                              RangeTblEntry *target_rte,
                              Relation target_relation)
{
}

static List *
duckdbPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
    return NIL;
}

static bool
duckdbPlanDirectModify(PlannerInfo *root,
                       ModifyTable *plan,
                       Index resultRelation,
                       int subplan_index)
{
    RelOptInfo *foreignrel = root->simple_rel_array[resultRelation];
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) foreignrel->fdw_private;

    /*
     * If the modify is pushable, return true.
     */
    if (fpinfo->pushdown_safe && fpinfo->local_conds == NIL)
        return true;

    return false;
}

static void
duckdbBeginDirectModify(ForeignScanState *node, int eflags)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    EState     *estate = node->ss.ps.state;
    ResultRelInfo *resultRelInfo = estate->es_result_relations[fsplan->resultRelation - 1];
    Relation    rel;
    Oid         serverid;
    StringInfoData sql;
    List       *retrieved_attrs = NIL;
    List       *params_list = NIL;

    if (resultRelInfo == NULL)
        elog(ERROR, "duckdb_fdw: no result relation for direct modify");

    rel = resultRelInfo->ri_RelationDesc;
    serverid = GetForeignTable(RelationGetRelid(rel))->serverid;

    initStringInfo(&sql);
    node->fdw_state = (void *)festate;
    festate->conn = duckdb_get_connection(GetForeignServer(serverid), false);

    if (fsplan->operation == CMD_UPDATE)
    {
        duckdb_deparse_direct_update_sql(&sql, NULL, /* root */
                                          fsplan->scan.scanrelid, rel,
                                          NULL, /* foreignrel */
                                          fsplan->fdw_scan_tlist,
                                          fsplan->fdw_scan_tlist, /* targetAttrs */
                                          fsplan->scan.plan.qual,
                                          &params_list, &retrieved_attrs);
    }
    else if (fsplan->operation == CMD_DELETE)
    {
        duckdb_deparse_direct_delete_sql(&sql, NULL, /* root */
                                          fsplan->scan.scanrelid, rel,
                                          NULL, /* foreignrel */
                                          fsplan->scan.plan.qual,
                                          &params_list, &retrieved_attrs);
    }

    festate->query = sql.data;
}

static TupleTableSlot *
duckdbIterateDirectModify(ForeignScanState *node)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    duckdb_result res;

    if (duckdb_query(festate->conn, festate->query, &res) == DuckDBError)
    {
        const char *err = duckdb_result_error(&res);
        elog(ERROR, "duckdb_fdw: direct modify failed: %s", err ? err : "unknown error");
    }
    duckdb_destroy_result(&res);

    return ExecClearTuple(node->ss.ss_ScanTupleSlot);
}

static void
duckdbEndDirectModify(ForeignScanState *node)
{
}

static void
duckdbBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
    Relation rel = resultRelInfo->ri_RelationDesc;
    duckdb_opt *options = duckdb_get_options(RelationGetRelid(rel));
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    
    festate->conn = duckdb_get_connection(GetForeignServer(table->serverid), false);
    festate->table_name = options->svr_table;
    festate->tupdesc = RelationGetDescr(rel);
    
    /* Create Appender */
    if (duckdb_appender_create(festate->conn, NULL, festate->table_name, &festate->appender) == DuckDBError)
    {
        /* Try to get error from appender if created, else generic */
        elog(ERROR, "duckdb_fdw: failed to create appender for table %s", festate->table_name);
    }
    
    festate->batch_row_count = 0;
    resultRelInfo->ri_FdwState = (void *)festate;
}

static void
duckdb_append_slot(duckdb_appender appender, TupleTableSlot *slot, TupleDesc tupdesc)
{
    int natts = tupdesc->natts;
    
    if (duckdb_appender_begin_row(appender) == DuckDBError)
        elog(ERROR, "duckdb_fdw: appender begin row failed: %s", duckdb_appender_error(appender));

    for (int i = 0; i < natts; i++)
    {
        bool isnull;
        Datum val = slot_getattr(slot, i + 1, &isnull);

        if (isnull)
        {
            duckdb_append_null(appender);
            continue;
        }

        Oid pgtype = tupdesc->attrs[i].atttypid;
        
        switch (pgtype)
        {
            case INT2OID:
                duckdb_append_int16(appender, DatumGetInt16(val));
                break;
            case INT4OID:
                duckdb_append_int32(appender, DatumGetInt32(val));
                break;
            case INT8OID:
                duckdb_append_int64(appender, DatumGetInt64(val));
                break;
            case FLOAT4OID:
                duckdb_append_float(appender, DatumGetFloat4(val));
                break;
            case FLOAT8OID:
                duckdb_append_double(appender, DatumGetFloat8(val));
                break;
            case BOOLOID:
                duckdb_append_bool(appender, DatumGetBool(val));
                break;
            case TEXTOID:
            case VARCHAROID:
            case BPCHAROID:
            {
                char *s = TextDatumGetCString(val);
                duckdb_append_varchar(appender, s);
                pfree(s);
                break;
            }
            case DATEOID:
            {
                int32_t days_pg = DatumGetInt32(val);
                duckdb_date d;
                d.days = days_pg + 10957; 
                duckdb_append_date(appender, d);
                break;
            }
            /* TODO: Add Timestamp support here */
            default:
            {
                Oid typoutput;
                bool typIsVarlena;
                getTypeOutputInfo(pgtype, &typoutput, &typIsVarlena);
                char *s = OidOutputFunctionCall(typoutput, val);
                duckdb_append_varchar(appender, s);
                pfree(s);
            }
        }
    }

    if (duckdb_appender_end_row(appender) == DuckDBError)
        elog(ERROR, "duckdb_fdw: appender end row failed: %s", duckdb_appender_error(appender));
}

static int
duckdbGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
    return 2048; /* Match DuckDB vector size */
}

static TupleTableSlot **
duckdbExecForeignBatchInsert(EState *executor,
                             ResultRelInfo *resultRelInfo,
                             TupleTableSlot **slots,
                             TupleTableSlot **planSlots,
                             int *numSlots)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)resultRelInfo->ri_FdwState;
    int i;

    for (i = 0; i < *numSlots; i++)
    {
        duckdb_append_slot(festate->appender, slots[i], festate->tupdesc);
    }
    
    festate->batch_row_count += *numSlots;
    return slots;
}

static TupleTableSlot *
duckdbExecForeignInsert(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)resultRelInfo->ri_FdwState;
    duckdb_append_slot(festate->appender, slot, festate->tupdesc);
    festate->batch_row_count++;
    return slot;
}

static TupleTableSlot *
duckdbExecForeignUpdate(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    elog(ERROR, "duckdb_fdw: UPDATE not implemented yet using Appender API");
    return slot;
}

static TupleTableSlot *
duckdbExecForeignDelete(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    elog(ERROR, "duckdb_fdw: DELETE not implemented yet using Appender API");
    return slot;
}

static void
duckdbEndForeignModify(EState *executor, ResultRelInfo *resultRelInfo)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)resultRelInfo->ri_FdwState;
    if (festate && festate->appender)
    {
        /* Flush is implicit on close, but explicit check is good */
        if (duckdb_appender_flush(festate->appender) == DuckDBError)
             elog(WARNING, "duckdb_fdw: appender flush failed: %s", duckdb_appender_error(festate->appender));
             
        if (duckdb_appender_close(festate->appender) == DuckDBError)
             elog(ERROR, "duckdb_fdw: appender close failed: %s", duckdb_appender_error(festate->appender));
             
        duckdb_appender_destroy(&festate->appender);
    }
}

PG_FUNCTION_INFO_V1(duckdb_fdw_handler);
Datum duckdb_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);
    fdwroutine->GetForeignRelSize = duckdbGetForeignRelSize;
    fdwroutine->GetForeignPaths = duckdbGetForeignPaths;
    fdwroutine->GetForeignPlan = duckdbGetForeignPlan;
    fdwroutine->GetForeignJoinPaths = duckdbGetForeignJoinPaths;
    fdwroutine->BeginForeignScan = duckdbBeginForeignScan;
    fdwroutine->IterateForeignScan = duckdbIterateForeignScan;
    fdwroutine->ReScanForeignScan = duckdbReScanForeignScan;
    fdwroutine->EndForeignScan = duckdbEndForeignScan;
    fdwroutine->GetForeignUpperPaths = duckdbGetForeignUpperPaths;
    fdwroutine->ImportForeignSchema = duckdb_import_foreign_schema;

    /* Write Support */
    fdwroutine->AddForeignUpdateTargets = duckdbAddForeignUpdateTargets;
    fdwroutine->PlanForeignModify = duckdbPlanForeignModify;
    fdwroutine->BeginForeignModify = duckdbBeginForeignModify;
    fdwroutine->ExecForeignInsert = duckdbExecForeignInsert;
    fdwroutine->ExecForeignUpdate = duckdbExecForeignUpdate;
    fdwroutine->ExecForeignDelete = duckdbExecForeignDelete;
    fdwroutine->EndForeignModify = duckdbEndForeignModify;
    fdwroutine->GetForeignModifyBatchSize = duckdbGetForeignModifyBatchSize;
    fdwroutine->ExecForeignBatchInsert = duckdbExecForeignBatchInsert;

    /* Direct Modify Support */
    fdwroutine->PlanDirectModify = duckdbPlanDirectModify;
    fdwroutine->BeginDirectModify = duckdbBeginDirectModify;
    fdwroutine->IterateDirectModify = duckdbIterateDirectModify;
    fdwroutine->EndDirectModify = duckdbEndDirectModify;

    PG_RETURN_POINTER(fdwroutine);
}

PG_FUNCTION_INFO_V1(duckdb_fdw_version);
Datum duckdb_fdw_version(PG_FUNCTION_ARGS) { PG_RETURN_TEXT_P(cstring_to_text(duckdb_library_version())); }

PG_FUNCTION_INFO_V1(duckdb_execute);
Datum duckdb_execute(PG_FUNCTION_ARGS) {
    char *servername = NameStr(*PG_GETARG_NAME(0));
    char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));
    duckdb_connection conn = duckdb_get_connection(GetForeignServerByName(servername, false), false);
    duckdb_do_sql_command(conn, query, LOG);
    PG_RETURN_VOID();
}

int
duckdb_set_transmission_modes(void)
{
    /* For now, just return 0 */
    return 0;
}

void
duckdb_reset_transmission_modes(int nestlevel)
{
}

Expr *
duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
    ListCell   *lc;

    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);

        if (bms_is_subset(em->em_relids, rel->relids) &&
            !bms_is_empty(em->em_relids))
            return em->em_expr;
    }

    return NULL;
}

Expr *
duckdb_find_em_expr_for_input_target(PlannerInfo *root,
                                    EquivalenceClass *ec,
                                    PathTarget *target,
                                    RelOptInfo *fallbackRel)
{
    ListCell   *lc1;

    foreach(lc1, ec->ec_members)
    {
        EquivalenceMember *em = (EquivalenceMember *) lfirst(lc1);
        ListCell   *lc2;

        foreach(lc2, target->exprs)
        {
            Expr       *expr = (Expr *) lfirst(lc2);

            if (equal(em->em_expr, expr))
                return em->em_expr;
        }
    }

    /* Fallback to rel-based search if target doesn't match directly */
    if (fallbackRel)
        return duckdb_find_em_expr_for_rel(ec, fallbackRel);

    return NULL;
}

void _PG_init(void) {}
void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, void *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost) {}