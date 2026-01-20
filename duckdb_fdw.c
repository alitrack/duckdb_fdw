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

    /* Classify conditions into remote and local */
    duckdb_classify_conditions(root, baserel, baserel->baserestrictinfo,
                                &fpinfo->remote_conds, &fpinfo->local_conds);

    /* Identify which attributes are used */
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
    
    /* Add attributes from conditions */
    ListCell *lc;
    foreach(lc, fpinfo->local_conds)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
        pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
    }
    foreach(lc, fpinfo->remote_conds)
    {
        RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
        pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
    }

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
                                     baserel->reltarget,
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
    
    if (list_length(fsplan->fdw_private) > 1)
        festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
    else
        festate->retrieved_attrs = NIL;
    
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
            if (!s) return (Datum)0;
            Datum res = DirectFunctionCall1(uuid_in, CStringGetDatum(s));
            duckdb_free(s);
            return res;
        }
        default: {
            char *s = duckdb_value_varchar(&festate->res, col_idx, global_row);
            if (!s) return (Datum)0;
            
            /* Handle array format conversion: DuckDB [1,2] -> PG {1,2} */
            size_t slen = strlen(s);
            if (slen >= 2 && s[0] == '[' && s[slen-1] == ']')
            {
                for (char *p = s; *p; p++) {
                    if (*p == '[') *p = '{';
                    else if (*p == ']') *p = '}';
                }
            }

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
    ListCell   *lc;
    int         i;

    ExecClearTuple(slot);

    if (festate->current_chunk_row_idx >= festate->current_chunk_row_count)
        return slot;

    if (festate->tupdesc == NULL)
        festate->tupdesc = slot->tts_tupleDescriptor;

    i = 0;
    foreach(lc, festate->retrieved_attrs)
    {
        int attnum_pg = lfirst_int(lc);
        if (attnum_pg > 0 && attnum_pg <= slot->tts_tupleDescriptor->natts) {
            int attnum_idx = attnum_pg - 1;
            Oid pgtype = TupleDescAttr(festate->tupdesc, attnum_idx)->atttypid;
            
            if (duckdb_value_is_null(&festate->res, i, festate->current_chunk_row_idx))
            {
                slot->tts_isnull[attnum_idx] = true;
                slot->tts_values[attnum_idx] = (Datum)0;
            }
            else
            {
                slot->tts_isnull[attnum_idx] = false;
                slot->tts_values[attnum_idx] = duckdb_value_to_pg(festate, i, festate->current_chunk_row_idx, pgtype);
            }
        }
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
    festate->conn = duckdb_get_connection(GetForeignServer(GetForeignTable(RelationGetRelid(rel))->serverid), false);
    festate->table_name = options->svr_table;
    festate->tupdesc = RelationGetDescr(rel);
    resultRelInfo->ri_FdwState = (void *)festate;
}

static TupleTableSlot *
duckdbExecForeignInsert(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)resultRelInfo->ri_FdwState;
    StringInfoData sql;
    int i;

    initStringInfo(&sql);
    appendStringInfo(&sql, "INSERT INTO %s VALUES (", festate->table_name);

    for (int i = 0; i < festate->tupdesc->natts; i++)
    {
        bool isnull;
        Datum val = slot_getattr(slot, i + 1, &isnull);
        if (i > 0) appendStringInfoString(&sql, ", ");
        if (isnull) appendStringInfoString(&sql, "NULL");
        else {
            Oid typ = TupleDescAttr(festate->tupdesc, i)->atttypid;
            Oid out; bool var; getTypeOutputInfo(typ, &out, &var);
            char *s = OidOutputFunctionCall(out, val);
            if (typ == BOOLOID) appendStringInfoString(&sql, (DatumGetBool(val) ? "true" : "false"));
            else if (typ == INT4OID || typ == INT8OID || typ == FLOAT8OID) appendStringInfoString(&sql, s);
            else {
                appendStringInfoString(&sql, "'");
                for (char *p = s; *p; p++) {
                    if (*p == '\'') appendStringInfoString(&sql, "''");
                    else appendStringInfoChar(&sql, *p);
                }
                appendStringInfoString(&sql, "'");
            }
            pfree(s);
        }
    }
    appendStringInfoString(&sql, ");");

    duckdb_result res;
    if (duckdb_query(festate->conn, sql.data, &res) == DuckDBError)
        elog(ERROR, "DuckDB insert failed: %s", duckdb_result_error(&res));
    duckdb_destroy_result(&res);
    pfree(sql.data);

    return slot;
}

static TupleTableSlot *
duckdbExecForeignUpdate(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    elog(ERROR, "UPDATE not supported");
    return slot;
}

static TupleTableSlot *
duckdbExecForeignDelete(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
    elog(ERROR, "DELETE not supported");
    return slot;
}

static void
duckdbEndForeignModify(EState *executor, ResultRelInfo *resultRelInfo)
{
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

PG_FUNCTION_INFO_V1(duckdb_create_s3_secret);
Datum duckdb_create_s3_secret(PG_FUNCTION_ARGS) {
    char *servername = NameStr(*PG_GETARG_NAME(0));
    char *secret_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
    char *key_id = text_to_cstring(PG_GETARG_TEXT_PP(2));
    char *secret = text_to_cstring(PG_GETARG_TEXT_PP(3));
    char *region = PG_ARGISNULL(4) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(4));
    
    duckdb_connection conn = duckdb_get_connection(GetForeignServerByName(servername, false), false);
    
    StringInfoData sql;
    initStringInfo(&sql);
    appendStringInfo(&sql, "CREATE OR REPLACE SECRET %s ( TYPE S3, KEY_ID '%s', SECRET '%s'", 
                     secret_name, key_id, secret);
    if (region)
        appendStringInfo(&sql, ", REGION '%s'", region);
    appendStringInfoString(&sql, " );");
    
    duckdb_do_sql_command(conn, sql.data, ERROR);
    
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