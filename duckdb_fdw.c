#include "postgres.h"
#include "duckdb_fdw.h"
#include "commands/defrem.h"
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

/* Stub for transmission modes */
int duckdb_set_transmission_modes(void) { return 0; }
void duckdb_reset_transmission_modes(int nestlevel) {}

/* FDW functions */
static void duckdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
    baserel->fdw_private = (void *) fpinfo;
    fpinfo->foreigntableid = foreigntableid;
    fpinfo->table = GetForeignTable(foreigntableid);
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);
    fpinfo->pushdown_safe = true;
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
    baserel->rows = 1000;
    fpinfo->rows = 1000;
    fpinfo->width = baserel->reltarget->width;
}

static void duckdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid) {
    add_path(baserel, (Path *) create_foreignscan_path(root, baserel, NULL, baserel->rows, 10, baserel->rows + 10, NIL, NULL, NULL, NIL));
}

static ForeignScan * duckdbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan) {
    StringInfoData sql;
    List *fdw_private;
    List *retrieved_attrs = NIL;
    List *params_list = NIL;
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) baserel->fdw_private;
    initStringInfo(&sql);
    List *deparse_tlist = duckdb_build_tlist_to_deparse(baserel);
    duckdb_deparse_select_stmt_for_rel(&sql, root, baserel, deparse_tlist, fpinfo->remote_conds, NIL, false, false, false, &retrieved_attrs, &params_list);
    fdw_private = list_make4(makeString(sql.data), retrieved_attrs, makeInteger(foreigntableid), makeInteger(fpinfo->server->serverid));
    return make_foreignscan(tlist, extract_actual_clauses(fpinfo->local_conds, false), baserel->relid, NIL, fdw_private, NIL, NIL, outer_plan);
}

static void duckdbBeginForeignScan(ForeignScanState *node, int eflags) {
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    node->fdw_state = (void *)festate;
    festate->tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
    Oid serverid = intVal(list_nth(fsplan->fdw_private, 3));
    festate->conn = duckdb_get_connection(GetForeignServer(serverid), false);
    festate->query = strVal(list_nth(fsplan->fdw_private, 0));
    festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
    if (duckdb_query(festate->conn, festate->query, &festate->res) == DuckDBError) elog(ERROR, "duckdb_fdw: query failed: %s", duckdb_result_error(&festate->res));
    festate->current_chunk_row_idx = 0;
    festate->current_chunk_row_count = duckdb_row_count(&festate->res);
}

static Datum duckdb_value_to_pg(DuckDBFdwExecState *festate, int col_idx, uint64_t global_row, Oid pgtype) {
    if (duckdb_value_is_null(&festate->res, col_idx, global_row)) return (Datum)0;
    switch (pgtype) {
        case BOOLOID: return BoolGetDatum(duckdb_value_boolean(&festate->res, col_idx, global_row));
        case INT4OID: return Int32GetDatum(duckdb_value_int32(&festate->res, col_idx, global_row));
        case INT8OID: return Int64GetDatum(duckdb_value_int64(&festate->res, col_idx, global_row));
        case FLOAT8OID: return Float8GetDatum(duckdb_value_double(&festate->res, col_idx, global_row));
        case DATEOID: return Int32GetDatum(duckdb_value_date(&festate->res, col_idx, global_row).days - 10957);
        default: {
            char *s = duckdb_value_varchar(&festate->res, col_idx, global_row);
            if (s && s[0] == '[' && s[strlen(s)-1] == ']') {
                char *p = s; while (*p) { if (*p == '[') *p = '{'; else if (*p == ']') *p = '}'; p++; }
            }
            Datum res;
            if (pgtype == TEXTOID || pgtype == VARCHAROID) res = PointerGetDatum(cstring_to_text(s));
            else {
                Oid typinput, typioparam; getTypeInputInfo(pgtype, &typinput, &typioparam);
                res = OidInputFunctionCall(typinput, s, typioparam, -1);
            }
            duckdb_free(s); return res;
        }
    }
}

static TupleTableSlot * duckdbIterateForeignScan(ForeignScanState *node) {
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);
    if (festate->current_chunk_row_idx >= festate->current_chunk_row_count) return slot;
    ListCell *lc; int i = 0;
    foreach(lc, festate->retrieved_attrs) {
        int attnum = lfirst_int(lc);
        if (attnum > 0) {
            slot->tts_values[attnum-1] = duckdb_value_to_pg(festate, i, festate->current_chunk_row_idx, festate->tupdesc->attrs[attnum-1].atttypid);
            slot->tts_isnull[attnum-1] = false;
        }
        i++;
    }
    ExecStoreVirtualTuple(slot);
    festate->current_chunk_row_idx++;
    return slot;
}

static void duckdbEndForeignScan(ForeignScanState *node) {
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    if (festate) duckdb_destroy_result(&festate->res);
}

static void duckdbReScanForeignScan(ForeignScanState *node) { duckdbEndForeignScan(node); duckdbBeginForeignScan(node, 0); }

/* Write Support - SQL based for stability */
static void duckdbBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags) {
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
    Relation rel = resultRelInfo->ri_RelationDesc;
    duckdb_opt *options = duckdb_get_options(RelationGetRelid(rel));
    festate->conn = duckdb_get_connection(GetForeignServer(GetForeignTable(RelationGetRelid(rel))->serverid), false);
    festate->table_name = options->svr_table;
    festate->tupdesc = RelationGetDescr(rel);
    resultRelInfo->ri_FdwState = (void *)festate;
}

static TupleTableSlot * duckdbExecForeignInsert(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot) {
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)resultRelInfo->ri_FdwState;
    StringInfoData sql; initStringInfo(&sql);
    appendStringInfo(&sql, "INSERT INTO %s VALUES (", festate->table_name);
    for (int i = 0; i < festate->tupdesc->natts; i++) {
        bool isnull; Datum val = slot_getattr(slot, i + 1, &isnull);
        if (i > 0) appendStringInfoString(&sql, ", ");
        if (isnull) appendStringInfoString(&sql, "NULL");
        else {
            Oid typoutput; bool typIsVarlena; getTypeOutputInfo(festate->tupdesc->attrs[i].atttypid, &typoutput, &typIsVarlena);
            char *s = OidOutputFunctionCall(typoutput, val);
            if (festate->tupdesc->attrs[i].atttypid <= 701) appendStringInfoString(&sql, s); /* Numeric */
            else appendStringInfo(&sql, "'%s'", s);
            pfree(s);
        }
    }
    appendStringInfoString(&sql, ");");
    duckdb_result res;
    if (duckdb_query(festate->conn, sql.data, &res) == DuckDBError) elog(ERROR, "DuckDB insert failed: %s", duckdb_result_error(&res));
    duckdb_destroy_result(&res); pfree(sql.data);
    return slot;
}

static TupleTableSlot * duckdbExecForeignUpdate(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot) { elog(ERROR, "UPDATE not supported"); return slot; }
static TupleTableSlot * duckdbExecForeignDelete(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot) { elog(ERROR, "DELETE not supported"); return slot; }
static void duckdbEndForeignModify(EState *executor, ResultRelInfo *resultRelInfo) {}

/* Handler */
PG_FUNCTION_INFO_V1(duckdb_fdw_handler);
Datum duckdb_fdw_handler(PG_FUNCTION_ARGS) {
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);
    fdwroutine->GetForeignRelSize = duckdbGetForeignRelSize;
    fdwroutine->GetForeignPaths = duckdbGetForeignPaths;
    fdwroutine->GetForeignPlan = duckdbGetForeignPlan;
    fdwroutine->BeginForeignScan = duckdbBeginForeignScan;
    fdwroutine->IterateForeignScan = duckdbIterateForeignScan;
    fdwroutine->ReScanForeignScan = duckdbReScanForeignScan;
    fdwroutine->EndForeignScan = duckdbEndForeignScan;
    fdwroutine->ImportForeignSchema = duckdb_import_foreign_schema;
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
    StringInfoData sql; initStringInfo(&sql);
    appendStringInfo(&sql, "CREATE OR REPLACE SECRET %s ( TYPE S3, KEY_ID '%s', SECRET '%s'", secret_name, key_id, secret);
    if (region) appendStringInfo(&sql, ", REGION '%s'", region);
    appendStringInfoString(&sql, " );");
    duckdb_do_sql_command(conn, sql.data, ERROR);
    PG_RETURN_VOID();
}

/* Missing symbols */
Expr * duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel) { return NULL; }
Expr * duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel) { return NULL; }
void _PG_init(void) {}
void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, void *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost) {}