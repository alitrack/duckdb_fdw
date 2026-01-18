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
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "utils/date.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

static void
duckdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *)palloc0(sizeof(DuckDBFdwRelationInfo));
    baserel->fdw_private = (void *)fpinfo;
    fpinfo->foreigntableid = foreigntableid;
    
    /* Initially assume base relations are shippable */
    fpinfo->pushdown_safe = true;
    
    /* Classification of conditions: Remote vs Local */
    duckdb_classify_conditions(root, baserel, baserel->baserestrictinfo,
                                &fpinfo->remote_conds, &fpinfo->local_conds);
                                
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
    else
    {
        /* Base relation scan */
        scanrelid = baserel->relid;
        deparse_tlist = duckdb_build_tlist_to_deparse(baserel);
    }

    duckdb_deparse_select_stmt_for_rel(&sql, root, baserel, deparse_tlist, fpinfo->remote_conds, NIL, false, false, false, &retrieved_attrs, &params_list);

    fdw_private = list_make3(makeString(sql.data), retrieved_attrs, makeInteger(rel_oid));
    
    return make_foreignscan(tlist, extract_actual_clauses(fpinfo->local_conds, false), scanrelid, NIL, fdw_private, (IS_UPPER_REL(baserel) ? tlist : NIL), NIL, outer_plan);
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
    }
    else
    {
        /* Upper relation (Aggregate) */
        festate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
        foreigntableid = intVal(list_nth(fsplan->fdw_private, 2));
    }

    festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);
    table = GetForeignTable(foreigntableid);
    festate->conn = duckdb_get_connection(GetForeignServer(table->serverid), false);
    
    festate->query = strVal(list_nth(fsplan->fdw_private, 0));
    festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
    
    elog(DEBUG1, "duckdb_fdw: BeginForeignScan query: %s", festate->query);

    /* Execute query using Standard Interface */
    if (duckdb_query(festate->conn, festate->query, &festate->res) == DuckDBError)
    {
        const char *err = duckdb_result_error(&festate->res);
        elog(ERROR, "duckdb_fdw: query failed: %s", err ? err : "unknown error");
    }

    /* Retrieve Arrow Options and Schema */
    festate->arrow_options = duckdb_result_get_arrow_options(&festate->res);
    
    idx_t col_count = duckdb_column_count(&festate->res);
    duckdb_logical_type *types = (duckdb_logical_type *) palloc(sizeof(duckdb_logical_type) * col_count);
    const char **names = (const char **) palloc(sizeof(const char *) * col_count);
    
    for (idx_t i = 0; i < col_count; i++)
    {
        types[i] = duckdb_column_logical_type(&festate->res, i);
        names[i] = duckdb_column_name(&festate->res, i);
    }
    
    duckdb_error_data err = duckdb_to_arrow_schema(festate->arrow_options, types, names, col_count, &festate->arrow_schema);
    if (err)
    {
        duckdb_destroy_error_data(&err);
        elog(ERROR, "duckdb_fdw: failed to create arrow schema");
    }
    
    for (idx_t i = 0; i < col_count; i++)
        duckdb_destroy_logical_type(&types[i]);
    pfree(types);
    pfree(names);

    festate->current_chunk_row_idx = 0;
    festate->current_chunk_row_count = 0;
    festate->arrow_initialized = true;
}

/* 
 * Helper: Convert Arrow String to PostgreSQL Datum using its input function.
 */
static Datum
duckdb_arrow_string_to_pg_datum(struct ArrowStringView sview, Oid pgtype, AttInMetadata *attinmeta, int attnum_idx)
{
    char *str = (char *) palloc(sview.size_bytes + 1);
    memcpy(str, sview.data, sview.size_bytes);
    str[sview.size_bytes] = '\0';
    
    Datum res;
    if (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID)
    {
        res = PointerGetDatum(cstring_to_text(str));
    }
    else
    {
        /* Handle DuckDB array format [1, 2] -> {1, 2} */
        if (sview.size_bytes >= 2 && str[0] == '[' && str[sview.size_bytes - 1] == ']')
        {
             str[0] = '{';
             str[sview.size_bytes - 1] = '}';
        }
        
        res = InputFunctionCall(&attinmeta->attinfuncs[attnum_idx],
                               str,
                               attinmeta->attioparams[attnum_idx],
                               attinmeta->atttypmods[attnum_idx]);
    }
    pfree(str);
    return res;
}

static TupleTableSlot *
duckdbIterateForeignScan(ForeignScanState *node)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    int i;
    ListCell *lc;
    Datum *values = slot->tts_values;
    bool *nulls = slot->tts_isnull;

    ExecClearTuple(slot);

    while (festate->current_chunk_row_idx >= festate->current_chunk_row_count)
    {
        if (festate->arrow_array.release)
        {
            ArrowArrayViewReset(&festate->arrow_array_view);
            ArrowArrayRelease(&festate->arrow_array);
        }

        duckdb_data_chunk chunk = duckdb_fetch_chunk(festate->res);
        if (chunk == NULL || duckdb_data_chunk_get_size(chunk) == 0)
        {
            if (chunk) duckdb_destroy_data_chunk(&chunk);
            return slot;
        }

        duckdb_error_data err = duckdb_data_chunk_to_arrow(festate->arrow_options, chunk, &festate->arrow_array);
        if (err)
        {
            duckdb_destroy_error_data(&err);
            duckdb_destroy_data_chunk(&chunk);
            elog(ERROR, "duckdb_fdw: failed to convert chunk to arrow array");
        }
        
        duckdb_destroy_data_chunk(&chunk);

        if (festate->arrow_array.release == NULL || festate->arrow_array.length == 0)
             return slot;

        if (ArrowArrayViewInitFromSchema(&festate->arrow_array_view, &festate->arrow_schema, NULL) != NANOARROW_OK)
            elog(ERROR, "duckdb_fdw: failed to init arrow array view from schema");

        if (ArrowArrayViewSetArray(&festate->arrow_array_view, &festate->arrow_array, NULL) != NANOARROW_OK)
            elog(ERROR, "duckdb_fdw: failed to set arrow array view");

        festate->current_chunk_row_count = festate->arrow_array.length;
        festate->current_chunk_row_idx = 0;
    }

    i = 0;
    foreach(lc, festate->retrieved_attrs)
    {
        int attnum_pg = lfirst_int(lc);
        if (attnum_pg <= 0) continue;

        int attnum_idx = attnum_pg - 1;
        Oid pgtype = festate->tupdesc->attrs[attnum_idx].atttypid;
        struct ArrowArrayView *col_view = festate->arrow_array_view.children[i];

        if (ArrowArrayViewIsNull(col_view, festate->current_chunk_row_idx))
        {
            nulls[attnum_idx] = true;
            values[attnum_idx] = (Datum)0;
        }
        else
        {
            nulls[attnum_idx] = false;
            switch (col_view->storage_type)
            {
                case NANOARROW_TYPE_INT32:
                {
                    int32_t val = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, festate->current_chunk_row_idx);
                    if (pgtype == DATEOID)
                        values[attnum_idx] = DateADTGetDatum(val - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE));
                    else
                        values[attnum_idx] = Int32GetDatum(val);
                    break;
                }
                case NANOARROW_TYPE_INT64:
                {
                    int64_t val = ArrowArrayViewGetIntUnsafe(col_view, festate->current_chunk_row_idx);
                    if (pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID)
                        values[attnum_idx] = TimestampGetDatum(val - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 86400 * 1000000L));
                    else
                        values[attnum_idx] = Int64GetDatum(val);
                    break;
                }
                case NANOARROW_TYPE_BOOL:
                {
                    struct ArrowBufferView data = ArrowArrayViewGetBufferView(col_view, 1);
                    bool val = ArrowBitGet(data.data.as_uint8, festate->current_chunk_row_idx);
                    values[attnum_idx] = BoolGetDatum(val);
                    break;
                }
                case NANOARROW_TYPE_DATE32:
                {
                    int32_t val = (int32_t)ArrowArrayViewGetIntUnsafe(col_view, festate->current_chunk_row_idx);
                    values[attnum_idx] = DateADTGetDatum(val - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE));
                    break;
                }
                case NANOARROW_TYPE_TIMESTAMP:
                {
                    int64_t val = ArrowArrayViewGetIntUnsafe(col_view, festate->current_chunk_row_idx);
                    values[attnum_idx] = TimestampGetDatum(val - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 86400 * 1000000L));
                    break;
                }
                case NANOARROW_TYPE_FLOAT:
                    values[attnum_idx] = Float4GetDatum((float)ArrowArrayViewGetDoubleUnsafe(col_view, festate->current_chunk_row_idx));
                    break;
                case NANOARROW_TYPE_DOUBLE:
                    values[attnum_idx] = Float8GetDatum(ArrowArrayViewGetDoubleUnsafe(col_view, festate->current_chunk_row_idx));
                    break;
                case NANOARROW_TYPE_STRING:
                case NANOARROW_TYPE_LARGE_STRING:
                case NANOARROW_TYPE_STRING_VIEW:
                    values[attnum_idx] = duckdb_arrow_string_to_pg_datum(ArrowArrayViewGetStringUnsafe(col_view, festate->current_chunk_row_idx), pgtype, festate->attinmeta, attnum_idx);
                    break;
                default:
                    elog(ERROR, "duckdb_fdw: unhandled arrow type %d", col_view->storage_type);
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
        if (festate->arrow_initialized)
        {
            if (festate->arrow_array.release)
            {
                ArrowArrayViewReset(&festate->arrow_array_view);
                ArrowArrayRelease(&festate->arrow_array);
            }
            ArrowSchemaRelease(&festate->arrow_schema);
            duckdb_destroy_arrow_options(&festate->arrow_options);
            duckdb_destroy_result(&festate->res);
        }
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
    fdwroutine->GetForeignUpperPaths = duckdbGetForeignUpperPaths;
    fdwroutine->ImportForeignSchema = duckdb_import_foreign_schema;
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
    appendStringInfo(&sql, "CREATE OR REPLACE SECRET %s (TYPE S3, KEY_ID '%s', SECRET '%s'", 
                     quote_identifier(secret_name), key_id, secret);
    if (region) appendStringInfo(&sql, ", REGION '%s'", region);
    appendStringInfoString(&sql, ");");
    duckdb_do_sql_command(conn, sql.data, NOTICE);
    PG_RETURN_VOID();
}

void _PG_init(void) {}
void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, void *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost) {}
int duckdb_set_transmission_modes(void) { return 0; }
void duckdb_reset_transmission_modes(int nestlevel) {}
Expr * duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel) { return NULL; }
Expr * duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel) { return NULL; }
