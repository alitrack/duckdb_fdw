#include "postgres.h"
#include "duckdb_fdw.h"
#include "access/xact.h"
#include "executor/spi.h"
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
#include "utils/guc.h"
#include "utils/json.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "catalog/pg_user_mapping.h"
#include "miscadmin.h"
#include "executor/executor.h"
#include "commands/explain.h"
#include "nodes/nodeFuncs.h"

PG_MODULE_MAGIC;

#define DUCKDB_EPOCH_DIFF_DAYS 10957
#define DUCKDB_EPOCH_DIFF_MICROS INT64CONST(946684800000000)

bool duckdb_fdw_allow_unsupported_pg_duckdb_coexistence = false;

static void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel,
										   List *param_join_conds, List *pathkeys,
										   void *fpextra, double *p_rows, int *p_width,
										   Cost *p_startup_cost, Cost *p_total_cost);
static bool duckdb_fdw_check_unsupported_pg_duckdb_coexistence(bool *newval,
															   void **extra,
															   GucSource source);
static void duckdb_jsonb_append_separator(StringInfo buf, bool *first_field);
static void duckdb_jsonb_append_string_field(StringInfo buf, const char *key,
											 const char *value, bool *first_field);
static void duckdb_jsonb_append_bool_field(StringInfo buf, const char *key,
										   bool value, bool *first_field);
static void duckdb_fdw_preflight_probe(bool *installed_in_database,
									   bool *available_in_instance,
									   bool *catalog_lookup_ok);

static char *
duckdb_build_relation_reference(const char *table_name)
{
	if (!table_name)
		return pstrdup("\"\"");

	if (!duckdb_fdw_is_safe_sql_fragment(table_name))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("unsafe table option value")));

	if (strstr(table_name, ".parquet") != NULL &&
		strstr(table_name, "read_parquet") == NULL)
	{
		char *lit = duckdb_fdw_quote_literal(table_name);
		char *expr = psprintf("read_parquet(%s)", lit);
		pfree(lit);
		return expr;
	}

	if (strstr(table_name, "read_parquet") != NULL ||
		strstr(table_name, "read_csv") != NULL ||
		strchr(table_name, '(') != NULL)
	{
		return pstrdup(table_name);
	}

	if (strchr(table_name, '.') != NULL || strchr(table_name, '"') != NULL)
		return pstrdup(table_name);

	return duckdb_fdw_quote_identifier(table_name);
}

static void
duckdb_bind_parameter(duckdb_prepared_statement stmt, idx_t param_idx, Oid typid, Datum val, bool isnull)
{
	Oid			typoutput;
	bool		typisvarlena;
	char	   *outstr;

	if (isnull)
	{
		if (duckdb_bind_null(stmt, param_idx) == DuckDBError)
			elog(ERROR, "duckdb_fdw: failed to bind NULL at parameter %zu", (size_t) param_idx);
		return;
	}

	switch (typid)
	{
		case BOOLOID:
			if (duckdb_bind_boolean(stmt, param_idx, DatumGetBool(val)) == DuckDBError)
				elog(ERROR, "duckdb_fdw: bind bool failed at parameter %zu", (size_t) param_idx);
			return;
		case INT2OID:
			if (duckdb_bind_int16(stmt, param_idx, DatumGetInt16(val)) == DuckDBError)
				elog(ERROR, "duckdb_fdw: bind int2 failed at parameter %zu", (size_t) param_idx);
			return;
		case INT4OID:
			if (duckdb_bind_int32(stmt, param_idx, DatumGetInt32(val)) == DuckDBError)
				elog(ERROR, "duckdb_fdw: bind int4 failed at parameter %zu", (size_t) param_idx);
			return;
		case INT8OID:
			if (duckdb_bind_int64(stmt, param_idx, DatumGetInt64(val)) == DuckDBError)
				elog(ERROR, "duckdb_fdw: bind int8 failed at parameter %zu", (size_t) param_idx);
			return;
		case FLOAT4OID:
			if (duckdb_bind_float(stmt, param_idx, DatumGetFloat4(val)) == DuckDBError)
				elog(ERROR, "duckdb_fdw: bind float4 failed at parameter %zu", (size_t) param_idx);
			return;
		case FLOAT8OID:
			if (duckdb_bind_double(stmt, param_idx, DatumGetFloat8(val)) == DuckDBError)
				elog(ERROR, "duckdb_fdw: bind float8 failed at parameter %zu", (size_t) param_idx);
			return;
		case DATEOID:
			{
				duckdb_date date = {DatumGetDateADT(val) + DUCKDB_EPOCH_DIFF_DAYS};
				if (duckdb_bind_date(stmt, param_idx, date) == DuckDBError)
					elog(ERROR, "duckdb_fdw: bind date failed at parameter %zu", (size_t) param_idx);
				return;
			}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				duckdb_timestamp ts;
				ts.micros = DatumGetInt64(val) + DUCKDB_EPOCH_DIFF_MICROS;
				if (duckdb_bind_timestamp(stmt, param_idx, ts) == DuckDBError)
					elog(ERROR, "duckdb_fdw: bind timestamp failed at parameter %zu", (size_t) param_idx);
				return;
			}
		default:
			getTypeOutputInfo(typid, &typoutput, &typisvarlena);
			outstr = OidOutputFunctionCall(typoutput, val);
			if (duckdb_bind_varchar(stmt, param_idx, outstr) == DuckDBError)
			{
				pfree(outstr);
				elog(ERROR, "duckdb_fdw: bind text fallback failed at parameter %zu", (size_t) param_idx);
			}
			pfree(outstr);
			return;
	}
}

static void
duckdb_execute_query(DuckDBFdwExecState *festate, ForeignScanState *node, ForeignScan *fsplan)
{
	if (fsplan->fdw_exprs != NIL)
	{
		ListCell   *lc_expr;
		ListCell   *lc_state;
		idx_t		param_idx = 1;

		festate->param_exprs = fsplan->fdw_exprs;
		festate->param_expr_states = ExecInitExprList(fsplan->fdw_exprs, &node->ss.ps);

		if (duckdb_prepare(festate->conn, festate->query, &festate->prepared_stmt) == DuckDBError)
		{
			const char *err = festate->prepared_stmt ? duckdb_prepare_error(festate->prepared_stmt) : "prepare error";
			char *err_msg = pstrdup(err ? err : "prepare error");
			if (festate->prepared_stmt)
				duckdb_destroy_prepare(&festate->prepared_stmt);
			elog(ERROR, "duckdb_fdw: prepare failed: %s", err_msg);
		}

		festate->use_prepared_stmt = true;

		forboth(lc_state, festate->param_expr_states, lc_expr, festate->param_exprs)
		{
			ExprState  *estate = lfirst(lc_state);
			Expr	   *expr = lfirst(lc_expr);
			bool		isnull = false;
			Datum		val;
			Oid			typid;

			val = ExecEvalExpr(estate, node->ss.ps.ps_ExprContext, &isnull);
			typid = exprType((Node *) expr);
			duckdb_bind_parameter(festate->prepared_stmt, param_idx, typid, val, isnull);
			param_idx++;
		}

		if (duckdb_execute_prepared(festate->prepared_stmt, &festate->res) == DuckDBError)
			elog(ERROR, "duckdb_fdw: execute prepared failed");
	}
	else
	{
		if (duckdb_query(festate->conn, festate->query, &festate->res) == DuckDBError)
			elog(ERROR, "duckdb_fdw: query failed: %s", duckdb_result_error(&festate->res));
		festate->use_prepared_stmt = false;
	}
}

static bool
duckdb_fetch_next_chunk(DuckDBFdwExecState *festate)
{
	if (festate->current_chunk)
		duckdb_destroy_data_chunk(&festate->current_chunk);

	festate->current_chunk = duckdb_result_get_chunk(festate->res, festate->current_chunk_idx++);
	festate->current_chunk_row_idx = 0;
	if (!festate->current_chunk)
	{
		festate->current_chunk_row_count = 0;
		return false;
	}

	festate->current_chunk_row_count = duckdb_data_chunk_get_size(festate->current_chunk);
	return festate->current_chunk_row_count > 0;
}

static bool
duckdb_can_use_chunk_scan(TupleDesc tupdesc, List *retrieved_attrs)
{
	ListCell   *lc;

	if (tupdesc == NULL || retrieved_attrs == NIL)
		return false;

	foreach(lc, retrieved_attrs)
	{
		int			attnum_pg = lfirst_int(lc);
		Oid			pgtype;

		if (attnum_pg <= 0 || attnum_pg > tupdesc->natts)
			return false;

		pgtype = TupleDescAttr(tupdesc, attnum_pg - 1)->atttypid;
		switch (pgtype)
		{
			case BOOLOID:
			case INT2OID:
			case INT4OID:
			case INT8OID:
			case FLOAT4OID:
			case FLOAT8OID:
			case DATEOID:
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
				break;
			default:
				return false;
		}
	}

	return true;
}

static bool
duckdb_append_slot_row(DuckDBFdwExecState *festate, TupleTableSlot *slot)
{
	int i;

	for (i = 0; i < festate->tupdesc->natts; i++)
	{
		bool		isnull;
		Datum		val = slot_getattr(slot, i + 1, &isnull);
		Oid			typ = TupleDescAttr(festate->tupdesc, i)->atttypid;
		duckdb_state state = DuckDBSuccess;

		if (isnull)
			state = duckdb_append_null(festate->appender);
		else
		{
			switch (typ)
			{
				case BOOLOID:
					state = duckdb_append_bool(festate->appender, DatumGetBool(val));
					break;
				case INT2OID:
					state = duckdb_append_int16(festate->appender, DatumGetInt16(val));
					break;
				case INT4OID:
					state = duckdb_append_int32(festate->appender, DatumGetInt32(val));
					break;
				case INT8OID:
					state = duckdb_append_int64(festate->appender, DatumGetInt64(val));
					break;
				case FLOAT4OID:
					state = duckdb_append_float(festate->appender, DatumGetFloat4(val));
					break;
				case FLOAT8OID:
					state = duckdb_append_double(festate->appender, DatumGetFloat8(val));
					break;
				case DATEOID:
					{
						duckdb_date date = {DatumGetDateADT(val) + DUCKDB_EPOCH_DIFF_DAYS};
						state = duckdb_append_date(festate->appender, date);
					}
					break;
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					{
						duckdb_timestamp ts;
						ts.micros = DatumGetInt64(val) + DUCKDB_EPOCH_DIFF_MICROS;
						state = duckdb_append_timestamp(festate->appender, ts);
					}
					break;
				default:
					{
						Oid typoutput;
						bool typisvarlena;
						char *outstr;

						getTypeOutputInfo(typ, &typoutput, &typisvarlena);
						outstr = OidOutputFunctionCall(typoutput, val);
						state = duckdb_append_varchar(festate->appender, outstr);
						pfree(outstr);
					}
					break;
			}
		}

		if (state == DuckDBError)
			return false;
	}

	return duckdb_appender_end_row(festate->appender) != DuckDBError;
}

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
        DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) joinrel->fdw_private;
        double      rows;
        Cost        startup_cost;
        Cost        total_cost;

        duckdb_estimate_path_cost_size(root, joinrel, fpinfo->joinclauses, NIL, NULL,
                                       &rows, NULL, &startup_cost, &total_cost);
        add_path(joinrel, (Path *)
                 create_foreignscan_path(root, joinrel,
                                          joinrel->reltarget,
                                          rows,
                                          startup_cost,
                                          total_cost,
                                          NIL,
                                          joinrel->lateral_relids,
                                          NULL,
                                          NIL,
                                          NIL));
    }
}

static void
duckdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    DuckDBFdwRelationInfo *fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
    duckdb_opt  *options;
    baserel->fdw_private = (void *) fpinfo;
    fpinfo->foreigntableid = foreigntableid;
    fpinfo->table = GetForeignTable(foreigntableid);
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);
    options = duckdb_get_options(foreigntableid);

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

    if (options && options->use_remote_estimate && options->svr_table)
    {
        duckdb_connection conn = duckdb_get_connection(fpinfo->server, false);
        duckdb_result count_res;
        StringInfoData count_sql;
        char *relation_ref = duckdb_build_relation_reference(options->svr_table);
        bool query_ok = false;

        MemSet(&count_res, 0, sizeof(count_res));
        initStringInfo(&count_sql);
        appendStringInfo(&count_sql, "SELECT COUNT(*) FROM %s", relation_ref);
        if (duckdb_query(conn, count_sql.data, &count_res) == DuckDBSuccess)
            query_ok = true;
        if (query_ok &&
            duckdb_row_count(&count_res) > 0)
        {
            int64_t count_rows = duckdb_value_int64(&count_res, 0, 0);
            if (count_rows > 0)
                baserel->rows = (double) count_rows;
        }
        duckdb_destroy_result(&count_res);
        pfree(relation_ref);
        pfree(count_sql.data);
    }

    fpinfo->rows = baserel->rows;
    fpinfo->width = baserel->reltarget->width;
}

static void
duckdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    double      rows;
    Cost        startup_cost;
    Cost        total_cost;

    duckdb_estimate_path_cost_size(root, baserel, NIL, NIL, NULL,
                                   &rows, NULL, &startup_cost, &total_cost);
    add_path(baserel, (Path *)
             create_foreignscan_path(root, baserel,
                                     baserel->reltarget,
                                     rows,
                                     startup_cost,
                                     total_cost,
                                     NIL,   /* no pathkeys */
                                     NULL,  /* no required_outer */
                                     NULL,  /* no fdw_outerpath */
                                     NIL,   /* no fdw_restrictinfo */
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

    return make_foreignscan(tlist, extract_actual_clauses(fpinfo->local_conds, false), scanrelid, params_list, fdw_private, (IS_UPPER_REL(baserel) || IS_JOIN_REL(baserel) ? tlist : NIL), NIL, outer_plan);
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
		Oid serverid = intVal(list_nth(fsplan->fdw_private, 3));
		if (node->ss.ss_ScanTupleSlot)
			festate->tupdesc = node->ss.ss_ScanTupleSlot->tts_tupleDescriptor;
		festate->conn = duckdb_get_connection(GetForeignServer(serverid), false);
	}

	festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));

	if (list_length(fsplan->fdw_private) > 1)
		festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
	else
		festate->retrieved_attrs = NIL;

	if (node->ss.ps.ps_ExprContext == NULL)
		ExecAssignExprContext(node->ss.ps.state, &node->ss.ps);

	duckdb_execute_query(festate, node, fsplan);

	festate->current_chunk_idx = 0;
	festate->current_chunk_row_idx = 0;
	festate->global_row_idx = 0;
	festate->use_chunk_scan = duckdb_can_use_chunk_scan(festate->tupdesc,
														 festate->retrieved_attrs);
	if (festate->use_chunk_scan)
		festate->use_chunk_scan = duckdb_fetch_next_chunk(festate);
	if (!festate->use_chunk_scan)
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
	            return Int32GetDatum(duckdb_value_date(&festate->res, col_idx, global_row).days - DUCKDB_EPOCH_DIFF_DAYS);
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

	    if (festate->use_chunk_scan)
		{
			while (festate->current_chunk_row_idx >= festate->current_chunk_row_count)
			{
				if (!duckdb_fetch_next_chunk(festate))
					return slot;
			}
		}
		else if (festate->current_chunk_row_idx >= festate->current_chunk_row_count)
		{
			return slot;
		}

    if (festate->tupdesc == NULL)
        festate->tupdesc = slot->tts_tupleDescriptor;

	    i = 0;
	    foreach(lc, festate->retrieved_attrs)
	    {
	        int attnum_pg = lfirst_int(lc);
	        if (attnum_pg > 0 && attnum_pg <= slot->tts_tupleDescriptor->natts) {
	            int attnum_idx = attnum_pg - 1;
	            Oid pgtype = TupleDescAttr(festate->tupdesc, attnum_idx)->atttypid;
				bool	isnull = false;
				Datum	dvalue = (Datum) 0;

				if (festate->use_chunk_scan && festate->current_chunk)
				{
					duckdb_vector vector = duckdb_data_chunk_get_vector(festate->current_chunk, i);
					uint64_t *validity = duckdb_vector_get_validity(vector);
					void *data = duckdb_vector_get_data(vector);
					idx_t row = festate->current_chunk_row_idx;

					if (validity && !duckdb_validity_row_is_valid(validity, row))
						isnull = true;
					else
					{
						switch (pgtype)
						{
							case BOOLOID:
								dvalue = BoolGetDatum(((bool *) data)[row]);
								break;
							case INT2OID:
								dvalue = Int16GetDatum(((int16_t *) data)[row]);
								break;
							case INT4OID:
								dvalue = Int32GetDatum(((int32_t *) data)[row]);
								break;
							case INT8OID:
								dvalue = Int64GetDatum(((int64_t *) data)[row]);
								break;
							case FLOAT4OID:
								dvalue = Float4GetDatum(((float *) data)[row]);
								break;
							case FLOAT8OID:
								dvalue = Float8GetDatum(((double *) data)[row]);
								break;
							case DATEOID:
								dvalue = Int32GetDatum(((duckdb_date *) data)[row].days - DUCKDB_EPOCH_DIFF_DAYS);
								break;
							case TIMESTAMPOID:
							case TIMESTAMPTZOID:
								dvalue = Int64GetDatum(((duckdb_timestamp *) data)[row].micros - DUCKDB_EPOCH_DIFF_MICROS);
								break;
							default:
								dvalue = duckdb_value_to_pg(festate, i, festate->global_row_idx, pgtype);
								break;
						}
					}
				}
				else if (duckdb_value_is_null(&festate->res, i, festate->current_chunk_row_idx))
				{
					isnull = true;
				}
				else
				{
					dvalue = duckdb_value_to_pg(festate, i, festate->current_chunk_row_idx, pgtype);
				}

				slot->tts_isnull[attnum_idx] = isnull;
				slot->tts_values[attnum_idx] = isnull ? (Datum) 0 : dvalue;
	        }
	        i++;
	    }

	    ExecStoreVirtualTuple(slot);
	    festate->current_chunk_row_idx++;
		festate->global_row_idx++;
	    return slot;
}

static void
duckdbEndForeignScan(ForeignScanState *node)
{
	    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
	    if (festate)
	    {
			if (festate->current_chunk)
				duckdb_destroy_data_chunk(&festate->current_chunk);
	        duckdb_destroy_result(&festate->res);
			if (festate->use_prepared_stmt && festate->prepared_stmt)
				duckdb_destroy_prepare(&festate->prepared_stmt);
	    }
}

static void
duckdbReScanForeignScan(ForeignScanState *node)
{
	DuckDBFdwExecState *oldstate = (DuckDBFdwExecState *)node->fdw_state;
	duckdbEndForeignScan(node);
	duckdbBeginForeignScan(node, 0);
	pfree(oldstate);
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
        double rows;
        Cost startup_cost;
        Cost total_cost;

        fpinfo->pushdown_safe = true;
        duckdb_estimate_path_cost_size(root, output_rel, NIL, NIL, NULL,
                                       &rows, NULL, &startup_cost, &total_cost);
        add_path(output_rel, (Path *)
                 create_foreignscan_path(root, output_rel,
                                          output_rel->reltarget,
                                          rows,
                                          startup_cost,
                                          total_cost,
                                          NIL,
                                          NULL,
                                          NULL,
                                          NIL,
                                          NIL));
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

static void
duckdbBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, List *fdw_private, int subplan_index, int eflags)
{
	    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
	    Relation rel = resultRelInfo->ri_RelationDesc;
	    duckdb_opt *options = duckdb_get_options(RelationGetRelid(rel));
		duckdb_state state;
	    festate->conn = duckdb_get_connection(GetForeignServer(GetForeignTable(RelationGetRelid(rel))->serverid), false);
	    festate->table_name = options->svr_table;
	    festate->tupdesc = RelationGetDescr(rel);
		festate->use_appender = false;
		state = duckdb_appender_create(festate->conn, NULL, festate->table_name, &festate->appender);
		if (state == DuckDBSuccess)
			festate->use_appender = true;
	    resultRelInfo->ri_FdwState = (void *)festate;
}

static TupleTableSlot *
duckdbExecForeignInsert(EState *executor, ResultRelInfo *resultRelInfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)resultRelInfo->ri_FdwState;
		if (festate->use_appender)
		{
			if (!duckdb_append_slot_row(festate, slot))
			{
				const char *err = duckdb_appender_error(festate->appender);
				elog(ERROR, "DuckDB appender insert failed: %s", err ? err : "unknown appender error");
			}
			return slot;
		}

		/* Legacy fallback path */
		{
			StringInfoData sql;
			int i;
			char *relref = duckdb_build_relation_reference(festate->table_name);

			initStringInfo(&sql);
			appendStringInfo(&sql, "INSERT INTO %s VALUES (", relref);
			pfree(relref);

			for (i = 0; i < festate->tupdesc->natts; i++)
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
						char *lit = duckdb_fdw_quote_literal(s);
						appendStringInfoString(&sql, lit);
						pfree(lit);
					}
					pfree(s);
				}
			}
			appendStringInfoString(&sql, ");");

			{
				duckdb_result res;
				if (duckdb_query(festate->conn, sql.data, &res) == DuckDBError)
					elog(ERROR, "DuckDB insert failed: %s", duckdb_result_error(&res));
				duckdb_destroy_result(&res);
			}
			pfree(sql.data);
		}

	    return slot;
}

static TupleTableSlot **
duckdbExecForeignBatchInsert(EState *estate,
							 ResultRelInfo *rinfo,
							 TupleTableSlot **slots,
							 TupleTableSlot **planSlots,
							 int *numSlots)
{
	int i;

	for (i = 0; i < *numSlots; i++)
		duckdbExecForeignInsert(estate, rinfo, slots[i], planSlots ? planSlots[i] : NULL);

	return slots;
}

static int
duckdbGetForeignModifyBatchSize(ResultRelInfo *rinfo)
{
	DuckDBFdwExecState *festate = (DuckDBFdwExecState *) rinfo->ri_FdwState;

	if (festate && festate->use_appender)
		return 2048;
	return 1;
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
	DuckDBFdwExecState *festate = (DuckDBFdwExecState *) resultRelInfo->ri_FdwState;

	if (!festate)
		return;
	if (festate->use_appender && festate->appender)
	{
		duckdb_appender_close(festate->appender);
		duckdb_appender_destroy(&festate->appender);
	}
}

static void
duckdbExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    List       *fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;
    char       *sql = strVal(list_nth(fdw_private, 0));

    ExplainPropertyText("Remote SQL", sql, es);
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
    fdwroutine->ExplainForeignScan = duckdbExplainForeignScan;

    /* Write Support */
    fdwroutine->AddForeignUpdateTargets = duckdbAddForeignUpdateTargets;
	    fdwroutine->PlanForeignModify = duckdbPlanForeignModify;
	    fdwroutine->BeginForeignModify = duckdbBeginForeignModify;
	    fdwroutine->ExecForeignInsert = duckdbExecForeignInsert;
		fdwroutine->ExecForeignBatchInsert = duckdbExecForeignBatchInsert;
		fdwroutine->GetForeignModifyBatchSize = duckdbGetForeignModifyBatchSize;
	    fdwroutine->ExecForeignUpdate = duckdbExecForeignUpdate;
	    fdwroutine->ExecForeignDelete = duckdbExecForeignDelete;
	    fdwroutine->EndForeignModify = duckdbEndForeignModify;

    PG_RETURN_POINTER(fdwroutine);
}

PG_FUNCTION_INFO_V1(duckdb_fdw_version);
Datum
duckdb_fdw_version(PG_FUNCTION_ARGS)
{
	duckdb_runtime_guard_check();
	PG_RETURN_TEXT_P(cstring_to_text(duckdb_library_version()));
}

static void
duckdb_jsonb_append_separator(StringInfo buf, bool *first_field)
{
	if (*first_field)
		*first_field = false;
	else
		appendStringInfoChar(buf, ',');
}

static void
duckdb_jsonb_append_string_field(StringInfo buf, const char *key, const char *value, bool *first_field)
{
	duckdb_jsonb_append_separator(buf, first_field);
	escape_json(buf, key);
	appendStringInfoChar(buf, ':');

	if (value == NULL)
	{
		appendStringInfoString(buf, "null");
		return;
	}

	escape_json(buf, value);
}

static void
duckdb_jsonb_append_bool_field(StringInfo buf, const char *key, bool value, bool *first_field)
{
	duckdb_jsonb_append_separator(buf, first_field);
	escape_json(buf, key);
	appendStringInfoChar(buf, ':');
	appendStringInfoString(buf, value ? "true" : "false");
}

PG_FUNCTION_INFO_V1(duckdb_fdw_runtime_compatibility_status);
Datum
duckdb_fdw_runtime_compatibility_status(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(
		duckdb_runtime_status_name(duckdb_runtime_guard_status())));
}

PG_FUNCTION_INFO_V1(duckdb_fdw_runtime_fingerprint);
Datum
duckdb_fdw_runtime_fingerprint(PG_FUNCTION_ARGS)
{
	DuckDBRuntimeFingerprint fingerprint;
	DuckDBRuntimeCompatibilityStatus status;
	StringInfoData json;
	bool first_field = true;

	status = duckdb_runtime_guard_status();
	duckdb_runtime_guard_fingerprint(&fingerprint);

	initStringInfo(&json);
	appendStringInfoChar(&json, '{');
	duckdb_jsonb_append_string_field(&json, "status",
									 duckdb_runtime_status_name(status),
									 &first_field);
	duckdb_jsonb_append_string_field(&json, "duckdb_version",
									 fingerprint.duckdb_version,
									 &first_field);
	duckdb_jsonb_append_string_field(&json, "module_path",
									 fingerprint.module_path,
									 &first_field);
	duckdb_jsonb_append_string_field(&json, "duckdb_symbol_path",
									 fingerprint.duckdb_symbol_path,
									 &first_field);
	duckdb_jsonb_append_string_field(&json, "peer_module_path",
									 fingerprint.peer_module_path,
									 &first_field);
	duckdb_jsonb_append_string_field(&json, "peer_runtime_path",
									 fingerprint.peer_runtime_path,
									 &first_field);
	duckdb_jsonb_append_bool_field(&json, "peer_loaded",
								   fingerprint.peer_loaded,
								   &first_field);
	duckdb_jsonb_append_bool_field(&json, "source_unproven",
								   fingerprint.source_unproven,
								   &first_field);
	appendStringInfoChar(&json, '}');

	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(json.data)));
}

static void
duckdb_fdw_preflight_probe(bool *installed_in_database, bool *available_in_instance,
						   bool *catalog_lookup_ok)
{
	int spi_rc;
	bool isnull = false;
	HeapTuple tuple;
	TupleDesc tupdesc;

	*installed_in_database = false;
	*available_in_instance = false;
	*catalog_lookup_ok = false;

	spi_rc = SPI_connect();
	if (spi_rc != SPI_OK_CONNECT)
		return;

	spi_rc = SPI_execute(
		"SELECT "
		"EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_duckdb') AS installed_in_database, "
		"EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pg_duckdb') AS available_in_instance",
		true,
		1);
	if (spi_rc != SPI_OK_SELECT || SPI_processed != 1)
	{
		(void) SPI_finish();
		return;
	}

	tuple = SPI_tuptable->vals[0];
	tupdesc = SPI_tuptable->tupdesc;
	*installed_in_database = DatumGetBool(SPI_getbinval(tuple, tupdesc, 1, &isnull));
	*available_in_instance = DatumGetBool(SPI_getbinval(tuple, tupdesc, 2, &isnull));
	*catalog_lookup_ok = true;
	(void) SPI_finish();
}

PG_FUNCTION_INFO_V1(duckdb_fdw_preflight);
Datum
duckdb_fdw_preflight(PG_FUNCTION_ARGS)
{
	bool installed_in_database;
	bool available_in_instance;
	bool catalog_lookup_ok;
	DuckDBRuntimeCompatibilityStatus status;
	StringInfoData json;
	bool first_field = true;

	duckdb_fdw_preflight_probe(&installed_in_database,
							   &available_in_instance,
							   &catalog_lookup_ok);
	status = duckdb_runtime_guard_status();

	if (installed_in_database || available_in_instance)
	{
		ereport(WARNING,
				(errmsg("duckdb_fdw detected pg_duckdb during preflight"),
				 errdetail("Installed in current database: %s. Available in instance: %s.",
						   installed_in_database ? "yes" : "no",
						   available_in_instance ? "yes" : "no"),
				 errhint("duckdb_fdw uses a strict coexistence policy. Install order does not determine runtime compatibility; backend-local runtime validation does.")));
	}

	initStringInfo(&json);
	appendStringInfoChar(&json, '{');
	duckdb_jsonb_append_string_field(&json, "policy",
									 "strict_runtime_guard",
									 &first_field);
	duckdb_jsonb_append_string_field(&json, "runtime_status",
									 duckdb_runtime_status_name(status),
									 &first_field);
	duckdb_jsonb_append_bool_field(&json, "installed_in_database",
								   installed_in_database,
								   &first_field);
	duckdb_jsonb_append_bool_field(&json, "available_in_instance",
								   available_in_instance,
								   &first_field);
	duckdb_jsonb_append_bool_field(&json, "catalog_lookup_ok",
								   catalog_lookup_ok,
								   &first_field);
	appendStringInfoChar(&json, '}');

	PG_RETURN_DATUM(DirectFunctionCall1(jsonb_in, CStringGetDatum(json.data)));
}

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
		char *secret_id;
		char *key_lit;
		char *secret_lit;
	    initStringInfo(&sql);
		if (!duckdb_fdw_is_valid_identifier(secret_name))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid secret name \"%s\"", secret_name)));
		secret_id = duckdb_fdw_quote_identifier(secret_name);
		key_lit = duckdb_fdw_quote_literal(key_id);
		secret_lit = duckdb_fdw_quote_literal(secret);
	    appendStringInfo(&sql, "CREATE OR REPLACE SECRET %s ( TYPE S3, KEY_ID %s, SECRET %s",
	                     secret_id, key_lit, secret_lit);
	    if (region)
		{
			char *region_lit = duckdb_fdw_quote_literal(region);
	        appendStringInfo(&sql, ", REGION %s", region_lit);
			pfree(region_lit);
		}
	    appendStringInfoString(&sql, " );");

	    duckdb_do_sql_command(conn, sql.data, ERROR);
		pfree(secret_id);
		pfree(key_lit);
		pfree(secret_lit);
		pfree(sql.data);

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

static bool
duckdb_fdw_check_unsupported_pg_duckdb_coexistence(bool *newval, void **extra, GucSource source)
{
	(void) extra;

	if (!*newval)
		return true;

	if (source != PGC_S_SESSION)
	{
		GUC_check_errmsg("duckdb_fdw.allow_unsupported_pg_duckdb_coexistence may only be enabled with SET in the current session");
		GUC_check_errdetail("Persistent, placeholder, and startup-time sources are intentionally rejected for this unsupported override.");
		GUC_check_errhint("Load duckdb_fdw in the target backend first, then run SET duckdb_fdw.allow_unsupported_pg_duckdb_coexistence = on.");
		return false;
	}

	if (IsTransactionBlock())
	{
		GUC_check_errmsg("duckdb_fdw.allow_unsupported_pg_duckdb_coexistence may not be enabled inside an explicit transaction block");
		GUC_check_errdetail("The unsupported override is restricted to ordinary session-level SET so it cannot hide inside SET LOCAL or preload transaction state.");
		GUC_check_errhint("Run LOAD duckdb_fdw and SET duckdb_fdw.allow_unsupported_pg_duckdb_coexistence = on as standalone session commands.");
		return false;
	}

	return true;
}

void
_PG_init(void)
{
	DefineCustomBoolVariable(
		"duckdb_fdw.allow_unsupported_pg_duckdb_coexistence",
		"Allow duckdb_fdw to continue in an unsupported backend-local pg_duckdb coexistence state.",
		"Intended only for session-scoped experiments when strict runtime validation rejects the current backend.",
		&duckdb_fdw_allow_unsupported_pg_duckdb_coexistence,
		false,
		PGC_SUSET,
		0,
		duckdb_fdw_check_unsupported_pg_duckdb_coexistence,
		NULL,
		NULL);

	/*
	 * Clear any pre-load placeholder or config-sourced value. The override
	 * must be armed explicitly after duckdb_fdw is loaded into the backend.
	 */
	SetConfigOption("duckdb_fdw.allow_unsupported_pg_duckdb_coexistence",
					"off",
					PGC_SUSET,
					PGC_S_SESSION);
}
static void
duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, void *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost)
{
    DuckDBFdwRelationInfo *fpinfo = foreignrel ? (DuckDBFdwRelationInfo *) foreignrel->fdw_private : NULL;
    double rows = (fpinfo && fpinfo->rows > 0) ? fpinfo->rows : (foreignrel ? foreignrel->rows : 1000.0);
    int width = (fpinfo && fpinfo->width > 0) ? fpinfo->width : (foreignrel ? foreignrel->reltarget->width : 0);
    Cost startup_cost = 10.0;
    Cost total_cost;

    (void) root;
    (void) fpextra;

    if (rows <= 0)
        rows = 1000.0;

    total_cost = startup_cost + rows * (cpu_tuple_cost + cpu_operator_cost);
    if (param_join_conds != NIL)
        total_cost += list_length(param_join_conds) * rows * cpu_operator_cost;
    if (pathkeys != NIL)
        total_cost += rows * cpu_operator_cost;

    if (p_rows)
        *p_rows = rows;
    if (p_width)
        *p_width = width;
    if (p_startup_cost)
        *p_startup_cost = startup_cost;
    if (p_total_cost)
        *p_total_cost = total_cost;
}
