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
#include "optimizer/paths.h"
#include "catalog/pg_collation.h"
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
static bool duckdb_chunk_types_ok(duckdb_result *res, TupleDesc tupdesc,
								  List *retrieved_attrs);
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
static bool duckdb_pathkeys_all_foreign(PlannerInfo *root, RelOptInfo *rel,
                                        List *pathkeys);
static void duckdb_add_presorted_foreign_paths(PlannerInfo *root,
                                               RelOptInfo *rel);

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

/*
 * duckdb_chunk_types_ok
 *  The fast chunk-scan path in duckdbIterateForeignScan reads each vector
 *  element as a fixed-width physical value sized by the *Postgres* column
 *  type (e.g. ((int64_t *) data)[row]).  That is only correct when the
 *  underlying DuckDB column is stored with exactly that physical width.
 *
 *  DuckDB widens some results beyond their Postgres counterpart: notably
 *  sum(int) / sum(smallint) / sum(tinyint) return HUGEINT (16 bytes), and
 *  an aggregate over a BIGINT expression can as well.  Reading a 16-byte
 *  vector with an 8-byte stride walks the high 64 bits on alternate rows,
 *  silently corrupting roughly half the values (e.g. sum columns coming
 *  back as 0 on odd-indexed rows).
 *
 *  When every retrieved column's DuckDB type is one of the fixed-width
 *  physical types the fast path assumes, it is safe to use; otherwise
 *  (HUGEINT, UBIGINT, DECIMAL, VARCHAR, LIST, ...) fall back to the
 *  type-correct duckdb_value_to_pg() text path.
 */
static bool
duckdb_chunk_types_ok(duckdb_result *res, TupleDesc tupdesc, List *retrieved_attrs)
{
	ListCell   *lc;
	int			i = 0;

	if (res == NULL || tupdesc == NULL || retrieved_attrs == NIL)
		return false;

	foreach(lc, retrieved_attrs)
	{
		int			attnum_pg = lfirst_int(lc);
		Oid			pgtype;
		duckdb_type dt;

		if (attnum_pg <= 0 || attnum_pg > tupdesc->natts)
			return false;

		pgtype = TupleDescAttr(tupdesc, attnum_pg - 1)->atttypid;
		dt = duckdb_column_type(res, i);

		switch (pgtype)
		{
			case BOOLOID:
				if (dt != DUCKDB_TYPE_BOOLEAN)
					return false;
				break;
			case INT2OID:
				if (dt != DUCKDB_TYPE_SMALLINT &&
					dt != DUCKDB_TYPE_TINYINT)
					return false;
				break;
			case INT4OID:
				if (dt != DUCKDB_TYPE_INTEGER &&
					dt != DUCKDB_TYPE_SMALLINT &&
					dt != DUCKDB_TYPE_TINYINT)
					return false;
				break;
			case INT8OID:
				/*
				 * Only a true 8-byte BIGINT is safe.  HUGEINT / UBIGINT /
				 * UINTEGER are wider or unsigned and must not be read with
				 * an 8-byte stride.
				 */
				if (dt != DUCKDB_TYPE_BIGINT)
					return false;
				break;
			case FLOAT4OID:
				if (dt != DUCKDB_TYPE_FLOAT)
					return false;
				break;
			case FLOAT8OID:
				if (dt != DUCKDB_TYPE_DOUBLE)
					return false;
				break;
			case DATEOID:
				if (dt != DUCKDB_TYPE_DATE)
					return false;
				break;
			case TIMESTAMPOID:
				if (dt != DUCKDB_TYPE_TIMESTAMP)
					return false;
				break;
			case TIMESTAMPTZOID:
				if (dt != DUCKDB_TYPE_TIMESTAMP_TZ)
					return false;
				break;
			default:
				return false;
		}
		i++;
	}

	return true;
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

/*
 * 把 PG 数组输出格式({1,2} / {"a","b"}) 转成 DuckDB list 字面量
 * ([1,2] / ['a','b'])。DuckDB 的 list 列不认花括号, 只认方括号;
 * 字符串元素用单引号。已知边界: 元素含引号、多维数组不在覆盖范围
 * (读路径同样只处理一层花括号), 遇到时原样透传, 由 DuckDB 报错。
 */
static char *
duckdb_pg_array_to_duckdb_list(const char *in)
{
	char		*out;
	size_t		len = strlen(in);
	char		*p;
	char		*q;
	bool		bad = false;

	out = palloc(len + 1);
	p = in;
	q = out;
	while (*p)
	{
		if (*p == '{')
			*q++ = '[';
		else if (*p == '}')
			*q++ = ']';
		else if (*p == '"')
		{
			if (q != out && q[-1] != '[' && q[-1] != ',')
				bad = true;
			*q++ = '\'';
		}
		else
			*q++ = *p;
		p++;
	}
	*q = '\0';
	if (bad)
	{
		pfree(out);
		return NULL;
	}
	return out;
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
						if (get_element_type(typ) != InvalidOid)
						{
							/*
							 * 数组列: PG 输出 {..}, DuckDB list 要 [..];
							 * 转换失败(元素含引号/多维)则原样透传让 DuckDB 报错。
							 */
							char *lst = duckdb_pg_array_to_duckdb_list(outstr);

							if (lst)
							{
								state = duckdb_append_varchar(festate->appender, lst);
								pfree(lst);
							}
							else
								state = duckdb_append_varchar(festate->appender, outstr);
						}
						else
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

    /*
     * Only join types the deparser can render (duckdb_get_jointype_name).
     * JOIN_SEMI / JOIN_ANTI etc. would elog(ERROR) mid-deparse; refuse
     * pushdown for them so the planner falls back to a local join.
     */
    if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
        jointype != JOIN_RIGHT && jointype != JOIN_FULL)
    {
        return false;
    }

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
     * Per-table predicates attached to a member baserel (e.g.
     * WHERE lineitem.l_shipdate > ...) are classified into that
     * baserel's own remote_conds/local_conds by duckdbGetForeignRelSize.
     * They are NOT part of joinrel->baserestrictinfo (the planner
     * attaches single-table quals to the base relation, not the join).
     * We must merge them into the join's fpinfo so the deparser emits
     * them in the remote SQL's WHERE clause.  (Without this, the join
     * pushdown silently dropped all per-table predicates and returned
     * the unfiltered join row count.)
     */

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
     * Merge the per-table predicates that were already classified on the
     * member baserels.  The planner attaches single-table quals (e.g.
     * WHERE lineitem.l_shipdate > ...) to each base relation, not to the
     * join, so joinrel->baserestrictinfo does not contain them.  Without
     * this merge the join pushdown drops them silently and returns the
     * unfiltered join (count(*) FROM lineitem, orders WHERE
     * l_orderkey = o_orderkey AND l_shipdate > '1995-01-01' used to return
     * 6001215 rows instead of 3424196).  Vars in these clauses carry
     * baserel varnos that are already part of joinrel->relids, so
     * duckdb_is_foreign_expr_full() above validates them against the join.
     */
    fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
                                       ofpinfo->remote_conds);
    fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
                                       ifpinfo->remote_conds);
    fpinfo->local_conds = list_concat(fpinfo->local_conds,
                                      ofpinfo->local_conds);
    fpinfo->local_conds = list_concat(fpinfo->local_conds,
                                      ifpinfo->local_conds);

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
            {
                pfree(fpinfo);
                return false;
            }
        }

        /*
         * Check if the join clauses are pushable.
         */
        foreach(lc, fpinfo->joinclauses)
        {
            RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
            if (!duckdb_is_foreign_expr_full(root, joinrel, ri->clause, &glob_cxt))
            {
                pfree(fpinfo);
                return false;
            }
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
        ReleaseSysCache(tp);
        fpinfo->user = GetUserMapping(GetUserId(), fpinfo->server->serverid);
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

    /*
     * Also offer a pre-sorted variant when the query's ORDER BY can be
     * produced by DuckDB, so a plain scan doesn't force a local Sort.
     */
    duckdb_add_presorted_foreign_paths(root, baserel);
}

/*
 * duckdb_pathkeys_all_foreign
 *	True if every pathkey can be pushed down to DuckDB.  We require that the
 *	sort follows each column type's default ordering (the type's default
 *	btree collation, using the standard < or > comparator) and that the
 *	equivalent expression is shippable.
 *
 * This mirrors postgres_fdw's is_foreign_pathkey().  Exotic operator classes
 * and collations (e.g. text_pattern_ops, ICU collations) are deliberately not
 * pushed down: DuckDB's collation handling does not guarantee to match
 * Postgres, and falling back to a local Sort is always correct.
 */
static bool
duckdb_pathkeys_all_foreign(PlannerInfo *root, RelOptInfo *rel, List *pathkeys)
{
	ListCell   *lc;

	foreach(lc, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(lc);
		Expr	   *em_expr;

		/*
		 * The comparator must be the type's standard < or > operator.
		 */
		if (pathkey->pk_strategy != BTLessStrategyNumber &&
			pathkey->pk_strategy != BTGreaterStrategyNumber)
			return false;

		/*
		 * Collation safety.  DuckDB has no notion of Postgres collations and
		 * always sorts text by its native (byte / UTF-8) ordering.  That
		 * matches Postgres only for non-collatable types (int, float, date,
		 * ... -- eclass collation is InvalidOid, and for collatable non-C
		 * types the collation-less "default" which is OID 100) and for the C
		 * collation itself.  Any other collation (locale / ICU /
		 * text_pattern_ops) could order rows differently, so do not push the
		 * sort down and let the planner use a local Sort instead.
		 */
		if (pathkey->pk_eclass->ec_collation != InvalidOid &&
			pathkey->pk_eclass->ec_collation != DEFAULT_COLLATION_OID &&
			pathkey->pk_eclass->ec_collation != C_COLLATION_OID)
			return false;

		/*
		 * The EC must contain a shippable expression computed from this
		 * relation, else DuckDB cannot evaluate the sort key.
		 */
		em_expr = duckdb_find_em_expr_for_rel(pathkey->pk_eclass, rel);
		if (em_expr == NULL)
			return false;
		if (!duckdb_is_foreign_expr(root, rel, em_expr))
			return false;
	}
	return true;
}

/*
 * duckdb_add_presorted_foreign_paths
 *	For a base or join relation, add pre-sorted ForeignPaths carrying
 *	root->query_pathkeys (which is root->sort_pathkeys at this planning
 *	stage for a simple SELECT ... ORDER BY), when the sort order is safe to
 *	produce remotely.  This mirrors postgres_fdw's
 *	add_paths_with_pathkeys_for_rel(): ORDER BY is pushed down by giving a
 *	based-path the query's pathkeys -- not by creating an ordered upper
 *	relation path -- so GetForeignPlan keeps treating the path as an ordinary
 *	base/join scan (SELECT list from attrs_used / join target).
 */
static void
duckdb_add_presorted_foreign_paths(PlannerInfo *root, RelOptInfo *rel)
{
	DuckDBFdwRelationInfo *fpinfo;
	List	   *pathkeys;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;

	if (rel->reloptkind != RELOPT_BASEREL && rel->reloptkind != RELOPT_JOINREL)
		return;

	fpinfo = (DuckDBFdwRelationInfo *) rel->fdw_private;
	if (fpinfo == NULL || !fpinfo->pushdown_safe)
		return;

	/*
	 * The planner will only make use of the remote sort if it can push down
	 * all of the query's pathkeys; a prefix is not useful (see
	 * get_useful_pathkeys_for_relation in postgres_fdw.c).
	 */
	pathkeys = root->query_pathkeys;
	if (pathkeys == NIL)
		return;

	/*
	 * A locally-filtered scan cannot be pre-sorted remotely: DuckDB would
	 * sort rows that the local Filter is going to discard anyway (wrong
	 * total rows below the LIMIT if any, and wasted work otherwise).
	 */
	if (fpinfo->local_conds != NIL)
		return;

	if (!duckdb_pathkeys_all_foreign(root, rel, pathkeys))
		return;

	duckdb_estimate_path_cost_size(root, rel, NIL, pathkeys, NULL,
								   &rows, &width, &startup_cost, &total_cost);

	if (IS_SIMPLE_REL(rel))
	{
		add_path(rel, (Path *)
				 create_foreignscan_path(root, rel,
										  NULL, /* target: use reltarget */
										  rows,
										  startup_cost,
										  total_cost,
										  list_copy(pathkeys),
										  rel->lateral_relids,
										  NULL, /* no fdw_outerpath */
										  NIL,  /* no fdw_restrictinfo */
										  NIL)); /* no fdw_private */
	}
	else
	{
		add_path(rel, (Path *)
				 create_foreign_join_path(root, rel,
										   NULL, /* target: use reltarget */
										   rows,
										   startup_cost,
										   total_cost,
										   list_copy(pathkeys),
										   rel->lateral_relids,
										   NULL, /* no fdw_outerpath */
										   fpinfo->joinclauses,
										   NIL)); /* no fdw_private */
	}
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

    {
        /*
         * ORDER BY / LIMIT pushdown (see duckdbGetForeignPaths and
         * duckdbGetForeignUpperPaths) travels on the ForeignPath: the expected
         * sort order is carried in the path's pathkeys, and the final path
         * additionally stashes [has_final_sort, has_limit] in fdw_private.
         * Read them back so the deparser can emit ORDER BY / LIMIT clauses.
         * The plain base-scan path has pathkeys == NIL and no fdw_private, so
         * this is a no-op for existing scans.
         */
        List   *pathkeys = best_path->path.pathkeys;
        bool    has_final_sort = false;
        bool    has_limit = false;

        if (best_path->fdw_private != NULL &&
            list_length(best_path->fdw_private) >= 2)
        {
            has_final_sort = boolVal(list_nth(best_path->fdw_private, 0));
            has_limit = boolVal(list_nth(best_path->fdw_private, 1));
        }

        duckdb_deparse_select_stmt_for_rel(&sql, root, baserel, deparse_tlist,
                                           fpinfo->remote_conds, pathkeys,
                                           has_final_sort, has_limit, false,
                                           &retrieved_attrs, &params_list);
    }

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
	/*
	 * The fast path assumes each vector element has the physical width of
	 * its Postgres type.  DuckDB widens some results (e.g. sum(int) is a
	 * 16-byte HUGEINT), so only trust the fast path when every retrieved
	 * column's DuckDB type actually matches; otherwise fall back to the
	 * type-correct text path.
	 */
	if (festate->use_chunk_scan)
		festate->use_chunk_scan =
			duckdb_chunk_types_ok(&festate->res, festate->tupdesc,
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
    /*
     * Zero the slot so that EVERY attribute is in a consistent state before
     * we fill only the retrieved ones.  ExecClearTuple does not reset
     * tts_values[]/tts_isnull[] for a virtual slot, so non-retrieved columns
     * would otherwise keep a stale (often zero) by-reference Datum while the
     * tts_nvalid = natts below claims them valid.  When the ForeignScan is
     * not the top node (e.g. a local Group/Sort sits on top and materializes
     * the full tuple via heap_form_tuple), that stale pointer is dereferenced
     * as VARSIZE(NULL) and segfaults.  Initializing all columns to NULL makes
     * the slot fully consistent: retrieved columns get real values, the rest
     * are legitimately NULL (no lower plan node needs their value).
     */
    {
        int natts = slot->tts_tupleDescriptor->natts;
        int     a;
        for (a = 0; a < natts; a++)
        {
            slot->tts_values[a] = 0;
            slot->tts_isnull[a] = true;
        }
    }


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
#if PG_VERSION_NUM >= 170000
	    /*
	     * PG 17+ refactored TupleTableSlot internals. Explicitly set
	     * tts_nvalid so the slot system knows all values are populated.
	     */
	    slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
#endif
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

/*
 * duckdb_add_foreign_ordered_paths
 *  Record FDW metadata on the ordered upper relation so that a downstream
 *  FINAL relation (ORDER BY ... LIMIT) can inherit the foreign context and
 *  the planner keeps calling our GetForeignUpperPaths for FINAL.
 *
 * For a base/join input relation no path is created here: the ORDER BY is
 * already pushed down by the pre-sorted base path (carrying
 * root->query_pathkeys).  This mirrors postgres_fdw's
 * add_foreign_ordered_paths() early-return for base/join inputs.
 */
static void
duckdb_add_foreign_ordered_paths(PlannerInfo *root, RelOptInfo *input_rel,
                                 RelOptInfo *ordered_rel)
{
    DuckDBFdwRelationInfo *ifpinfo =
        (DuckDBFdwRelationInfo *) input_rel->fdw_private;
    DuckDBFdwRelationInfo *fpinfo =
        (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));

    if (root->parse->hasTargetSRFs)
    {
        pfree(fpinfo);
        return;
    }

    fpinfo->outerrel = input_rel;
    fpinfo->foreigntableid = ifpinfo->foreigntableid;
    fpinfo->server = ifpinfo->server;
    fpinfo->user = ifpinfo->user;
    fpinfo->stage = UPPERREL_ORDERED;

    /*
     * The ORDER BY is only pushable when every query pathkey is safe to
     * produce remotely (see duckdb_add_presorted_foreign_paths, which
     * creates the pre-sorted base path on exactly this condition).  Record
     * that so the FINAL handler can tell a real remote sort from a local
     * one and not emit an ORDER BY that DuckDB cannot honour.
     */
    fpinfo->pushdown_safe =
        ifpinfo->pushdown_safe &&
        root->query_pathkeys != NIL &&
        duckdb_pathkeys_all_foreign(root, input_rel, root->query_pathkeys);

    ordered_rel->fdw_private = fpinfo;

    /*
     * Base / join input: the ORDER BY is handled by the pre-sorted base
     * path, so nothing more to do (the FINAL handler will unwrap back to
     * the base relation and push the LIMIT on top of it).
     */
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

    if (stage != UPPERREL_GROUP_AGG && stage != UPPERREL_ORDERED &&
        stage != UPPERREL_FINAL)
        return;

    /* Skip duplicate calls. */
    if (output_rel->fdw_private != NULL)
        return;

    if (stage == UPPERREL_ORDERED)
    {
        duckdb_add_foreign_ordered_paths(root, input_rel, output_rel);
        return;
    }

    /*
     * Only the FINAL stage is new; GROUP_AGG keeps the (HAVING-aware) path
     * construction below, unchanged.
     */
    if (stage == UPPERREL_FINAL)
    {
        Query		*parse = root->parse;
        FinalPathExtraData *final_extra = (FinalPathExtraData *) extra;
        DuckDBFdwRelationInfo *ifpinfo =
            (DuckDBFdwRelationInfo *) input_rel->fdw_private;
        List	   *pathkeys = NIL;
        bool		has_final_sort = false;
        double		rows;
        int			width;
        Cost		startup_cost;
        Cost		total_cost;
        List	   *fdw_private;

        /*
         * If the input is the ORDERED upper relation (ORDER BY ... LIMIT),
         * the ORDER BY itself has already been pushed down by the
         * pre-sorted base path.  Unwrap back to the underlying base/join
         * relation and push the LIMIT on top of it, re-marking the sort as
         * a final sort.  Same design as add_foreign_final_paths() in
         * postgres_fdw.c.
         */
        if (input_rel->reloptkind == RELOPT_UPPER_REL &&
            ifpinfo->stage == UPPERREL_ORDERED)
        {
            input_rel = ifpinfo->outerrel;
            ifpinfo = (DuckDBFdwRelationInfo *) input_rel->fdw_private;
            has_final_sort = true;
            pathkeys = list_copy(root->sort_pathkeys);
        }

        /*
         * Currently we only push down LIMIT/OFFSET for a plain (base or
         * join) scan.  A remote "GROUP BY ... LIMIT" (input_rel being a
         * grouping relation) is a separate, less common case that would
         * require re-deriving the aggregate scan's attrs_used; leave it to
         * the planner's local Limit-over-GroupAggregate until needed.
         */
        if (input_rel->reloptkind != RELOPT_BASEREL &&
            input_rel->reloptkind != RELOPT_JOINREL)
            return;

        if (parse->commandType != CMD_SELECT || parse->hasTargetSRFs)
            return;

        /* The LIMIT itself must be required and simple (no WITH TIES). */
        if (!final_extra->limit_needed)
            return;
        if (parse->limitOption == LIMIT_OPTION_WITH_TIES)
            return;

        /*
         * A locally-filtered scan cannot be LIMITed remotely: DuckDB would
         * count rows that the local Filter discards anyway, returning a
         * different (wrong) number of rows.
         */
        if (ifpinfo->local_conds != NIL)
            return;

        /* The LIMIT / OFFSET expressions must be pushable. */
        if (parse->limitCount &&
            !duckdb_is_foreign_expr(root, input_rel, (Expr *) parse->limitCount))
            return;
        if (parse->limitOffset &&
            !duckdb_is_foreign_expr(root, input_rel, (Expr *) parse->limitOffset))
            return;

        /*
         * If the query also has an ORDER BY, the remote scan must produce it
         * in order; that requires every sort pathkey to be safe to ship.
         * When it is, the final path carries the sort pathkeys and the
         * deparser emits ORDER BY; otherwise the planner sorts locally.
         */
        if (root->sort_pathkeys)
        {
            if (!duckdb_pathkeys_all_foreign(root, input_rel,
                                             root->sort_pathkeys))
                return;
            pathkeys = list_copy(root->sort_pathkeys);
            has_final_sort = true;
        }

        /*
         * Build the final relation's fpinfo.  The path's parent is the input
         * (base) relation, not the final rel, so GetForeignPlan deparses it
         * as an ordinary base scan (SELECT list from attrs_used -- which
         * already includes the ORDER BY columns).  See
         * add_foreign_final_paths() in postgres_fdw.c for the same design.
         */
        fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
        fpinfo->outerrel = input_rel;
        fpinfo->foreigntableid = ifpinfo->foreigntableid;
        fpinfo->server = ifpinfo->server;
        fpinfo->user = ifpinfo->user;
        fpinfo->pushdown_safe = true;
        output_rel->fdw_private = fpinfo;

        /*
         * Cost.  The remote engine applies the ORDER BY / LIMIT *during* the
         * scan, so it produces and transfers only the requested window of
         * rows.  A local Limit-over-scan instead pulls the full result set
         * back from DuckDB and only then discards rows, so the remote final
         * path is cheaper on the data-transfer portion.
         *
         * We scale the (total - startup) data portion down to the window
         * fraction (like adjust_limit_rows_costs does for a local Limit) and
         * then apply a small "remote early-termination" discount.  Without
         * the discount the two paths cost exactly the same and the planner's
         * add_path tie-break keeps the local Limit (it is added first, see
         * grouping_planner in planner.c); the discount makes the remote
         * LIMIT win precisely when there is a real transfer benefit, and
         * tie (local Limit kept) when the data portion is negligible.
         */
        duckdb_estimate_path_cost_size(root, input_rel, NIL, pathkeys, NULL,
                                       &rows, &width, &startup_cost, &total_cost);
        {
            double window_frac = 1.0;
            Cost     full_data = total_cost - startup_cost;
            double   new_rows;

            if (final_extra->count_est > 0 && rows > 0)
                window_frac = (double) final_extra->count_est / rows;
            if (window_frac < 0.0)
                window_frac = 0.0;
            if (window_frac > 1.0)
                window_frac = 1.0;

            /* transfer only the window, at a discount for remote early-stop */
            total_cost = startup_cost + full_data * window_frac * 0.75;
            new_rows = final_extra->count_est > 0
                       ? (double) final_extra->count_est
                       : rows * window_frac;
            if (new_rows < 1.0)
                new_rows = 1.0;
            rows = new_rows;
        }

        /*
         * fdw_private for the ForeignPath: [has_final_sort, has_limit].
         * duckdbGetForeignPlan reads these back when it deparses the query.
         */
        fdw_private = list_make2(makeBoolean(has_final_sort),
                                 makeBoolean(final_extra->limit_needed));

        /*
         * create_foreign_upper_path() sets path->parent = input_rel (the
         * base relation) even though the path is added to output_rel (the
         * FINAL upper relation).  GetForeignPlan then deparses it as an
         * ordinary base scan (SELECT list from attrs_used, which already
         * includes the ORDER BY columns).  Same design as
         * add_foreign_final_paths() in postgres_fdw.c.
         */
        add_path(output_rel, (Path *)
                 create_foreign_upper_path(root,
                                           input_rel,
                                           root->upper_targets[UPPERREL_FINAL],
                                           rows,
                                           startup_cost,
                                           total_cost,
                                           pathkeys,
                                           NULL, /* no fdw_outerpath */
                                           NIL,  /* no fdw_restrictinfo */
                                           fdw_private));
        return;
    }

    GroupPathExtraData *grouping_extra = (GroupPathExtraData *) extra;
    DuckDBFdwRelationInfo *ofpinfo =
        (DuckDBFdwRelationInfo *) input_rel->fdw_private;

    /*
     * If the underlying scan has local conditions, they must be applied
     * before the aggregation runs; DuckDB can only evaluate a WHERE over its
     * own scan, so an aggregation on top of a locally-filtered scan cannot
     * be pushed as one remote query.  (Without this guard the remote
     * GROUP BY would silently aggregate unfiltered rows.)
     */
    if (ofpinfo->local_conds != NIL)
        return;

    fpinfo = (DuckDBFdwRelationInfo *) palloc0(sizeof(DuckDBFdwRelationInfo));
    fpinfo->pushdown_safe = false;
    fpinfo->outerrel = input_rel;
    fpinfo->foreigntableid = ofpinfo->foreigntableid;
    fpinfo->server = ofpinfo->server;
    fpinfo->user = ofpinfo->user;
    output_rel->fdw_private = fpinfo;

    if (!duckdb_is_foreign_expr(root, output_rel, (Expr *) output_rel->reltarget->exprs))
        return;

    /*
     * BUG: a GROUP BY whose query carries a subquery (initplan / SubPlan)
     * combined with aggregation pushdown can segfault the backend while
     * executing the remote aggregate (TPC-H Q11 / Q20: a scalar-subquery
     * HAVING plus a multiplication aggregate).  By the time this upper path
     * is built the subquery has usually been turned into an initplan and the
     * HAVING into a reference to its output Param, so checking the HAVING for
     * a SubLink does not see it; the reliable signal is that the query has
     * initplans.  Until the execution layer handles a pushed-down remote
     * aggregate alongside an initplan, keep such aggregations local: PG's
     * (local GroupAggregate over the ForeignScan, the initplan
     * run separately, HAVING over the local aggregate) is correct.  Queries
     * without subqueries (TPC-H Q3/Q10 join aggregates) are unaffected.
     */
    if (root->parse->hasSubLinks)
        return;

    /*
     * Classify the HAVING quals: those evaluable remotely go to
     * fpinfo->remote_conds (deparse emits them as the HAVING clause), the
     * rest to fpinfo->local_conds (re-applied by the local plan).  Without
     * this classification remote_conds stays NULL and the HAVING clause is
     * silently dropped from the remote query, returning unfiltered groups.
     * The core planner does not wrap HAVING quals in RestrictInfos, so we
     * make our own.
     */
    if (grouping_extra->havingQual != NULL)
    {
        ListCell   *lc;

        foreach(lc, (List *) grouping_extra->havingQual)
        {
            Expr       *hexpr = (Expr *) lfirst(lc);
            RestrictInfo *rinfo;

            Assert(!IsA(hexpr, RestrictInfo));
            rinfo = make_restrictinfo(root, hexpr, true, false, false, false,
                                      root->qual_security_level,
                                      output_rel->relids, NULL, NULL);
            if (duckdb_is_foreign_expr(root, output_rel, hexpr))
                fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
            else
                fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
        }
    }

    /*
     * Aggregates referenced by HAVING quals kept local still have to be
     * computed remotely (the group aggregates are not available locally),
     * so every such aggregate must be shippable.
     */
    if (fpinfo->local_conds != NIL)
    {
        List       *aggvars = NIL;
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
            Expr       *expr = (Expr *) lfirst(lc);

            if (IsA(expr, Aggref) &&
                !duckdb_is_foreign_expr(root, output_rel, expr))
                return;
        }
    }

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

static int
duckdbIsForeignRelUpdatable(Relation rel)
{
	/*
	 * duckdb_fdw supports INSERT via the Appender API and the legacy
	 * SQL fallback path. UPDATE and DELETE are not supported — the
	 * planner will skip FDW modify paths for those operations rather
	 * than failing at execution time.
	 *
	 * Servers created with force_readonly reject all DML at plan time.
	 */
	ForeignTable *ftable;

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		ftable = GetForeignTable(rel->rd_id);
		if (duckdb_fdw_server_is_readonly(GetForeignServer(ftable->serverid)))
			return 0;
	}
	return (1 << CMD_INSERT);
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
    /* options->svr_table points into persistent catalog memory — safe to free the wrapper */
    pfree(options);
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

	/*
	 * Disable batching when we have to use RETURNING, there are any
	 * BEFORE/AFTER ROW INSERT triggers on the foreign table, or there are any
	 * WITH CHECK OPTION constraints from parent views.
	 *
	 * The executor projects RETURNING and bumps the command-tag counter only
	 * on the per-row (ri_BatchSize == 1) path.  If we advertise a batch size
	 * greater than 1 while a RETURNING clause is present, the insert is
	 * diverted into the batch-accumulation path in nodeModifyTable.c, where a
	 * single row never reaches the 2048-row flush threshold: ExecForeignInsert
	 * is therefore never called, RETURNING projects no rows and the tag reports
	 * "INSERT 0 0" even though the row is eventually flushed at shutdown.  This
	 * mirrors postgres_fdw's postgresGetForeignModifyBatchSize(), which returns
	 * 1 in the same situations.
	 */
	if (rinfo->ri_projectReturning != NULL ||
		rinfo->ri_WithCheckOptions != NIL ||
		(rinfo->ri_TrigDesc &&
		 (rinfo->ri_TrigDesc->trig_insert_before_row ||
		  rinfo->ri_TrigDesc->trig_insert_after_row)))
		return 1;

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
		duckdb_state close_state = duckdb_appender_close(festate->appender);
		duckdb_appender_destroy(&festate->appender);
		if (close_state == DuckDBError)
			ereport(WARNING,
					(errmsg("duckdb_fdw: appender close reported an error"),
					 errdetail("Some buffered rows may not have been flushed to DuckDB.")));
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
    fdwroutine->IsForeignRelUpdatable = duckdbIsForeignRelUpdatable;
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
    ForeignServer *server = GetForeignServerByName(servername, false);

    /*
     * force_readonly servers reject anything but conservative read-only
     * statements through the generic escape hatch as well.  File-backed
     * databases additionally cannot be written because DuckDB itself
     * opens them READ_ONLY; this gate also covers :memory: databases.
     */
    if (duckdb_fdw_server_is_readonly(server) &&
        !duckdb_fdw_sql_is_readonly(query))
        elog(ERROR, "duckdb_fdw: statement rejected on force_readonly server %s",
             servername);

    /*
     * Run the statement and surface any DuckDB error to the caller.
     * duckdb_do_sql_command() reports at the given elog level; passing ERROR
     * (not LOG) is what makes a failed DDL/DML raise here instead of being
     * logged server-side while the client still sees success.  Without this
     * a mistyped statement (e.g. a CTAS with an explicit column definition,
     * which DuckDB rejects) silently no-ops and the caller cannot tell.
     */
    duckdb_connection conn = duckdb_get_connection(server, false);
    duckdb_do_sql_command(conn, query, ERROR);
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
