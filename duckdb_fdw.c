/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * IDENTIFICATION
 *        duckdb_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "duckdb_fdw.h"

/* Postgres Headers */
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"

PG_MODULE_MAGIC;

/* 
 * 核心回调函数实现 
 */

static void
duckdbGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	DuckDBFdwRelationInfo *fpinfo;
	fpinfo = (DuckDBFdwRelationInfo *)palloc0(sizeof(DuckDBFdwRelationInfo));
	baserel->fdw_private = (void *)fpinfo;
    
    /* 识别查询中用到的列 */
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);
    
    /* 简单估算：默认 1000 行，宽度基于表定义 */
    /* 在 V2.1 中，这里可以执行 EXPLAIN 甚至 COUNT(*) 来获取精确值 */
	baserel->rows = 1000;
    baserel->reltarget->width = 100; // 默认宽度
}

static void
duckdbGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    /* 估算成本：启动成本 10.0，每行成本 0.01 */
    Cost startup_cost = 10.0;
    Cost total_cost = startup_cost + baserel->rows * 0.01;

	/* 添加默认的扫描路径 */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL, /* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL, /* no pathkeys */
									 baserel->lateral_relids,
									 NULL, /* no extra plan */
									 NIL)); /* no fdw_private yet */
}

static ForeignScan *
duckdbGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
					 ForeignPath *best_path, List *tlist, List *scan_clauses,
					 Plan *outer_plan)
{
    /* 
     * 构造查询 SQL 
     * 注意：这里应该调用 deparse.c 的逻辑。
     * 为了简化，目前先演示全表扫描。实际逻辑需对接 deparse.c。
     */
	StringInfoData sql;
    List *fdw_private;
    List *retrieved_attrs = NIL;
    List *params_list = NIL;
    
    /* 获取表名 (deparse.c 会处理更复杂的逻辑) */
    /* 临时简化：假设 deparse 已经工作。这里我们手动构造一个简单的 SQL 用于测试 */
    /* 实际生产代码应调用 duckdb_deparse_select_stmt_for_rel */
    
    initStringInfo(&sql);
    /* 暂时留空，依靠 BeginForeignScan 里的 deparse 调用，或者这里必须生成 */
    /* 为了让它能跑，我们调用 deparse 接口 */
    
    tlist = duckdb_build_tlist_to_deparse(baserel);
    duckdb_deparse_select_stmt_for_rel(&sql, root, baserel, tlist, NIL, NIL, false, false, false, &retrieved_attrs, &params_list);

	/*
	 * Build the fdw_private list for the ForeignScan node.
     * Index 0: SQL string
     * Index 1: Retrieved attributes list
	 */
	fdw_private = list_make2(makeString(sql.data), retrieved_attrs);

	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							NIL,
							fdw_private,
							NIL,
							NIL,
							outer_plan);
}

static void
duckdbBeginForeignScan(ForeignScanState *node, int eflags)
{
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    EState *estate = node->ss.ps.state;
    DuckDBFdwExecState *festate;
    ForeignServer *server;
    ForeignTable *table;
    char *query;
    RangeTblEntry *rte;
    
    /* 1. 初始化状态 */
    festate = (DuckDBFdwExecState *)palloc0(sizeof(DuckDBFdwExecState));
    node->fdw_state = (void *)festate;
    
    /* 2. 获取连接 */
    rte = exec_rt_fetch(fsplan->scan.scanrelid, estate);
    table = GetForeignTable(rte->relid);
    server = GetForeignServer(table->serverid);
    
    festate->conn = duckdb_get_connection(server, false);
    
    /* 3. 获取 SQL 并执行 */
    query = strVal(list_nth(fsplan->fdw_private, 0));
    festate->query = query;
    festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
    
    elog(DEBUG1, "duckdb_fdw: executing query: %s", query);
    
    /* 4. 执行查询 (DuckDB Native API) */
    if (duckdb_prepare(festate->conn, query, &festate->stmt) == DuckDBError)
    {
        const char *err = duckdb_prepare_error(festate->stmt);
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("duckdb_fdw: prepare failed: %s", err ? err : "unknown error")));
    }
    
    if (duckdb_execute_prepared(festate->stmt, &festate->res) == DuckDBError)
    {
        const char *err = duckdb_result_error(&festate->res);
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR),
                        errmsg("duckdb_fdw: execute failed: %s", err ? err : "unknown error")));
    }
    
    festate->rowidx = 0;
    festate->row_count = duckdb_row_count(&festate->res);
    
    /* 准备元数据转换所需的 AttInMetadata */
    festate->tupdesc = RelationGetDescr(node->ss.ss_currentRelation);
    festate->attinmeta = TupleDescGetAttInMetadata(festate->tupdesc);
}

static TupleTableSlot *
duckdbIterateForeignScan(ForeignScanState *node)
{
	DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    int i;
    ListCell *lc;

	ExecClearTuple(slot);

    /* 检查是否遍历完 */
    if (festate->rowidx >= festate->row_count)
    {
        return slot; /* 返回空 Tuple，表示结束 */
    }

    /* 填充 Tuple */
    /* 我们需要遍历 retrieved_attrs 来知道每一列对应哪个 Postgres 属性 */
    i = 0;
    foreach(lc, festate->retrieved_attrs)
    {
        int attnum = lfirst_int(lc) - 1; /* Postgres attnum 从 1 开始，C 数组从 0 */
        Oid pgtype = festate->tupdesc->attrs[attnum].atttypid;
        int32 pgtypmod = festate->tupdesc->attrs[attnum].atttypmod;
        
        Datum d = duckdb_convert_to_pg(pgtype, pgtypmod, &festate->res, i, festate->rowidx, festate->attinmeta);
        
        slot->tts_values[attnum] = d;
        slot->tts_isnull[attnum] = false; /* convert_to_pg 处理 NULL */
        if (duckdb_value_is_null(&festate->res, i, festate->rowidx))
             slot->tts_isnull[attnum] = true;
             
        i++;
    }
    
    ExecStoreVirtualTuple(slot);
    festate->rowidx++;

	return slot;
}

static void
duckdbReScanForeignScan(ForeignScanState *node)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    
    /* 清理上一次结果 */
    duckdb_destroy_result(&festate->res);
    duckdb_destroy_prepare(&festate->stmt);
    
    /* 重新执行 */
    if (duckdb_prepare(festate->conn, festate->query, &festate->stmt) == DuckDBError)
         elog(ERROR, "duckdb_fdw: rescan prepare failed");
         
    if (duckdb_execute_prepared(festate->stmt, &festate->res) == DuckDBError)
         elog(ERROR, "duckdb_fdw: rescan execute failed");
         
    festate->rowidx = 0;
    festate->row_count = duckdb_row_count(&festate->res);
}

static void
duckdbEndForeignScan(ForeignScanState *node)
{
    DuckDBFdwExecState *festate = (DuckDBFdwExecState *)node->fdw_state;
    
    if (festate)
    {
        duckdb_destroy_result(&festate->res);
        duckdb_destroy_prepare(&festate->stmt);
    }
}

/*
 * 必要的导出函数
 */

void _PG_init(void)
{
    /* 初始化钩子 */
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
Datum duckdb_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(duckdb_library_version()));
}

PG_FUNCTION_INFO_V1(duckdb_execute);
Datum duckdb_execute(PG_FUNCTION_ARGS)
{
    /* 支持 DDL 执行 */
    char *servername = NameStr(*PG_GETARG_NAME(0));
    char *query = text_to_cstring(PG_GETARG_TEXT_PP(1));
    ForeignServer *server = GetForeignServerByName(servername, false);
    duckdb_connection conn = duckdb_get_connection(server, false);
    
    duckdb_do_sql_command(conn, query, NOTICE);
    PG_RETURN_VOID();
}

/* 占位实现 - 暂时不需要复杂功能 */
void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, DuckDBFdwPathExtraData *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost) {}

int
duckdb_set_transmission_modes(void)
{
	return 0;
}

void
duckdb_reset_transmission_modes(int nestlevel)
{
}

Expr *
duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	return NULL;
}

Expr *
duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel)
{
	return NULL;
}
