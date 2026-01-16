#ifndef duckdb_fdw_H
#define duckdb_fdw_H

#include "duckdb.h"
#include "postgres.h"
#include "funcapi.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/pathnodes.h"
#include "utils/rel.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"

/* 兼容性定义 */
#define DUCKDB_OK DuckDBSuccess
#define DUCKDB_DONE DuckDBSuccess
#define DUCKDB_ROW DuckDBSuccess

/* 选项结构体 */
typedef struct duckdb_opt
{
	char	   *svr_database;
	char	   *svr_table;
	bool		use_remote_estimate;
} duckdb_opt;

/* 结构体声明 - 用于解决 deparse.c 依赖 */
typedef struct DuckDBFdwPathExtraData
{
	PathTarget *target;
	bool		has_final_sort;
	bool		has_limit;
	double		limit_tuples;
	int64		count_est;
	int64		offset_est;
} DuckDBFdwPathExtraData;

typedef struct DuckDBFdwRelationInfo
{
	bool		pushdown_safe;
	List	   *remote_conds;
	List	   *local_conds;
	double		rows;
	int			width;
	Cost		startup_cost;
	Cost		total_cost;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *user;
	bool		use_remote_estimate;
	Bitmapset  *attrs_used;
	bool		qp_is_pushdown_safe;
	Selectivity local_conds_sel;
	QualCost	local_conds_cost;
	char	   *relation_name;
	int			relation_index;
    bool        make_outerrel_subquery;
    bool        make_innerrel_subquery;
    Relids      lower_subquery_rels;
    JoinType    jointype;
    RelOptInfo *outerrel;
    RelOptInfo *innerrel;
    List       *joinclauses;
    List       *grouped_tlist;
    bool        is_tlist_func_pushdown;
    List       *final_remote_exprs;
    double      retrieved_rows;
    Cost        rel_startup_cost;
    Cost        rel_total_cost;
    int         fetch_size;
    List       *shippable_extensions;
    UpperRelationKind stage;
    Selectivity joinclause_sel;
} DuckDBFdwRelationInfo;

typedef struct DuckDBFdwExecState
{
	duckdb_database db;
	duckdb_connection conn;
	duckdb_prepared_statement stmt;
	duckdb_result res;
	char	   *query;
	Relation	rel;
	TupleDesc	tupdesc;
	AttInMetadata *attinmeta;
	List	   *retrieved_attrs;
	bool		cursor_exists;
	int64		rowidx;
	int64       row_count;
	MemoryContext temp_cxt;
    int         batch_size;
    List       *param_exprs;
    FmgrInfo   *param_flinfo;
    Oid        *param_types;
    const char **param_values;
    int         numParams;
    bool        for_update;
    int         num_slots;
    char       *orig_query;
    List       *target_attrs;
    int         values_end;
    int         p_nums;
    FmgrInfo   *p_flinfo;
    AttrNumber *junk_idx;
	void       *arrow_result;
	void       *arrow_array;
	int64		arrow_row_idx;
	int64		arrow_row_count;
} DuckDBFdwExecState;

typedef struct DuckDBFdwDirectModifyState
{
    Relation    rel;
    duckdb_connection conn;
    duckdb_prepared_statement stmt;
    duckdb_result res;
    int         num_tuples;
    char       *query;
    bool        has_returning;
    List       *retrieved_attrs;
    bool        set_processed;
    int         numParams;
    FmgrInfo   *param_flinfo;
    List       *param_exprs;
    const char **param_values;
    Oid        *param_types;
    MemoryContext temp_cxt;
    Relation    resultRel;
} DuckDBFdwDirectModifyState;

/* 枚举定义 (移自 duckdb_fdw.c) */
enum FdwPathPrivateIndex
{
	FdwPathPrivateHasFinalSort,
	FdwPathPrivateHasLimit
};

enum FdwScanPrivateIndex
{
	FdwScanPrivateSelectSql,
	FdwScanPrivateRetrievedAttrs,
	FdwScanPrivateForUpdate,
	FdwScanPrivateRtIndex,
	FdwScanPrivateRelations
};

enum FdwModifyPrivateIndex
{
	FdwModifyPrivateUpdateSql,
	FdwModifyPrivateTargetAttnums,
	FdwModifyPrivateLen
};

enum FdwDirectModifyPrivateIndex
{
	FdwDirectModifyPrivateUpdateSql,
	FdwDirectModifyPrivateHasReturning,
	FdwDirectModifyPrivateRetrievedAttrs,
	FdwDirectModifyPrivateSetProcessed
};

/* 导出函数 */
extern Datum duckdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum duckdb_fdw_version(PG_FUNCTION_ARGS);
extern Datum duckdb_execute(PG_FUNCTION_ARGS);
extern duckdb_opt * duckdb_get_options(Oid foreigntableid);

/* 内部函数 */
extern void duckdb_do_sql_command(duckdb_connection conn, const char *sql, int level);
extern duckdb_connection duckdb_get_connection(ForeignServer *server, bool truncatable);
extern void duckdb_cleanup_connection(void);
extern Datum duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, duckdb_result *res, int col, uint64_t row, AttInMetadata *attinmeta);
extern void duckdb_bind_sql_var(Oid type, int attnum, Datum value, duckdb_prepared_statement stmt, bool *isnull);

/* deparse.c 函数 */
extern bool duckdb_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern bool duckdb_is_foreign_param(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);
extern void duckdb_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist, List *remote_conds, List *pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List **retrieved_attrs, List **params_list);
extern void duckdb_deparse_insert(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, bool doNothing, int *values_end_len);
extern void duckdb_rebuild_insert(StringInfo buf, Relation rel, char *orig_query, List *target_attrs, int values_end_len, int num_params, int num_rows);
extern void duckdb_deparse_update(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, List *attname);
extern void duckdb_deparse_direct_update_sql(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *targetlist, List *targetAttrs, List *remote_conds, List **params_list, List **retrieved_attrs);
extern void duckdb_deparse_delete(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *name);
extern void duckdb_deparse_direct_delete_sql(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *remote_conds, List **params_list, List **retrieved_attrs);
extern void duckdb_classify_conditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **remote_conds, List **local_conds);
extern void duckdb_append_where_clause(StringInfo buf, PlannerInfo *root, RelOptInfo *baserel, List *exprs, bool is_first, List **params);
extern void duckdb_deparse_analyze(StringInfo buf, char *dbname, char *relname);
extern void duckdb_deparse_string_literal(StringInfo buf, const char *val);
extern void duckdb_deparse_truncate(StringInfo buf, List *rels);
extern List *duckdb_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern bool duckdb_is_foreign_function_tlist(PlannerInfo *root, RelOptInfo *baserel, List *tlist);

/* duckdb_fdw.c 内部私有声明 */
extern Expr *duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern Expr *duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel);
extern void duckdb_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds, List *pathkeys, DuckDBFdwPathExtraData *fpextra, double *p_rows, int *p_width, Cost *p_startup_cost, Cost *p_total_cost);
extern int duckdb_set_transmission_modes(void);
extern void duckdb_reset_transmission_modes(int nestlevel);
extern const char * duckdb_get_jointype_name(JoinType jointype);
extern TupleDesc duckdb_get_tupdesc_for_join_scan_tuples(ForeignScanState *node);

#endif