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

typedef struct duckdb_opt
{
	char	   *svr_database;
	char	   *svr_table;
    bool        use_remote_estimate;
} duckdb_opt;

typedef struct DuckDBFdwRelationInfo
{
	/*
	 * True means that the relation can be pushed down to the foreign server.
	 */
	bool		pushdown_safe;
    
    /*
     * Foreign table OID.
     */
    Oid         foreigntableid;

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown.
	 */
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
	Bitmapset  *attrs_used;
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
} DuckDBFdwRelationInfo;

typedef struct DuckDBFdwExecState
{
	duckdb_connection conn;
	duckdb_result res;
	char	   *query;
	TupleDesc	tupdesc;
    AttInMetadata *attinmeta; /* Required for BuildTupleFromCStrings */
	List	   *retrieved_attrs;
	int64		rowidx;
    int64       row_count;
} DuckDBFdwExecState;

/* Exported functions */
extern Datum duckdb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum duckdb_fdw_validator(PG_FUNCTION_ARGS);
extern Datum duckdb_fdw_version(PG_FUNCTION_ARGS);
extern Datum duckdb_execute(PG_FUNCTION_ARGS);
extern List *duckdb_import_foreign_schema(ImportForeignSchemaStmt *stmt, Oid serverOid);
extern duckdb_opt * duckdb_get_options(Oid foreigntableid);

/* Internal functions */
extern void duckdb_do_sql_command(duckdb_connection conn, const char *sql, int level);
extern duckdb_connection duckdb_get_connection(ForeignServer *server, bool truncatable);

/* Helper to get cleaned C-String for BuildTupleFromCStrings */
extern char *duckdb_extract_as_cstring(duckdb_result *res, int col, uint64_t row, Oid pgtyp);

/* Deparse functions */
extern void duckdb_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist, List *remote_conds, List *pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List **retrieved_attrs, List **params_list);
extern List *duckdb_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void duckdb_classify_conditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **remote_conds, List **local_conds);
extern bool duckdb_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);

/* Missing symbols */
extern Expr * duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern Expr * duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel);
/* duckdb_get_jointype_name is in deparse.c */
extern int duckdb_set_transmission_modes(void);
extern void duckdb_reset_transmission_modes(int nestlevel);

#endif
