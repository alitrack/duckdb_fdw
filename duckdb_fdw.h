#ifndef duckdb_fdw_H
#define duckdb_fdw_H

#include "duckdb.h"
#include "postgres.h"
#include "nanoarrow/nanoarrow.h"
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
    
    /* Arrow Vectorized Execution Fields */
    duckdb_arrow_options arrow_options;
    idx_t current_chunk_idx;
    duckdb_data_chunk current_chunk; /* Keep chunk alive for Arrow Array */
    struct ArrowSchema arrow_schema;
    struct ArrowArray arrow_array;
    struct ArrowArrayView arrow_array_view;
    
    /* Legacy / Fallback result (kept for compatibility and error handling) */
    duckdb_result res;

	char	   *query;
	TupleDesc	tupdesc;
    AttInMetadata *attinmeta; /* Required for BuildTupleFromCStrings (Legacy/Fallback) */
	List	   *retrieved_attrs;
	
    /* Iteration state */
    int64_t     current_chunk_row_idx; /* Current row index within the current Arrow Array chunk */
    int64_t     current_chunk_row_count; /* Total rows in the current Arrow Array chunk */
    bool        arrow_initialized;

    /* Appender state for high-performance writes */
    duckdb_appender appender;
    int64_t     batch_row_count;
    char       *table_name; /* Target table name */
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
extern Datum duckdb_convert_to_pg(Oid pgtyp, int pgtypmod, duckdb_result *res, int col, uint64_t row);

/* Deparse functions */
extern void duckdb_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist, List *remote_conds, List *pathkeys, bool has_final_sort, bool has_limit, bool is_subquery, List **retrieved_attrs, List **params_list);
extern void duckdb_deparse_direct_update_sql(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *targetlist, List *targetAttrs, List *remote_conds, List **params_list, List **retrieved_attrs);
extern void duckdb_deparse_direct_delete_sql(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, RelOptInfo *foreignrel, List *remote_conds, List **params_list, List **retrieved_attrs);
extern List *duckdb_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void duckdb_classify_conditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **remote_conds, List **local_conds);
extern bool duckdb_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr);

typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

extern bool duckdb_is_foreign_expr_full(PlannerInfo *root, RelOptInfo *baserel, Expr *expr, foreign_glob_cxt *glob_cxt);

/* Missing symbols */
extern Expr * duckdb_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);
extern Expr * duckdb_find_em_expr_for_input_target(PlannerInfo *root, EquivalenceClass *ec, PathTarget *target, RelOptInfo *fallbackRel);
/* duckdb_get_jointype_name is in deparse.c */
extern int duckdb_set_transmission_modes(void);
extern void duckdb_reset_transmission_modes(int nestlevel);

#endif
