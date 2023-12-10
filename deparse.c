/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        deparse.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "duckdb_fdw.h"

#include "pgtime.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"
#include "commands/tablecmds.h"

/*
 * Global context for sqlite_foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for sqlite_foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/*
 * Context for sqlite_deparse_expr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
	Expr	   *complementarynode;	/* variable where we can store, only if
									 * needed, a complementary node to obtain
									 * info for processing actual node.
									 * Created mostly for
									 * sqlite_deparse_op_expr to have both
									 * nodes accesible during each node
									 * deparse. */
} deparse_expr_cxt;

#define QUOTE '"'
#define REL_ALIAS_PREFIX	"r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)	\
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX	"s"
#define SUBQUERY_COL_ALIAS_PREFIX	"c"

/*
 * Functions to determine whether an expression can be evaluated safely on
 * remote server.
 */
static bool sqlite_foreign_expr_walker(Node *node,
									   foreign_glob_cxt *glob_cxt,
									   foreign_loc_cxt *outer_cxt);

/*
 * Functions to construct string representation of a node tree.
 */
static void sqlite_deparse_expr(Expr *expr, deparse_expr_cxt *context);
static void sqlite_deparse_var(Var *node, deparse_expr_cxt *context);
static void sqlite_deparse_const(Const *node, deparse_expr_cxt *context, int showtype);
static void sqlite_deparse_param(Param *node, deparse_expr_cxt *context);
static void sqlite_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void sqlite_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void sqlite_deparse_operator_name(StringInfo buf, Form_pg_operator opform);

static void sqlite_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
												deparse_expr_cxt *context);
static void sqlite_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context);
static void sqlite_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void sqlite_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void sqlite_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context);
static void sqlite_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,
									  deparse_expr_cxt *context);
static void sqlite_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
											deparse_expr_cxt *context);
static void sqlite_deparse_relation(StringInfo buf, Relation rel);
static void sqlite_deparse_target_list(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel,
									   Bitmapset *attrs_used, bool qualify_col, List **retrieved_attrs, bool is_concat, bool check_null);
static void sqlite_deparse_column_ref(StringInfo buf, int varno, int varattno, PlannerInfo *root, bool qualify_col);
static void sqlite_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context);
static void sqlite_deparse_case_expr(CaseExpr *node, deparse_expr_cxt *context);
static void sqlite_deparse_null_if_expr(NullIfExpr *node, deparse_expr_cxt *context);
static void sqlite_deparse_coalesce_expr(CoalesceExpr *node, deparse_expr_cxt *context);
static void sqlite_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
											 bool use_alias, Index ignore_rel, List **ignore_conds,
											 List **params_list);
static void sqlite_deparse_range_tbl_ref(StringInfo buf, PlannerInfo *root,
										 RelOptInfo *foreignrel, bool make_subquery,
										 Index ignore_rel, List **ignore_conds, List **params_list);
static void sqlite_deparse_from_expr(List *quals, deparse_expr_cxt *context);
static void sqlite_deparse_aggref(Aggref *node, deparse_expr_cxt *context);
static void sqlite_append_limit_clause(deparse_expr_cxt *context);
static void sqlite_append_conditions(List *exprs, deparse_expr_cxt *context);
static void sqlite_append_group_by_clause(List *tlist, deparse_expr_cxt *context);
static void sqlite_append_agg_order_by(List *orderList, List *targetList,
									   deparse_expr_cxt *context);
static void sqlite_append_order_by_clause(List *pathkeys, bool has_final_sort, deparse_expr_cxt *context);
static void sqlite_append_function_name(Oid funcid, deparse_expr_cxt *context);
const char *sqlite_get_jointype_name(JoinType jointype);
static Node *sqlite_deparse_sort_group_clause(Index ref, List *tlist, bool force_colno,
											  deparse_expr_cxt *context);
static void sqlite_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,
												deparse_expr_cxt *context);
static void sqlite_deparse_minmax_expr(MinMaxExpr *node, deparse_expr_cxt *context);

/*
 * Helper functions
 */
static bool sqlite_is_subquery_var(Var *node, RelOptInfo *foreignrel,
								   int *relno, int *colno);
static void sqlite_get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel,
												 int *relno, int *colno);
static char *sqlite_quote_identifier(const char *s, char q);
static bool sqlite_contain_immutable_functions_walker(Node *node, void *context);
static bool sqlite_is_valid_type(Oid type);
static bool sqlite_is_builtin(Oid objectId);

/*
 * Append remote name of specified foreign table to buf.
 * Use value of table_name FDW option (if any) instead of relation's name.
 * Similarly, schema_name FDW option overrides schema name.
 */
static void
sqlite_deparse_relation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *relname = NULL;
	ListCell   *lc = NULL;

	/* obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table") == 0)
			relname = defGetString(def);
	}

	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	/*
	 * DuckDB now has the concept of multiple databases, so pass the table name in
	 * without prepending "main" and without quotes.
	 * Ex: my_db.my_schema.my_table is allowed
	 */
	appendStringInfo(buf, "%s", relname);
}

static char *
sqlite_quote_identifier(const char *s, char q)
{
	char	   *result = palloc(strlen(s) * 2 + 3);
	char	   *r = result;

	*r++ = q;
	while (*s)
	{
		if (*s == q)
			*r++ = *s;
		*r++ = *s;
		s++;
	}
	*r++ = q;
	*r++ = '\0';
	return result;
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
sqlite_is_foreign_expr(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) (baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!sqlite_foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity. (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * Returns true if given expr is something we'd have to send the value of
 * to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * deparseExpr would add to context->params_list.  Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
bool
sqlite_is_foreign_param(PlannerInfo *root,
						RelOptInfo *baserel,
						Expr *expr)
{
	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			{
				/* It would have to be sent unless it's a foreign Var */
				Var		   *var = (Var *) expr;
				SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) (baserel->fdw_private);
				Relids		relids;

				if (IS_UPPER_REL(baserel))
					relids = fpinfo->outerrel->relids;
				else
					relids = baserel->relids;

				if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
					return false;	/* foreign Var, so not a param */
				else
					return true;	/* it'd have to be a param */
				break;
			}
		case T_Param:
			/* Params always have to be sent to the foreign server */
			return true;
		default:
			break;
	}
	return false;
}

static bool
sqlite_is_valid_type(Oid type)
{
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case VARCHAROID:
		case TEXTOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return true;
	}
	return false;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.  Because of the latter, the logic is
 * pretty close to assign_collations_walker() in parse_collate.c, though we
 * can assume here that the given expression is valid.
 */
static bool
sqlite_foreign_expr_walker(Node *node,
						   foreign_glob_cxt *glob_cxt,
						   foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid			collation = InvalidOid;
	FDWCollateState state = FDW_COLLATE_NONE;
	HeapTuple	tuple;
	Form_pg_operator form;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, ie it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */

					/*
					 * System columns (e.g. oid, ctid) should not be sent to
					 * the remote, since we don't make any effort to ensure
					 * that local and remote values match (tableoid, in
					 * particular, almost certainly doesn't match).
					 */
					if (var->varattno < 0)
						return false;

					/* Else check the collation */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Var belongs to some other table */
					collation = var->varcollid;
					if (collation == InvalidOid ||
						collation == DEFAULT_COLLATION_OID)
					{
						/*
						 * It's noncollatable, or it's safe to combine with a
						 * collatable foreign Var, so set state to NONE.
						 */
						state = FDW_COLLATE_NONE;
					}
					else
					{
						/*
						 * Do not fail right away, since the Var might appear
						 * in a collation-insensitive context.
						 */
						state = FDW_COLLATE_UNSAFE;
					}
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/* SQLite cannot handle interval type */
				if (c->consttype == INTERVALOID)
					return false;

				/*
				 * If the constant has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr;
				 * either way, it's unsafe to send to the remote.
				 */
				if (c->constcollid != InvalidOid &&
					c->constcollid != DEFAULT_COLLATION_OID)
					return false;

				/* Otherwise, we can consider that it doesn't set collation */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_CaseTestExpr:
			{
				CaseTestExpr *c = (CaseTestExpr *) node;

				/*
				 * If the expr has nondefault collation, either it's of a
				 * non-builtin type, or it reflects folding of a CollateExpr;
				 * either way, it's unsafe to send to the remote.
				 */
				if (c->collation != InvalidOid &&
					c->collation != DEFAULT_COLLATION_OID)
					return false;

				/* Otherwise, we can consider that it doesn't set collation */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				/*
				 * If it's a MULTIEXPR Param, punt.  We can't tell from here
				 * whether the referenced sublink/subplan contains any remote
				 * Vars; if it does, handling that is too complicated to
				 * consider supporting at present.  Fortunately, MULTIEXPR
				 * Params are not reduced to plain PARAM_EXEC until the end of
				 * planning, so we can easily detect this case.  (Normal
				 * PARAM_EXEC Params are safe to ship because their values
				 * come from somewhere else in the plan tree; but a MULTIEXPR
				 * references a sub-select elsewhere in the same targetlist,
				 * so we'd be on the hook to evaluate it somehow if we wanted
				 * to handle such cases as direct foreign updates.)
				 */
				if (p->paramkind == PARAM_MULTIEXPR)
					return false;

				if (!sqlite_is_valid_type(p->paramtype))
					return false;

				/*
				 * Collation rule is same as for Consts and non-foreign Vars.
				 */
				collation = p->paramcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *func = (FuncExpr *) node;
				char	   *opername = NULL;
				Oid			schema;

				/* get function name and schema */
				tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->funcid));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for function %u", func->funcid);
				}
				opername = pstrdup(((Form_pg_proc) GETSTRUCT(tuple))->proname.data);
				schema = ((Form_pg_proc) GETSTRUCT(tuple))->pronamespace;
				ReleaseSysCache(tuple);

				/* ignore functions in other than the pg_catalog schema */
				if (schema != PG_CATALOG_NAMESPACE)
					return false;

				/*
				 * These function can be passed to SQLite. In case of nest
				 * functions, for example, (round(abs(c1), 0) = 1, the
				 * postgres core will transform to
				 * (round((abs(t1.c1))::numeric, 0) = '1'::numeric), so
				 * "::numberic" is kind of function format
				 * COERCE_IMPLICIT_CAST we must check.
				 *
				 * In SQLite, lower/upper function does not support UNICODE
				 * character, so we don't push down these functions.
				 *
				 */
				if (!(func->funcformat == COERCE_IMPLICIT_CAST
					  || strcmp(opername, "abs") == 0
					  || strcmp(opername, "floor") == 0
					  || strcmp(opername, "ceil") == 0
					  || strcmp(opername, "btrim") == 0
					  || strcmp(opername, "length") == 0
					  || strcmp(opername, "ltrim") == 0
					  || strcmp(opername, "replace") == 0
					  || strcmp(opername, "round") == 0
					  || strcmp(opername, "rtrim") == 0
					  || strcmp(opername, "substr") == 0))
				{
					return false;
				}

				if (!sqlite_foreign_expr_walker((Node *) func->args,
												glob_cxt, &inner_cxt))
					return false;


				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (func->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 func->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = func->funccollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
		case T_NullIfExpr:
			{
				char	   *cur_opname = NULL;
				OpExpr	   *oe = (OpExpr *) node;

				/*
				 * Similarly, only built-in operators can be sent to remote.
				 * (If the operator is, surely its underlying function is
				 * too.)
				 */
				if (!sqlite_is_builtin(oe->opno))
					return false;

				tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for operator %u", oe->opno);
				form = (Form_pg_operator) GETSTRUCT(tuple);

				/* opname is not a SQL identifier, so we should not quote it. */
				cur_opname = pstrdup(NameStr(form->oprname));
				ReleaseSysCache(tuple);

				/*
				 * Factorial (!) and Bitwise XOR (^) cannot be pushed down to
				 * SQLite
				 */
				if (strcmp(cur_opname, "!") == 0
					|| strcmp(cur_opname, "^") == 0)
				{
					return false;
				}

				/* ILIKE cannot be pushed down to SQLite */
				if (strcmp(cur_opname, "~~*") == 0 || strcmp(cur_opname, "!~~*") == 0)
				{
					return false;
				}

				/*
				 * Recurse to input subexpressions.
				 */
				if (!sqlite_foreign_expr_walker((Node *) oe->args,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Result-collation handling is same as for functions */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/*
				 * Again, only built-in operators can be sent to remote.
				 */
				if (!sqlite_is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!sqlite_foreign_expr_walker((Node *) oe->args,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!sqlite_foreign_expr_walker((Node *) r->arg,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var.
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!sqlite_foreign_expr_walker((Node *) b->args,
												glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!sqlite_foreign_expr_walker((Node *) nt->arg,
												glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!sqlite_foreign_expr_walker((Node *) lfirst(lc),
													glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_CoalesceExpr:
			{
				CoalesceExpr *coalesce = (CoalesceExpr *) node;
				ListCell   *lc;

				if (list_length(coalesce->args) < 2)
					return false;

				/* Recurse to each argument */
				foreach(lc, coalesce->args)
				{
					if (!sqlite_foreign_expr_walker((Node *) lfirst(lc),
													glob_cxt, &inner_cxt))
						return false;
				}
			}
			break;
		case T_CaseExpr:
			{
				ListCell   *lc;

				/* Recurse to condition subexpressions. */
				foreach(lc, ((CaseExpr *) node)->args)
				{
					if (!sqlite_foreign_expr_walker((Node *) lfirst(lc),
													glob_cxt, &inner_cxt))
						return false;
				}
			}
			break;
		case T_CaseWhen:
			{
				CaseWhen   *whenExpr = (CaseWhen *) node;

				/* Recurse to condition expression. */
				if (!sqlite_foreign_expr_walker((Node *) whenExpr->expr,
												glob_cxt, &inner_cxt))
					return false;
				/* Recurse to result expression. */
				if (!sqlite_foreign_expr_walker((Node *) whenExpr->result,
												glob_cxt, &inner_cxt))
					return false;
				/* Don't apply exprType() to the case when expr. */
				check_type = false;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;
				char	   *opername = NULL;
				Oid			schema;

				/* get function name and schema */
				tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(agg->aggfnoid));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for function %u", agg->aggfnoid);
				}
				opername = pstrdup(((Form_pg_proc) GETSTRUCT(tuple))->proname.data);
				schema = ((Form_pg_proc) GETSTRUCT(tuple))->pronamespace;
				ReleaseSysCache(tuple);

				/* ignore functions in other than the pg_catalog schema */
				if (schema != PG_CATALOG_NAMESPACE)
					return false;

				/* these function can be passed to SQLite */
				if (!(strcmp(opername, "sum") == 0
					  || strcmp(opername, "avg") == 0
					  || strcmp(opername, "max") == 0
					  || strcmp(opername, "min") == 0
					  || strcmp(opername, "array_agg") == 0
					  || strcmp(opername, "stddev_pop") == 0
					  || strcmp(opername, "stddev_samp") == 0
					  || strcmp(opername, "mode") == 0
					  || strcmp(opername, "percentile_cont") == 0
					  || strcmp(opername, "percentile_disc") == 0
					  || strcmp(opername, "count") == 0))
				{
					return false;
				}


				/* Not safe to pushdown when not in grouping context */
				if (!IS_UPPER_REL(glob_cxt->foreignrel))
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!sqlite_foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!sqlite_foreign_expr_walker((Node *) a->elements,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * ArrayExpr must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a function).
				 */
				collation = a->array_collid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_MinMaxExpr:
			{
				MinMaxExpr *mmexpr = (MinMaxExpr *) node;

				if (mmexpr->op != IS_LEAST && mmexpr->op != IS_GREATEST)
					return false;

				if (!sqlite_foreign_expr_walker((Node *) mmexpr->args,
												glob_cxt, &inner_cxt))
					return false;

				/*
				 * If minmax's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (mmexpr->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 mmexpr->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * MinMaxExpr must not introduce a collation not derived from
				 * an input foreign Var (same logic as for a function).
				 */
				collation = mmexpr->minmaxcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_DistinctExpr:
			/* IS DISTINCT FROM */
			return false;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !sqlite_is_builtin(exprType(node)))
		return false;

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}
	/* It looks OK */
	return true;
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.  If foreignrel is an upper relation,
 * then the output targetlist can also contains expressions to be evaluated on
 * foreign server.
 */
List *
sqlite_build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List	   *tlist = NIL;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) foreignrel->fdw_private;
	ListCell   *lc;

	/*
	 * For an upper relation, we have already built the target list while
	 * checking shippability, so just return that.
	 */
	if (IS_UPPER_REL(foreignrel))
		return fpinfo->grouped_tlist;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist = add_to_flat_tlist(tlist,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_RECURSE_PLACEHOLDERS));
	}

	return tlist;
}


/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign server.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause
 * (or, in the case of upper relations, into the HAVING clause).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void
sqlite_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
								   List *tlist, List *remote_conds, List *pathkeys,
								   bool has_final_sort, bool has_limit, bool is_subquery,
								   List **retrieved_attrs,
								   List **params_list)
{
	deparse_expr_cxt context;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) rel->fdw_private;
	List	   *quals;

	/*
	 * We handle relations for foreign tables, joins between those and upper
	 * relations.
	 */
	Assert(IS_JOIN_REL(rel) ||
		   IS_SIMPLE_REL(rel) ||
		   IS_OTHER_REL(rel) ||
		   IS_UPPER_REL(rel));
	/* Fill portions of context common to upper, join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.scanrel = IS_UPPER_REL(rel) ?
		fpinfo->outerrel : rel;
	context.params_list = params_list;

	/* Construct SELECT clause */
	sqlite_deparse_select(tlist, retrieved_attrs, &context);

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(rel))
	{
		SqliteFdwRelationInfo *ofpinfo;

		ofpinfo = (SqliteFdwRelationInfo *) fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds;
	}
	else
		quals = remote_conds;

	/* Construct FROM and WHERE clauses */
	sqlite_deparse_from_expr(quals, &context);

	if (IS_UPPER_REL(rel))
	{
		/* Append GROUP BY clause */
		sqlite_append_group_by_clause(tlist, &context);

		/* Append HAVING clause */
		if (remote_conds)
		{
			appendStringInfo(buf, " HAVING ");
			sqlite_append_conditions(remote_conds, &context);
		}
	}

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		sqlite_append_order_by_clause(pathkeys, has_final_sort, &context);

	/* Add LIMIT clause if necessary */
	if (has_limit)
		sqlite_append_limit_clause(&context);

}



/*
 * Deparese SELECT statment
 */
static void
sqlite_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	PlannerInfo *root = context->root;
	RelOptInfo *foreignrel = context->foreignrel;
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) foreignrel->fdw_private;

	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");

	if (IS_JOIN_REL(foreignrel) ||
		fpinfo->is_tlist_func_pushdown == true ||
		IS_UPPER_REL(foreignrel))
	{
		/*
		 * For a join or upper relation the input tlist gives the list of
		 * columns required to be fetched from the foreign server.
		 */
		sqlite_deparse_explicit_target_list(tlist, retrieved_attrs, context);
	}
	else
	{
		/*
		 * For a base relation fpinfo->attrs_used gives the list of columns
		 * required to be fetched from the foreign server.
		 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = table_open(rte->relid, NoLock);

		sqlite_deparse_target_list(buf, root, foreignrel->relid, rel, fpinfo->attrs_used, false, retrieved_attrs, false, false);

		table_close(rel, NoLock);
	}
}

/*
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 */
static void
sqlite_deparse_from_expr(List *quals, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;

	/* For upper relations, scanrel must be either a joinrel or a baserel */
	Assert(!IS_UPPER_REL(context->foreignrel) ||
		   IS_JOIN_REL(scanrel) ||
		   IS_SIMPLE_REL(scanrel));

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	sqlite_deparse_from_expr_for_rel(buf, context->root, scanrel,
									 (bms_num_members(scanrel->relids) == BMS_MULTIPLE),
									 (Index) 0, NULL,
									 context->params_list);

	/* Construct WHERE clause */
	if (quals != NIL)
	{
		appendStringInfo(buf, " WHERE ");
		sqlite_append_conditions(quals, context);
	}
}


/*
 * Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to
 * deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 */
static void
sqlite_append_conditions(List *exprs, deparse_expr_cxt *context)
{
	int			nestlevel;
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = sqlite_set_transmission_modes();

	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *) expr)->clause;

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		sqlite_deparse_expr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}

	sqlite_reset_transmission_modes(nestlevel);
}

/* Output join name for given join type */
const char *
sqlite_get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		case JOIN_FULL:
			return "FULL";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}


/*
 * Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 */
static void
sqlite_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,
									deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	*retrieved_attrs = NIL;

	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (i > 0)
			appendStringInfoString(buf, ", ");
		sqlite_deparse_expr((Expr *) tle->expr, context);

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0)
		appendStringInfoString(buf, "NULL");
}


/*
 * Construct FROM clause for given relation
 *
 * The function constructs ... JOIN ... ON ... for join relation. For a base
 * relation it just returns schema-qualified tablename, with the appropriate
 * alias if so requested.
 */
static void
sqlite_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
								 bool use_alias, Index ignore_rel, List **ignore_conds,
								 List **params_list)
{
	if (IS_JOIN_REL(foreignrel))
	{
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;
		SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) foreignrel->fdw_private;
		RelOptInfo *outerrel = fpinfo->outerrel;
		RelOptInfo *innerrel = fpinfo->innerrel;
		bool		outerrel_is_target = false;
		bool		innerrel_is_target = false;

		if (ignore_rel > 0 && bms_is_member(ignore_rel, foreignrel->relids))
		{
			/*
			 * If this is an inner join, add joinclauses to *ignore_conds and
			 * set it to empty so that those can be deparsed into the WHERE
			 * clause.  Note that since the target relation can never be
			 * within the nullable side of an outer join, those could safely
			 * be pulled up into the WHERE clause (see foreign_join_ok()).
			 * Note also that since the target relation is only inner-joined
			 * to any other relation in the query, all conditions in the join
			 * tree mentioning the target relation could be deparsed into the
			 * WHERE clause by doing this recursively.
			 */
			if (fpinfo->jointype == JOIN_INNER)
			{
				*ignore_conds = list_concat(*ignore_conds,
											fpinfo->joinclauses);
				fpinfo->joinclauses = NIL;
			}

			/*
			 * Check if either of the input relations is the target relation.
			 */
			if (outerrel->relid == ignore_rel)
				outerrel_is_target = true;
			else if (innerrel->relid == ignore_rel)
				innerrel_is_target = true;
		}

		/* Deparse outer relation */
		if (!outerrel_is_target)
		{
			initStringInfo(&join_sql_o);
			sqlite_deparse_range_tbl_ref(&join_sql_o, root, outerrel,
										 fpinfo->make_outerrel_subquery,
										 ignore_rel, ignore_conds, params_list);

			/*
			 * If inner relation is the target relation, skip deparsing it.
			 * Note that since the join of the target relation with any other
			 * relation in the query is an inner join and can never be within
			 * the nullable side of an outer join, the join could be
			 * interchanged with higher-level joins (cf. identity 1 on outer
			 * join reordering shown in src/backend/optimizer/README), which
			 * means it's safe to skip the target-relation deparsing here.
			 */
			if (innerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendBinaryStringInfo(buf, join_sql_o.data, join_sql_o.len);
				return;
			}
		}

		/* Deparse inner relation */
		if (!innerrel_is_target)
		{
			initStringInfo(&join_sql_i);
			sqlite_deparse_range_tbl_ref(&join_sql_i, root, innerrel,
										 fpinfo->make_innerrel_subquery,
										 ignore_rel, ignore_conds, params_list);

			/*
			 * If outer relation is the target relation, skip deparsing it.
			 * See the above note about safety.
			 */
			if (outerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendBinaryStringInfo(buf, join_sql_i.data, join_sql_i.len);
				return;
			}
		}

		/* Neither of the relations is the target relation. */
		Assert(!outerrel_is_target && !innerrel_is_target);

		/*
		 * For a join relation FROM clause entry is deparsed as ((outer
		 * relation) <join type> (inner relation) ON (joinclauses))
		 */
		appendStringInfo(buf, "(%s %s JOIN %s ON ", join_sql_o.data,
						 sqlite_get_jointype_name(fpinfo->jointype), join_sql_i.data);

		/* Append join clause; (TRUE) if no join clause */
		if (fpinfo->joinclauses)
		{
			deparse_expr_cxt context;

			context.buf = buf;
			context.foreignrel = foreignrel;
			context.scanrel = foreignrel;
			context.root = root;
			context.params_list = params_list;

			appendStringInfo(buf, "(");
			sqlite_append_conditions(fpinfo->joinclauses, &context);
			appendStringInfo(buf, ")");
		}
		else
			appendStringInfoString(buf, "(TRUE)");

		/* End the FROM clause entry. */
		appendStringInfo(buf, ")");
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation	rel = table_open(rte->relid, NoLock);

		sqlite_deparse_relation(buf, rel);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);

		table_close(rel, NoLock);
	}
}

/*
 * Append FROM clause entry for the given relation into buf.
 */
static void
sqlite_deparse_range_tbl_ref(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
							 bool make_subquery, Index ignore_rel, List **ignore_conds,
							 List **params_list)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) foreignrel->fdw_private;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	Assert(fpinfo->local_conds == NIL);

	/* If make_subquery is true, deparse the relation as a subquery. */
	if (make_subquery)
	{
		List	   *retrieved_attrs;
		int			ncols;

		/*
		 * The given relation shouldn't contain the target relation, because
		 * this should only happen for input relations for a full join, and
		 * such relations can never contain an UPDATE/DELETE target.
		 */
		Assert(ignore_rel == 0 ||
			   !bms_is_member(ignore_rel, foreignrel->relids));

		/* Deparse the subquery representing the relation. */
		appendStringInfoChar(buf, '(');
		sqlite_deparse_select_stmt_for_rel(buf, root, foreignrel, NIL,
										   fpinfo->remote_conds, NIL,
										   false, false, true,
										   &retrieved_attrs, params_list);
		appendStringInfoChar(buf, ')');

		/* Append the relation alias. */
		appendStringInfo(buf, " %s%d", SUBQUERY_REL_ALIAS_PREFIX,
						 fpinfo->relation_index);

		/*
		 * Append the column aliases if needed.  Note that the subquery emits
		 * expressions specified in the relation's reltarget (see
		 * deparseSubqueryTargetList).
		 */
		ncols = list_length(foreignrel->reltarget->exprs);
		if (ncols > 0)
		{
			int			i;

			appendStringInfoChar(buf, '(');
			for (i = 1; i <= ncols; i++)
			{
				if (i > 1)
					appendStringInfoString(buf, ", ");

				appendStringInfo(buf, "%s%d", SUBQUERY_COL_ALIAS_PREFIX, i);
			}
			appendStringInfoChar(buf, ')');
		}
	}
	else
		sqlite_deparse_from_expr_for_rel(buf, root, foreignrel, true, ignore_rel,
										 ignore_conds, params_list);
}

/*
 * deparse remote INSERT statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
sqlite_deparse_insert(StringInfo buf, PlannerInfo *root,
					  Index rtindex, Relation rel,
					  List *targetAttrs, bool doNothing,
					  int *values_end_len)
{
#if PG_VERSION_NUM >= 140000
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		all_columns_generated = true;
#endif
	AttrNumber	pindex;
	bool		first;
	ListCell   *lc;

	appendStringInfo(buf, "INSERT %sINTO ", doNothing ? "OR IGNORE " : "");
	sqlite_deparse_relation(buf, rel);

#if PG_VERSION_NUM >= 140000

	/*
	 * Check all columns in table that they are all generated column or not.
	 * If true, we will skip all columns and just add 'DEFAULT VALUES'. If
	 * not, we still push down other columns which are not generated column.
	 */
	if (targetAttrs)
	{
		foreach(lc, targetAttrs)
		{
			int			attnum = linitial_int(targetAttrs);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attgenerated)
			{
				all_columns_generated = false;
				break;
			}
		}
	}
#endif

#if (PG_VERSION_NUM >= 140000)
	if (targetAttrs && !all_columns_generated)
#else
	if (targetAttrs)
#endif
	{
		appendStringInfoChar(buf, '(');

		first = true;
		foreach(lc, targetAttrs)
		{
			int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM >= 140000
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attgenerated)
			{
#endif
				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				sqlite_deparse_column_ref(buf, rtindex, attnum, root, false);
#if PG_VERSION_NUM >= 140000
			}
#endif
		}

		appendStringInfoString(buf, ") VALUES (");

		pindex = 1;
		first = true;
		foreach(lc, targetAttrs)
		{
#if PG_VERSION_NUM >= 140000
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attgenerated)
			{
#endif
				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;
				appendStringInfo(buf, "?");
				pindex++;
#if PG_VERSION_NUM >= 140000
			}
#endif
		}

		appendStringInfoChar(buf, ')');
	}
	else
		appendStringInfoString(buf, " DEFAULT VALUES");
	*values_end_len = buf->len;
}

#if PG_VERSION_NUM >= 140000
/*
 * rebuild remote INSERT statement
 *
 * Provided a number of rows in a batch, builds INSERT statement with the
 * right number of parameters.
 */
void
sqlite_rebuild_insert(StringInfo buf, Relation rel, char *orig_query,
					  List *target_attrs, int values_end_len, int num_params,
					  int num_rows)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;
	bool		first;
	ListCell   *lc;

	/* Make sure the values_end_len is sensible */
	Assert((values_end_len > 0) && (values_end_len <= strlen(orig_query)));

	/* Copy up to the end of the first record from the original query */
	appendBinaryStringInfo(buf, orig_query, values_end_len);

	/*
	 * Add records to VALUES clause (we already have parameters for the first
	 * row, so start at the right offset).
	 */
	for (i = 0; i < num_rows; i++)
	{
		appendStringInfoString(buf, ", (");

		first = true;
		foreach(lc, target_attrs)
		{
			int			attnum = lfirst_int(lc);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attgenerated)
			{
				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				appendStringInfo(buf, "?");
			}
		}

		appendStringInfoChar(buf, ')');
	}

	/* Copy stuff after VALUES clause from the original query */
	appendStringInfoString(buf, orig_query + values_end_len);
}
#endif

void
sqlite_deparse_analyze(StringInfo sql, char *dbname, char *relname)
{
	appendStringInfo(sql, "SELECT");
	appendStringInfo(sql, " round(((data_length + index_length)), 2)");
	appendStringInfo(sql, " FROM information_schema.TABLES");
	appendStringInfo(sql, " WHERE table_schema = '%s' AND table_name = '%s'", dbname, relname);
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists.
 */
static void
sqlite_deparse_target_list(StringInfo buf,
						   PlannerInfo *root,
						   Index rtindex,
						   Relation rel,
						   Bitmapset *attrs_used,
						   bool qualify_col,
						   List **retrieved_attrs,
						   bool is_concat,
						   bool check_null)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	first = true;
	*retrieved_attrs = NIL;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			if (!first)
			{
				if (is_concat)
					appendStringInfoString(buf, ", '') || \",\" || COALESCE(");
				else if (check_null)
					appendStringInfoString(buf, "OR ( ");
				else
					appendStringInfoString(buf, ", ");
			}
			else if (is_concat)
				appendStringInfoString(buf, "COALESCE(");
			else if (check_null)
				appendStringInfoString(buf, "( ");

			first = false;

			sqlite_deparse_column_ref(buf, rtindex, i, root, qualify_col);

			if (check_null)
				appendStringInfoString(buf, " IS NOT NULL) ");

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first)
		appendStringInfoString(buf, "NULL");
	else if (is_concat)
		appendStringInfoString(buf, ", '')");
}

/*
 * Deparse WHERE clauses in given list of RestrictInfos and append them to buf.
 *
 * baserel is the foreign table we're planning for.
 *
 * If no WHERE clause already exists in the buffer, is_first should be true.
 *
 * If params is not NULL, it receives a list of Params and other-relation Vars
 * used in the clauses; these values must be transmitted to the remote server
 * as parameter values.
 *
 * If params is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 */
void
sqlite_append_where_clause(StringInfo buf,
						   PlannerInfo *root,
						   RelOptInfo *baserel,
						   List *exprs,
						   bool is_first,
						   List **params)
{
	deparse_expr_cxt context;
	ListCell   *lc;

	if (params)
		*params = NIL;			/* initialize result list to empty */

	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = baserel;
	context.buf = buf;
	context.params_list = params;

	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (is_first)
			appendStringInfoString(buf, " WHERE ");
		else
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		sqlite_deparse_expr(ri->clause, &context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

#if PG_VERSION_NUM >= 140000
/*
 * TRUNCATE in SQLite is supported by use DELETE FROM without WHERE condition.
 */
void
sqlite_deparse_truncate(StringInfo buf,
						List *rels)
{
	ListCell   *cell;
	Relation	rel;

	appendStringInfoString(buf, "PRAGMA foreign_keys = ON;");

	foreach(cell, rels)
	{
		appendStringInfoString(buf, "DELETE FROM ");

		rel = lfirst(cell);

		sqlite_deparse_relation(buf, rel);
		appendStringInfoChar(buf, ';');
	}
}
#endif

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 */
static void
sqlite_deparse_column_ref(StringInfo buf, int varno, int varattno, PlannerInfo *root, bool qualify_col)
{
	RangeTblEntry *rte;

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);
	if (varattno == 0)
	{
		/* Whole row reference */
		Relation	rel;
		Bitmapset  *attrs_used;

		/* Required only to be passed down to deparseTargetList(). */
		List	   *retrieved_attrs;

		/*
		 * The lock on the relation will be held by upper callers, so it's
		 * fine to open it with no lock here.
		 */
		rel = table_open(rte->relid, NoLock);

		/*
		 * The local name of the foreign table can not be recognized by the
		 * foreign server and the table it references on foreign server might
		 * have different column ordering or different columns than those
		 * declared locally. Hence we have to deparse whole-row reference as
		 * ROW(columns referenced locally). Construct this by deparsing a
		 * "whole row" attribute.
		 */
		attrs_used = bms_add_member(NULL,
									0 - FirstLowInvalidHeapAttributeNumber);

		/*
		 * In case the whole-row reference is under an outer join then it has
		 * to go NULL whenever the rest of the row goes NULL. Deparsing a join
		 * query would always involve multiple relations, thus qualify_col
		 * would be true.
		 */
		appendStringInfoString(buf, "CASE WHEN ");
		sqlite_deparse_target_list(buf, root, varno, rel, attrs_used, qualify_col,
								   &retrieved_attrs, false, true);
		appendStringInfoString(buf, "THEN ");

		appendStringInfoString(buf, "(\"(\" || ");
		sqlite_deparse_target_list(buf, root, varno, rel, attrs_used, qualify_col,
								   &retrieved_attrs, true, false);
		appendStringInfoString(buf, "|| \")\")");
		appendStringInfoString(buf, " END");

		table_close(rel, NoLock);
		bms_free(attrs_used);
	}
	else
	{
		char	   *colname = NULL;
		List	   *options;
		ListCell   *lc;

		/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
		Assert(!IS_SPECIAL_VARNO(varno));

		/*
		 * If it's a column of a foreign table, and it has the column_name FDW
		 * option, use that value.
		 */
		options = GetForeignColumnOptions(rte->relid, varattno);
		foreach(lc, options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "column_name") == 0)
			{
				colname = defGetString(def);
				break;
			}
			elog(DEBUG1, "column name = %s\n", def->defname);
		}

		/*
		 * If it's a column of a regular table or it doesn't have column_name
		 * FDW option, use attribute name.
		 */
		if (colname == NULL)
#if (PG_VERSION_NUM >= 110000)
			colname = get_attname(rte->relid, varattno, false);
#else
			colname = get_attname(rte->relid, varattno);
#endif

		if (qualify_col)
			ADD_REL_QUALIFIER(buf, varno);

		appendStringInfoString(buf, sqlite_quote_identifier(colname, '"'));
	}
}

static char *
sqlite_deparse_column_option(int varno, int varattno, PlannerInfo *root, char *optionname)
{
	RangeTblEntry *rte;
	char	   *coloptionvalue = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, optionname) == 0)
		{
			coloptionvalue = defGetString(def);
			break;
		}
	}

	return coloptionvalue;
}

/*
* Append a SQL string literal representing "val" to buf.
*/
void
sqlite_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that sqlite_foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
sqlite_deparse_expr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			sqlite_deparse_var((Var *) node, context);
			break;
		case T_Const:
			sqlite_deparse_const((Const *) node, context, 0);
			break;
		case T_Param:
			sqlite_deparse_param((Param *) node, context);
			break;
		case T_FuncExpr:
			sqlite_deparse_func_expr((FuncExpr *) node, context);
			break;
		case T_OpExpr:
			sqlite_deparse_op_expr((OpExpr *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			sqlite_deparse_scalar_array_op_expr((ScalarArrayOpExpr *) node, context);
			break;
		case T_RelabelType:
			sqlite_deparse_relabel_type((RelabelType *) node, context);
			break;
		case T_BoolExpr:
			sqlite_deparse_bool_expr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			sqlite_deparse_null_test((NullTest *) node, context);
			break;
		case T_ArrayExpr:
			sqlite_deparse_array_expr((ArrayExpr *) node, context);
			break;
		case T_CaseExpr:
			sqlite_deparse_case_expr((CaseExpr *) node, context);
			break;
		case T_CoalesceExpr:
			sqlite_deparse_coalesce_expr((CoalesceExpr *) node, context);
			break;
		case T_NullIfExpr:
			sqlite_deparse_null_if_expr((NullIfExpr *) node, context);
			break;
		case T_MinMaxExpr:
			sqlite_deparse_minmax_expr((MinMaxExpr *) node, context);
			break;
		case T_Aggref:
			sqlite_deparse_aggref((Aggref *) node, context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}



/*
 * deparse remote UPDATE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
sqlite_deparse_update(StringInfo buf, PlannerInfo *root,
					  Index rtindex, Relation rel,
					  List *targetAttrs, List *attnums)
{
#if PG_VERSION_NUM >= 140000
	TupleDesc	tupdesc = RelationGetDescr(rel);
#endif
	AttrNumber	pindex;
	bool		first;
	ListCell   *lc;
	int			i;

	appendStringInfoString(buf, "UPDATE ");
	sqlite_deparse_relation(buf, rel);
	appendStringInfoString(buf, " SET ");

	pindex = 2;
	first = true;
	foreach(lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);
#if PG_VERSION_NUM >= 140000
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

		if (!attr->attgenerated)
		{
#endif
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;
			sqlite_deparse_column_ref(buf, rtindex, attnum, root, false);
			appendStringInfo(buf, " = ?");
			pindex++;
#if PG_VERSION_NUM >= 140000
		}
#endif
	}
	i = 0;
	foreach(lc, attnums)
	{
		int			attnum = lfirst_int(lc);

		appendStringInfo(buf, i == 0 ? " WHERE " : " AND ");
		sqlite_deparse_column_ref(buf, rtindex, attnum, root, false);
		appendStringInfo(buf, "=?");
		i++;
	}
}


/*
 * deparse remote UPDATE statement
 *
 * 'buf' is the output buffer to append the statement to 'rtindex' is the RT
 * index of the associated target relation 'rel' is the relation descriptor
 * for the target relation 'foreignrel' is the RelOptInfo for the target
 * relation or the join relation containing all base relations in the query
 * 'targetlist' is the tlist of the underlying foreign-scan plan node
 * 'targetAttrs' is the target columns of the UPDATE 'remote_conds' is the
 * qual clauses that must be evaluated remotely '*params_list' is an output
 * list of exprs that will become remote Params '*retrieved_attrs' is an
 * output list of integers of columns being retrieved by RETURNING (if any)
 */
void
sqlite_deparse_direct_update_sql(StringInfo buf, PlannerInfo *root,
								 Index rtindex, Relation rel,
								 RelOptInfo *foreignrel,
								 List *targetlist,
								 List *targetAttrs,
								 List *remote_conds,
								 List **params_list,
								 List **retrieved_attrs)
{
	deparse_expr_cxt context;
	int			nestlevel;
	bool		first;
	ListCell   *lc;
	ListCell   *lc2;

	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = foreignrel;
	context.scanrel = foreignrel;
	context.buf = buf;
	context.params_list = params_list;

	appendStringInfoString(buf, "UPDATE ");
	sqlite_deparse_relation(buf, rel);
	if (IS_JOIN_REL(foreignrel))
		appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, rtindex);
	appendStringInfoString(buf, " SET ");

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = sqlite_set_transmission_modes();

	first = true;
	forboth(lc, targetlist, lc2, targetAttrs)
	{
		int			attnum = lfirst_int(lc2);
		TargetEntry *tle;
#if (PG_VERSION_NUM >= 140000)
		tle = lfirst_node(TargetEntry, lc);

		/* update's new-value expressions shouldn't be resjunk */
		Assert(!tle->resjunk);
#else
		(void) lc;
		tle = get_tle_by_resno(targetlist, attnum);
#endif

		if (!tle)
			elog(ERROR, "attribute number %d not found in UPDATE targetlist",
				 attnum);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		sqlite_deparse_column_ref(buf, rtindex, attnum, root, false);
		appendStringInfoString(buf, " = ");
		sqlite_deparse_expr((Expr *) tle->expr, &context);
	}

	sqlite_reset_transmission_modes(nestlevel);

	if (IS_JOIN_REL(foreignrel))
	{
		List	   *ignore_conds = NIL;

		appendStringInfo(buf, " FROM ");
		sqlite_deparse_from_expr_for_rel(buf, root, foreignrel, true, rtindex,
										 &ignore_conds, params_list);
		remote_conds = list_concat(remote_conds, ignore_conds);
	}

	if (remote_conds)
	{
		appendStringInfoString(buf, " WHERE ");
		sqlite_append_conditions(remote_conds, &context);
	}
}

/*
 * deparse remote DELETE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
sqlite_deparse_delete(StringInfo buf, PlannerInfo *root,
					  Index rtindex, Relation rel,
					  List *attname)
{
	int			i = 0;
	ListCell   *lc;

	appendStringInfoString(buf, "DELETE FROM ");
	sqlite_deparse_relation(buf, rel);
	foreach(lc, attname)
	{
		int			attnum = lfirst_int(lc);

		appendStringInfo(buf, i == 0 ? " WHERE " : " AND ");
		sqlite_deparse_column_ref(buf, rtindex, attnum, root, false);
		appendStringInfo(buf, "=?");
		i++;
	}
}


/*
 * deparse remote DELETE statement
 *
 * 'buf' is the output buffer to append the statement to 'rtindex' is the RT
 * index of the associated target relation 'rel' is the relation descriptor
 * for the target relation 'foreignrel' is the RelOptInfo for the target
 * relation or the join relation containing all base relations in the query
 * 'remote_conds' is the qual clauses that must be evaluated remotely
 * '*params_list' is an output list of exprs that will become remote Params
 * '*retrieved_attrs' is an output list of integers of columns being
 * retrieved by RETURNING (if any)
 */
void
sqlite_deparse_direct_delete_sql(StringInfo buf, PlannerInfo *root,
								 Index rtindex, Relation rel,
								 RelOptInfo *foreignrel,
								 List *remote_conds,
								 List **params_list,
								 List **retrieved_attrs)
{
	deparse_expr_cxt context;

	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = foreignrel;
	context.scanrel = foreignrel;
	context.buf = buf;
	context.params_list = params_list;

	appendStringInfoString(buf, "DELETE FROM ");
	sqlite_deparse_relation(buf, rel);
	if (IS_JOIN_REL(foreignrel))
		appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, rtindex);

	if (IS_JOIN_REL(foreignrel))
	{
		List	   *ignore_conds = NIL;

		appendStringInfo(buf, " USING ");
		sqlite_deparse_from_expr_for_rel(buf, root, foreignrel, true, rtindex,
										 &ignore_conds, params_list);
		remote_conds = list_concat(remote_conds, ignore_conds);
	}

	if (remote_conds)
	{
		appendStringInfoString(buf, " WHERE ");
		sqlite_append_conditions(remote_conds, &context);
	}
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
sqlite_deparse_var(Var *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Relids		relids = context->scanrel->relids;
	int			relno;
	int			colno;

	/* Qualify columns when multiple relations are involved. */
	bool		qualify_col = (bms_membership(relids) == BMS_MULTIPLE);

	/*
	 * If the Var belongs to the foreign relation that is deparsed as a
	 * subquery, use the relation and column alias to the Var provided by the
	 * subquery, instead of the remote name.
	 */
	if (sqlite_is_subquery_var(node, context->scanrel, &relno, &colno))
	{
		appendStringInfo(context->buf, "%s%d.%s%d",
						 SUBQUERY_REL_ALIAS_PREFIX, relno,
						 SUBQUERY_COL_ALIAS_PREFIX, colno);
		return;
	}

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		sqlite_deparse_column_ref(buf, node->varno, node->varattno, context->root, qualify_col);
	}
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int			pindex = 0;
			ListCell   *lc;

			/* find its index in params_list */
			foreach(lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *) lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				/* not in list, so add it */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}
			sqlite_print_remote_param(pindex, node->vartype, node->vartypmod, context);
		}
		else
		{
			sqlite_print_remote_placeholder(node->vartype, node->vartypmod, context);
		}
	}
}

/*
 * With this function, we try to obtain complementary node for operation to be able
 * to obtain column name and column type to whom const value its compared to.
 * If we obtain type, we know if we need to use datetime convert expressions
 * or not depending if sqlite column is TEXT or INT */
static Var *
get_complementary_var_node(Expr *node)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
			/* Only supported case by now is T_Var complementary node */
		case T_Var:
			return (Var *) node;
			break;
		default:
			return NULL;
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 * As for that function, showtype can be -1 to never show "::typename" decoration,
 * or +1 to always show it, or 0 to show it only if the constant wouldn't be assumed
 * to be the right type by default.
 */
static void
sqlite_deparse_const(Const *node, deparse_expr_cxt *context, int showtype)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	char	   *sqlitecolumntype;
	bool		convert_timestamp_tounixepoch;
	Var		   *varnode;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	getTypeOutputInfo(node->consttype,
					  &typoutput, &typIsVarlena);

	switch (node->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			{
				extval = OidOutputFunctionCall(typoutput, node->constvalue);

				/*
				 * No need to quote unless it's a special value such as 'NaN'.
				 * See comments in get_const_expr().
				 */
				if (strspn(extval, "0123456789+-eE.") == strlen(extval))
				{
					if (extval[0] == '+' || extval[0] == '-')
						appendStringInfo(buf, "(%s)", extval);
					else
						appendStringInfoString(buf, extval);
				}
				else
					appendStringInfo(buf, "\'%s\'", extval);
			}
			break;
		case BITOID:
		case VARBITOID:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			appendStringInfo(buf, "B\'%s\'", extval);
			break;
		case BOOLOID:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "1");
			else
				appendStringInfoString(buf, "0");
			break;

		case BYTEAOID:

			/*
			 * the string for BYTEA always seems to be in the format "\\x##"
			 * where # is a hex digit, Even if the value passed in is
			 * 'hi'::bytea we will receive "\x6869". Making this assumption
			 * allows us to quickly convert postgres escaped strings to sqlite
			 * ones for comparison
			 */
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			appendStringInfo(buf, "X\'%s\'", extval + 2);
			break;
		case TIMESTAMPOID:
			convert_timestamp_tounixepoch = false;
			extval = OidOutputFunctionCall(typoutput, node->constvalue);

			if (context->complementarynode != NULL)
			{
				varnode = get_complementary_var_node(context->complementarynode);
				if (varnode != NULL)
				{
					sqlitecolumntype = sqlite_deparse_column_option(varnode->varno, varnode->varattno, context->root, "column_type");

					if (sqlitecolumntype != NULL && strcmp(sqlitecolumntype, "INT") == 0)
						convert_timestamp_tounixepoch = true;
				}
			}

			if (convert_timestamp_tounixepoch)
				appendStringInfo(buf, "strftime('%%s', '%s')", extval);
			else
				sqlite_deparse_string_literal(buf, extval);

			break;
		default:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			sqlite_deparse_string_literal(buf, extval);
			break;
	}
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list if it's not already present, and then use its index
 * in that list as the remote parameter number.  During EXPLAIN, there's
 * no need to identify a parameter number.
 */
static void
sqlite_deparse_param(Param *node, deparse_expr_cxt *context)
{
	if (context->params_list)
	{
		int			pindex = 0;
		ListCell   *lc;

		/* find its index in params_list */
		foreach(lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *) lfirst(lc)))
				break;
		}
		if (lc == NULL)
		{
			/* not in list, so add it */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		sqlite_print_remote_param(pindex, node->paramtype, node->paramtypmod, context);
	}
	else
	{
		sqlite_print_remote_placeholder(node->paramtype, node->paramtypmod, context);
	}
}

/*
 * This possible that name of function in PostgreSQL and
 * sqlite differ, so return the sqlite equelent function name
 */
static char *
sqlite_replace_function(char *in)
{
	if (strcmp(in, "btrim") == 0)
	{
		return "trim";
	}
	return in;
}

/*
 * Deparse a function call.
 */
static void
sqlite_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;
	bool		first;
	ListCell   *arg;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		sqlite_deparse_expr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * Normal function: display as proname(args).
	 */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", node->funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Translate PostgreSQL function into sqlite function */
	proname = sqlite_replace_function(NameStr(procform->proname));

	/* Deparse the function name ... */
	appendStringInfo(buf, "%s(", proname);

	/* ... and all the arguments */
	first = true;
	foreach(arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		sqlite_deparse_expr((Expr *) lfirst(arg), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
	ReleaseSysCache(proctup);
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
sqlite_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'b')
	{
		context->complementarynode = llast(node->args);

		sqlite_deparse_expr(linitial(node->args), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	sqlite_deparse_operator_name(buf, form);

	/* Deparse right operand. */
	appendStringInfoChar(buf, ' ');
	if (oprkind == 'b')
		context->complementarynode = linitial(node->args);

	sqlite_deparse_expr(llast(node->args), context);

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
sqlite_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
{
	char	   *cur_opname = NULL;

	/* opname is not a SQL identifier, so we should not quote it. */
	cur_opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		/* Print fully qualified operator name. */
		appendStringInfo(buf, "OPERATOR(%s.%s)",
						 sqlite_quote_identifier(opnspname, QUOTE), cur_opname);
	}
	else
	{
		if (strcmp(cur_opname, "~~") == 0)
		{
			appendStringInfoString(buf, "LIKE");
		}
		else if (strcmp(cur_opname, "!~~") == 0)
		{
			appendStringInfoString(buf, "NOT LIKE");
		}
		else if (strcmp(cur_opname, "~~*") == 0 ||
				 strcmp(cur_opname, "!~~*") == 0 ||
				 strcmp(cur_opname, "~") == 0 ||
				 strcmp(cur_opname, "!~") == 0 ||
				 strcmp(cur_opname, "~*") == 0 ||
				 strcmp(cur_opname, "!~*") == 0)
		{
			elog(ERROR, "OPERATOR is not supported");
		}

		else
		{
			appendStringInfoString(buf, cur_opname);
		}
	}
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
sqlite_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Expr	   *arg1;
	Expr	   *arg2;
	Form_pg_operator form;
	char	   *opname = NULL;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	bool		useIn = false;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	opname = pstrdup(NameStr(form->oprname));
	ReleaseSysCache(tuple);

	/* Using IN clause for '= ANY' and NOT IN clause for '<> ALL' */
	if ((strcmp(opname, "=") == 0 && node->useOr == true) ||
		(strcmp(opname, "<>") == 0 && node->useOr == false))
		useIn = true;

	/* Get left and right argument for deparsing */
	arg1 = linitial(node->args);
	arg2 = lsecond(node->args);

	if (useIn)
	{
		/* Deparse left operand. */
		sqlite_deparse_expr(arg1, context);
		appendStringInfoChar(buf, ' ');

		/* Add IN clause */
		if (strcmp(opname, "<>") == 0)
		{
			appendStringInfoString(buf, "NOT IN (");
		}
		else if (strcmp(opname, "=") == 0)
		{
			appendStringInfoString(buf, "IN (");
		}
	}

	switch (nodeTag((Node *) arg2))
	{
		case T_Const:
			{
				Const	   *c = (Const *) arg2;
				bool		isstr = false;
				const char *valptr;
				int			i = -1;
				bool		deparseLeft = true;

				if (!c->constisnull)
				{
					getTypeOutputInfo(c->consttype,
									  &typoutput, &typIsVarlena);
					extval = OidOutputFunctionCall(typoutput, c->constvalue);

					/* Determine array type */
					switch (c->consttype)
					{
						case INT4ARRAYOID:
						case OIDARRAYOID:
							isstr = false;
							break;
						default:
							isstr = true;
							break;
					}

					for (valptr = extval; *valptr; valptr++)
					{
						char		ch = *valptr;

						i++;

						if (useIn)
						{
							if (i == 0 && isstr)
								appendStringInfoChar(buf, '\'');
						}
						else if (deparseLeft)
						{
							/* Deparse left operand. */
							sqlite_deparse_expr(arg1, context);
							/* Append operator */
							appendStringInfo(buf, " %s ", opname);
							if (isstr)
								appendStringInfoChar(buf, '\'');
							deparseLeft = false;
						}

						/*
						 * Remove '{', '}' and \" character from the string.
						 * Because this syntax is not recognize by the remote
						 * Sqlite server.
						 */
						if ((ch == '{' && i == 0) || (ch == '}' && (i == (strlen(extval) - 1))) || ch == '\"')
							continue;

						if (ch == ',')
						{
							if (useIn)
							{
								if (isstr)
									appendStringInfoChar(buf, '\'');
								appendStringInfoChar(buf, ch);
								appendStringInfoChar(buf, ' ');
								if (isstr)
									appendStringInfoChar(buf, '\'');
							}
							else
							{
								if (isstr)
									appendStringInfoChar(buf, '\'');
								if (node->useOr)
									appendStringInfoString(buf, " OR ");
								else
									appendStringInfoString(buf, " AND ");
								deparseLeft = true;
							}
							continue;
						}
						appendStringInfoChar(buf, ch);
					}

					if (isstr)
						appendStringInfoChar(buf, '\'');
				}
				else
				{
					appendStringInfoString(buf, " NULL");
					return;
				}
			}
			break;
		case T_ArrayExpr:
			{
				bool		first = true;
				ListCell   *lc;

				foreach(lc, ((ArrayExpr *) arg2)->elements)
				{
					if (!first)
					{
						if (useIn)
						{
							appendStringInfoString(buf, ", ");
						}
						else
						{
							if (node->useOr)
								appendStringInfoString(buf, " OR ");
							else
								appendStringInfoString(buf, " AND ");
						}
					}

					if (useIn)
					{
						sqlite_deparse_expr(lfirst(lc), context);
					}
					else
					{
						/* Deparse left argument */
						appendStringInfoChar(buf, '(');
						sqlite_deparse_expr(arg1, context);

						appendStringInfo(buf, " %s ", opname);

						/* Deparse each element in right argument */
						sqlite_deparse_expr(lfirst(lc), context);
						appendStringInfoChar(buf, ')');
					}
					first = false;
				}
				break;
			}
		default:
			elog(ERROR, "unsupported expression type for deparse: %d", (int) nodeTag(node));
			break;
	}

	/* Close IN clause */
	if (useIn)
		appendStringInfoChar(buf, ')');

}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
sqlite_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	sqlite_deparse_expr(node->arg, context);
}

/*
 * Deparse a BoolExpr node.
 *
 * Note: by the time we get here, AND and OR expressions have been flattened
 * into N-argument form, so we'd better be prepared to deal with that.
 */
static void
sqlite_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfoString(buf, "(NOT ");
			sqlite_deparse_expr(linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		sqlite_deparse_expr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
sqlite_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	sqlite_deparse_expr(node->arg, context);
	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
sqlite_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoString(buf, "ARRAY[");
	foreach(lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		sqlite_deparse_expr(lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ']');
}

/*
 * Deparse given CASE expression
 */
static void
sqlite_deparse_case_expr(CaseExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc = NULL;

	appendStringInfoString(buf, "CASE ");

	/* If CASE arg WHEN then appen arg before continuing */
	if (node->arg != NULL)
		sqlite_deparse_expr(node->arg, context);

	/* Add individual cases */
	foreach(lc, node->args)
	{
		CaseWhen   *whenclause = (CaseWhen *) lfirst(lc);

		/* WHEN */
		appendStringInfoString(buf, " WHEN ");
		if (node->arg == NULL)	/* CASE WHEN */
			sqlite_deparse_expr(whenclause->expr, context);
		else					/* CASE arg WHEN */
			sqlite_deparse_expr(lsecond(((OpExpr *) whenclause->expr)->args), context);

		/* THEN */
		appendStringInfoString(buf, " THEN ");
		sqlite_deparse_expr(whenclause->result, context);
	}

	/* add ELSE if needed */
	if (node->defresult != NULL)
	{
		appendStringInfoString(buf, " ELSE ");
		sqlite_deparse_expr(node->defresult, context);
	}

	/* append END */
	appendStringInfoString(buf, " END");
}

/*
 * Deparse given NULLIF(val1, val2) expression.
 */
static void
sqlite_deparse_null_if_expr(NullIfExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoString(buf, "NULLIF(");
	sqlite_deparse_expr(lfirst(list_head(node->args)), context);
	appendStringInfoString(buf, ", ");
	sqlite_deparse_expr(lfirst(list_tail(node->args)), context);
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given COALESCE(...) expression.
 */
static void
sqlite_deparse_coalesce_expr(CoalesceExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;
	bool		first = true;

	appendStringInfoString(buf, "COALESCE(");
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		sqlite_deparse_expr(lfirst(lc), context);
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given MinMaxExpr node.
 */
static void
sqlite_deparse_minmax_expr(MinMaxExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;
	bool		first = true;

	/* Check operation accepted. */
	Assert(node->op == IS_LEAST || node->op == IS_GREATEST);

	if (node->op == IS_LEAST) {
		appendStringInfoString(buf, "LEAST(");
	}
	else {
		appendStringInfoString(buf, "GREATEST(");
	}

	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		sqlite_deparse_expr(lfirst(lc), context);
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void
sqlite_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,
						  deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfo(buf, "?");
}

static void
sqlite_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
								deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfo(buf, "(SELECT null)");
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
static bool
sqlite_is_builtin(Oid oid)
{
#if PG_VERSION_NUM >= 120000
	return (oid < FirstGenbkiObjectId);
#else
	return (oid < FirstBootstrapObjectId);
#endif
}

/*
 * Deparse an Aggref node.
 */
static void
sqlite_deparse_aggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		use_variadic;

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->aggvariadic;

	/* Find aggregate name from aggfnoid which is a pg_proc entry */
	sqlite_append_function_name(node->aggfnoid, context);
	appendStringInfoChar(buf, '(');

	/* Add DISTINCT */
	appendStringInfo(buf, "%s", (node->aggdistinct != NIL) ? "DISTINCT " : "");

	if (AGGKIND_IS_ORDERED_SET(node->aggkind))
	{
		/* Add WITHIN GROUP (ORDER BY ..) */
		ListCell   *arg;
		bool		first = true;

		Assert(!node->aggvariadic);
		Assert(node->aggorder != NIL);

		foreach(arg, node->aggdirectargs)
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			sqlite_deparse_expr((Expr *) lfirst(arg), context);
		}

		appendStringInfoString(buf, ") WITHIN GROUP (ORDER BY ");
		sqlite_append_agg_order_by(node->aggorder, node->args, context);
	}
	else
	{
		/* aggstar can be set only in zero-argument aggregates */
		if (node->aggstar)
			appendStringInfoChar(buf, '*');
		else
		{
			ListCell   *arg;
			bool		first = true;

			/* Add all the arguments */
			foreach(arg, node->args)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(arg);
				Node	   *n = (Node *) tle->expr;

				if (tle->resjunk)
					continue;

				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				/* Add VARIADIC */
#if PG_VERSION_NUM < 130000
				if (use_variadic && lnext(arg) == NULL)
#else
				if (use_variadic && lnext(node->args, arg) == NULL)
#endif
					appendStringInfoString(buf, "VARIADIC ");

				sqlite_deparse_expr((Expr *) n, context);
			}
		}

		/* Add ORDER BY */
		if (node->aggorder != NIL)
		{
			appendStringInfoString(buf, " ORDER BY ");
			sqlite_append_agg_order_by(node->aggorder, node->args, context);
		}
	}

	/* Add FILTER (WHERE ..) */
	if (node->aggfilter != NULL)
	{
		appendStringInfoString(buf, ") FILTER (WHERE ");
		sqlite_deparse_expr((Expr *) node->aggfilter, context);
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse GROUP BY clause.
 */
static void
sqlite_append_group_by_clause(List *tlist, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Query	   *query = context->root->parse;
	ListCell   *lc;
	bool		first = true;

	/* Nothing to be done, if there's no GROUP BY clause in the query. */
	if (!query->groupClause)
		return;

	appendStringInfo(buf, " GROUP BY ");

	/*
	 * Queries with grouping sets are not pushed down, so we don't expect
	 * grouping sets here.
	 */
	Assert(!query->groupingSets);

	foreach(lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *) lfirst(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		sqlite_deparse_sort_group_clause(grp->tleSortGroupRef, tlist, true, context);
	}
}


/*
 * Append ORDER BY within aggregate function.
 */
static void
sqlite_append_agg_order_by(List *orderList, List *targetList, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	ListCell   *lc;
	bool		first = true;

	foreach(lc, orderList)
	{
		SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
		Node	   *sortexpr;
		Oid			sortcoltype;
		TypeCacheEntry *typentry;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		sortexpr = sqlite_deparse_sort_group_clause(srt->tleSortGroupRef, targetList, false,
													context);
		sortcoltype = exprType(sortexpr);
		/* See whether operator is default < or > for datatype */
		typentry = lookup_type_cache(sortcoltype,
									 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
		if (srt->sortop == typentry->lt_opr)
			appendStringInfoString(buf, " ASC");
		else if (srt->sortop == typentry->gt_opr)
			appendStringInfoString(buf, " DESC");
		else
		{
			HeapTuple	opertup;
			Form_pg_operator operform;

			appendStringInfoString(buf, " USING ");

			/* Append operator name. */
			opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(srt->sortop));
			if (!HeapTupleIsValid(opertup))
				elog(ERROR, "cache lookup failed for operator %u", srt->sortop);
			operform = (Form_pg_operator) GETSTRUCT(opertup);
			sqlite_deparse_operator_name(buf, operform);
			ReleaseSysCache(opertup);
		}

		if (srt->nulls_first)
			appendStringInfoString(buf, " NULLS FIRST");
		else
			appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Deparse ORDER BY clause according to the given pathkeys for given base
 * relation. From given pathkeys expressions belonging entirely to the given
 * base relation are obtained and deparsed.
 */
static void
sqlite_append_order_by_clause(List *pathkeys, bool has_final_sort, deparse_expr_cxt *context)
{
	ListCell   *lcell;
	int			nestlevel;
	char	   *delim = " ";
	RelOptInfo *baserel = context->scanrel;
	StringInfo	buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = sqlite_set_transmission_modes();

	appendStringInfo(buf, " ORDER BY");
	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = lfirst(lcell);
		Expr	   *em_expr;
		int			sqliteVersion = sqlite3_libversion_number();

		if (has_final_sort)
		{
			/*
			 * By construction, context->foreignrel is the input relation to
			 * the final sort.
			 */
			em_expr = sqlite_find_em_expr_for_input_target(context->root,
														   pathkey->pk_eclass,
														   context->foreignrel->reltarget,
														   baserel);
		}
		else
			em_expr = sqlite_find_em_expr_for_rel(pathkey->pk_eclass, baserel);

		Assert(em_expr != NULL);

		appendStringInfoString(buf, delim);
		sqlite_deparse_expr(em_expr, context);
		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC");
		else
			appendStringInfoString(buf, " DESC");

		/*
		 * In SQLITE3 Release v3.30.0 (2019-10-04) NULLS FIRST/LAST is
		 * supported, but not in prior versions More info:
		 * https://www.sqlite.org/changes.html
		 * https://www.sqlite.org/lang_select.html#orderby
		 */
		if (sqliteVersion >= 3030000)
		{
			if (pathkey->pk_nulls_first)
				appendStringInfoString(buf, " NULLS FIRST");
			else
				appendStringInfoString(buf, " NULLS LAST");
		}
		else
		{
			/*
			 * If we need a different behaviour than SQLite default...we show
			 * warning message because NULLS FIRST/LAST is not implemented in
			 * this SQLite version.
			 */
			if (!pathkey->pk_nulls_first && pathkey->pk_strategy == BTLessStrategyNumber)
				elog(WARNING, "Current Sqlite Version (%d) does not support NULLS LAST for ORDER BY ASC, degraded emitted query to ORDER BY ASC NULLS FIRST (default sqlite behaviour).", sqliteVersion);
			else if (pathkey->pk_nulls_first && pathkey->pk_strategy != BTLessStrategyNumber)
				elog(WARNING, "Current Sqlite Version (%d) does not support NULLS FIRST for ORDER BY DESC, degraded emitted query to ORDER BY DESC NULLS LAST (default sqlite behaviour).", sqliteVersion);
		}

		delim = ", ";
	}
	sqlite_reset_transmission_modes(nestlevel);
}

/*
 * Deparse LIMIT/OFFSET clause.
 */
static void
sqlite_append_limit_clause(deparse_expr_cxt *context)
{
	PlannerInfo *root = context->root;
	StringInfo	buf = context->buf;
	int			nestlevel;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = sqlite_set_transmission_modes();

	if (root->parse->limitCount)
	{
		appendStringInfoString(buf, " LIMIT ");
		sqlite_deparse_expr((Expr *) root->parse->limitCount, context);
	}
	else
	{
		/*
		 * We add this LIMIT -1 because OFFSET by itself its not
		 * implemented/allowed in SQLite. You need to provide LIMIT *always*
		 * when using OFFSET
		 */
		appendStringInfoString(buf, " LIMIT -1");
	}

	if (root->parse->limitOffset)
	{
		appendStringInfoString(buf, " OFFSET ");
		sqlite_deparse_expr((Expr *) root->parse->limitOffset, context);
	}

	sqlite_reset_transmission_modes(nestlevel);
}

/*
 * sqlite_append_function_name
 *		Deparses function name from given function oid.
 */
static void
sqlite_append_function_name(Oid funcid, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Print schema name only if it's not pg_catalog */
	if (procform->pronamespace != PG_CATALOG_NAMESPACE)
	{
		const char *schemaname;

		schemaname = get_namespace_name(procform->pronamespace);
		appendStringInfo(buf, "%s.", quote_identifier(schemaname));
	}

	/* Always print the function name */
	proname = NameStr(procform->proname);
	appendStringInfo(buf, "%s", quote_identifier(proname));

	ReleaseSysCache(proctup);
}

/*
 * Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node *
sqlite_deparse_sort_group_clause(Index ref, List *tlist, bool force_colno, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Expr	   *expr;

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (force_colno)
	{
		/* Use column-number form when requested by caller. */
		Assert(!tle->resjunk);
		appendStringInfo(buf, "%d", tle->resno);
	}
	else if (expr && IsA(expr, Const))
	{
		/*
		 * Force a typecast here so that we don't emit something like "GROUP
		 * BY 2", which will be misconstrued as a column position rather than
		 * a constant.
		 */
		sqlite_deparse_const((Const *) expr, context, 1);
	}
	else if (!expr || IsA(expr, Var))
		sqlite_deparse_expr(expr, context);
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoString(buf, "(");
		sqlite_deparse_expr(expr, context);
		appendStringInfoString(buf, ")");
	}

	return (Node *) expr;
}


/*
 * Returns true if given Var is deparsed as a subquery output column, in
 * which case, *relno and *colno are set to the IDs for the relation and
 * column alias to the Var provided by the subquery.
 */
static bool
sqlite_is_subquery_var(Var *node, RelOptInfo *foreignrel, int *relno, int *colno)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) foreignrel->fdw_private;
	RelOptInfo *outerrel = fpinfo->outerrel;
	RelOptInfo *innerrel = fpinfo->innerrel;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	/*
	 * If the given relation isn't a join relation, it doesn't have any lower
	 * subqueries, so the Var isn't a subquery output column.
	 */
	if (!IS_JOIN_REL(foreignrel))
		return false;

	/*
	 * If the Var doesn't belong to any lower subqueries, it isn't a subquery
	 * output column.
	 */
	if (!bms_is_member(node->varno, fpinfo->lower_subquery_rels))
		return false;

	if (bms_is_member(node->varno, outerrel->relids))
	{
		/*
		 * If outer relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_outerrel_subquery)
		{
			sqlite_get_relation_column_alias_ids(node, outerrel, relno, colno);
			return true;
		}

		/* Otherwise, recurse into the outer relation. */
		return sqlite_is_subquery_var(node, outerrel, relno, colno);
	}
	else
	{
		Assert(bms_is_member(node->varno, innerrel->relids));

		/*
		 * If inner relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_innerrel_subquery)
		{
			sqlite_get_relation_column_alias_ids(node, innerrel, relno, colno);
			return true;
		}

		/* Otherwise, recurse into the inner relation. */
		return sqlite_is_subquery_var(node, innerrel, relno, colno);
	}
}


/*
 * Get the IDs for the relation and column alias to given Var belonging to
 * given relation, which are returned into *relno and *colno.
 */
static void
sqlite_get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel,
									 int *relno, int *colno)
{
	SqliteFdwRelationInfo *fpinfo = (SqliteFdwRelationInfo *) foreignrel->fdw_private;
	int			i;
	ListCell   *lc;

	/* Get the relation alias ID */
	*relno = fpinfo->relation_index;

	/* Get the column alias ID */
	i = 1;
	foreach(lc, foreignrel->reltarget->exprs)
	{
		if (equal(lfirst(lc), (Node *) node))
		{
			*colno = i;
			return;
		}
		i++;
	}

	/* Shouldn't get here */
	elog(ERROR, "unexpected expression in subquery output");
}


/*****************************************************************************
 *		Check clauses for immutable functions
 *****************************************************************************/

/*
 * contain_immutable_functions
 *	  Recursively search for immutable functions within a clause.
 *
 * Returns true if any immutable function (or operator implemented by a
 * immutable function) is found.
 *
 * We will recursively look into TargetEntry exprs.
 */
static bool
sqlite_contain_immutable_functions(Node *clause)
{
	return sqlite_contain_immutable_functions_walker(clause, NULL);
}

static bool
sqlite_contain_immutable_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	/* Check for mutable functions in node itself */
	if (nodeTag(node) == T_FuncExpr)
	{
		FuncExpr   *expr = (FuncExpr *) node;

		if (func_volatile(expr->funcid) == PROVOLATILE_IMMUTABLE)
			return true;
	}

	/*
	 * It should be safe to treat MinMaxExpr as immutable, because it will
	 * depend on a non-cross-type btree comparison function, and those should
	 * always be immutable.  Treating XmlExpr as immutable is more dubious,
	 * and treating CoerceToDomain as immutable is outright dangerous.  But we
	 * have done so historically, and changing this would probably cause more
	 * problems than it would fix.  In practice, if you have a non-immutable
	 * domain constraint you are in for pain anyhow.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node,
								 sqlite_contain_immutable_functions_walker,
								 context, 0);
	}
	return expression_tree_walker(node, sqlite_contain_immutable_functions_walker,
								  context);
}

/*
 * Returns true if given tlist is safe to evaluate on the foreign server.
 */
bool
sqlite_is_foreign_function_tlist(PlannerInfo *root,
								 RelOptInfo *baserel,
								 List *tlist)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	ListCell   *lc;
	bool		is_contain_function;

	if (!IS_SIMPLE_REL(baserel) ||
		IS_OTHER_REL(baserel))
		return false;

	/*
	 * Check that the expression consists of any immutable function.
	 */
	is_contain_function = false;
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (sqlite_contain_immutable_functions((Node *) tle->expr))
		{
			is_contain_function = true;
			break;
		}
	}

	if (!is_contain_function)
		return false;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	foreach(lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		glob_cxt.root = root;
		glob_cxt.foreignrel = baserel;
		glob_cxt.relids = baserel->relids;
		loc_cxt.collation = InvalidOid;
		loc_cxt.state = FDW_COLLATE_NONE;

		if (!sqlite_foreign_expr_walker((Node *) tle->expr, &glob_cxt, &loc_cxt))
			return false;

		/*
		 * If the expression has a valid collation that does not arise from a
		 * foreign var, the expression can not be sent over.
		 */
		if (loc_cxt.state == FDW_COLLATE_UNSAFE)
			return false;

		/*
		 * An expression which includes any mutable functions can't be sent
		 * over because its result is not stable.  For example, sending now()
		 * remote side could cause confusion from clock offsets.  Future
		 * versions might be able to make this choice with more granularity.
		 * (We check this last because it requires a lot of expensive catalog
		 * lookups.)
		 */
		if (contain_mutable_functions((Node *) tle->expr))
			return false;
	}

	/* OK for the target list with functions to evaluate on the remote server */
	return true;
}

/*
 * Examine each qual clause in input_conds, and classify them into two
 * groups, which are returned as two lists: - remote_conds contains
 * expressions that can be evaluated remotely - local_conds contains
 * expressions that can't be evaluated remotely
 */
void
sqlite_classify_conditions(PlannerInfo *root,
						   RelOptInfo *baserel,
						   List *input_conds,
						   List **remote_conds,
						   List **local_conds)
{
	ListCell   *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach(lc, input_conds)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);

		if (sqlite_is_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}
