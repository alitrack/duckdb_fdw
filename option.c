/*-------------------------------------------------------------------------
 * 
 * DuckDB Foreign Data Wrapper for PostgreSQL
 * 
 * IDENTIFICATION
 *        option.c
 * 
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "duckdb_fdw.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * 选项上下文结构体
 */
struct DuckDBFdwOption
{
	const char *defname;
	Oid			optcontext;
};

static struct DuckDBFdwOption valid_options[] =
{
	{"database", ForeignServerRelationId},
	{"table", ForeignTableRelationId},
	{"use_remote_estimate", ForeignServerRelationId},
	{NULL, InvalidOid}
};

/*
 * 检查选项是否有效
 */
static bool
duckdb_is_valid_option(const char *option, Oid context)
{
	struct DuckDBFdwOption *opt;
	for (opt = valid_options; opt->defname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->defname, option) == 0)
			return true;
	}
	return false;
}

/*
 * FDW 选项验证器
 */
PG_FUNCTION_INFO_V1(duckdb_fdw_validator);
Datum
duckdb_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *lc;

	foreach(lc, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (!duckdb_is_valid_option(def->defname, catalog))
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
		}
	}
	PG_RETURN_VOID();
}

/*
 * 提取 FDW 选项
 */
duckdb_opt *
duckdb_get_options(Oid foreignoid)
{
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	List	   *options;
	ListCell   *lc;
	duckdb_opt *opt;

	opt = (duckdb_opt *) palloc0(sizeof(duckdb_opt));

	PG_TRY();
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	PG_CATCH();
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}
	PG_END_TRY();

	options = NIL;
	if (f_table)
		options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		if (strcmp(def->defname, "database") == 0)
			opt->svr_database = defGetString(def);
		else if (strcmp(def->defname, "table") == 0)
			opt->svr_table = defGetString(def);
		else if (strcmp(def->defname, "use_remote_estimate") == 0)
			opt->use_remote_estimate = defGetBoolean(def);
	}

	/* 如果没有显式指定表名，使用 Postgres 本地表名 */
	if (!opt->svr_table && f_table)
		opt->svr_table = get_rel_name(foreignoid);

	return opt;
}