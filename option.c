/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2021, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 *        option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "duckdb_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
/*
 * Describes the valid options for objects that use this wrapper.
 */
struct SqliteFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};


/*
 * Valid options for duckdb_fdw.
 *
 */
static struct SqliteFdwOption valid_options[] =
{
	{"table", ForeignTableRelationId},
	{"database", ForeignServerRelationId},
	{"key", AttributeRelationId},
	{"column_name", AttributeRelationId},
	{"column_type", AttributeRelationId},
	/* truncatable is available on both server and table */
	{"truncatable", ForeignServerRelationId},
	{"truncatable", ForeignTableRelationId},
	{"keep_connections", ForeignServerRelationId},
	/* batch_size is available on both server and table */
	{"batch_size", ForeignServerRelationId},
	{"batch_size", ForeignTableRelationId},
	/* Sentinel */
	{NULL, InvalidOid}
};

extern PGDLLEXPORT Datum duckdb_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(duckdb_fdw_validator);
bool
			sqlite_is_valid_option(const char *option, Oid context);

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
duckdb_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	/*
	 * Check that only options supported by duckdb_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!sqlite_is_valid_option(def->defname, catalog))
		{
			struct SqliteFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
					 ));
		}

		/* Validate option value */
		if (strcmp(def->defname, "truncatable") == 0 ||
			strcmp(def->defname, "keep_connections") == 0)
		{
			defGetBoolean(def);
		}
		else if (strcmp(def->defname, "batch_size") == 0)
		{
			char	   *value;
			int			int_val;
			bool		is_parsed;

			value = defGetString(def);
			is_parsed = parse_int(value, &int_val, 0, NULL);

			if (!is_parsed)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for integer option \"%s\": %s",
								def->defname, value)));

			if (int_val <= 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"%s\" must be an integer value greater than zero",
								def->defname)));
		}
	}
	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
bool
sqlite_is_valid_option(const char *option, Oid context)
{
	struct SqliteFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a duckdb_fdw foreign table.
 */
sqlite_opt *
sqlite_get_options(Oid foreignoid)
{
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	List	   *options;
	ListCell   *lc;
	sqlite_opt *opt;

	opt = (sqlite_opt *) palloc(sizeof(sqlite_opt));
	memset(opt, 0, sizeof(sqlite_opt));

	/*
	 * Extract options from FDW objects.
	 */
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

	opt->use_remote_estimate = false;

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "database") == 0)
			opt->svr_database = defGetString(def);
		if (strcmp(def->defname, "table") == 0)
			opt->svr_table = defGetString(def);
	}

	if (!opt->svr_table && f_table)
		opt->svr_table = get_rel_name(foreignoid);

	return opt;
}
