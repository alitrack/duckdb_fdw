/*-------------------------------------------------------------------------
 * 
 * DuckDB Foreign Data Wrapper for PostgreSQL
 * 
 * IDENTIFICATION
 *        option.c
 * 
 *------------------------------------------------------------------------*/

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
 * Option context structure
 */
struct DuckDBFdwOption
{
	const char *defname;
	Oid			optcontext;
};

/*
 * Valid options
 */
static struct DuckDBFdwOption valid_options[] =
{
	/* Connection options */
	{"database", ForeignServerRelationId},
    
    /* S3 / Cloud Credentials */
    {"s3_region", ForeignServerRelationId},
    {"s3_access_key_id", ForeignServerRelationId},
    {"s3_secret_access_key", ForeignServerRelationId},
    {"s3_endpoint", ForeignServerRelationId},
    {"s3_endpoint_type", ForeignServerRelationId}, /* e.g. 's3_tables' */
    {"s3_use_ssl", ForeignServerRelationId},

    /* S3 credentials can also be set per-user via USER MAPPING (preferred
     * for security — pg_foreign_server options are visible to all users
     * with SELECT on pg_foreign_server, which is public by default.) */
    {"s3_access_key_id", UserMappingRelationId},
    {"s3_secret_access_key", UserMappingRelationId},

    /* MotherDuck token */
    {"motherduck_token", ForeignServerRelationId},
    {"motherduck_token", UserMappingRelationId},
    
    /* Catalogs */
    {"attach_catalogs", ForeignServerRelationId}, /* format: 'name=uri;type iceberg' */

    /* Extensions */
    {"extensions", ForeignServerRelationId}, /* e.g., 'httpfs,spatial,iceberg' */

	/* Table options */
	{"table", ForeignTableRelationId},
    {"read_parquet", ForeignTableRelationId}, /* Path to parquet file */
	
    /* Execution options */
	{"use_remote_estimate", ForeignServerRelationId},
	
	{NULL, InvalidOid}
};

/*
 * Check if the option is valid
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
 * FDW Option Validator
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
 * Extract FDW options into a struct
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
		FlushErrorState();
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

	/* If table name is not specified, use Postgres relation name */
	if (!opt->svr_table && f_table)
		opt->svr_table = get_rel_name(foreignoid);

	return opt;
}
