/*-------------------------------------------------------------------------
 *
 * DuckDB Foreign Data Wrapper for PostgreSQL
 *
 * Portions Copyright (c) 2018, TOSHIBA CORPORATION
 *
 * IDENTIFICATION
 * 		sqlite_query.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "duckdb_fdw.h"

#include <stdio.h>

#include <sqlite3.h>

#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/fd.h"
#include "catalog/pg_type.h"

static int32
			sqlite_from_pgtyp(Oid pgtyp);

/*
 * convert_sqlite_to_pg: Convert Sqlite data into PostgreSQL's compatible data types
 */
Datum
sqlite_convert_to_pg(Oid pgtyp, int pgtypmod, sqlite3_stmt * stmt, int attnum, AttInMetadata *attinmeta)
{
	Datum		value_datum = 0;
	Datum		valueDatum = 0;
	regproc		typeinput;
	HeapTuple	tuple;
	int			typemod;
	int			col_type;
	int			sqlite_type;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type%u", pgtyp);

	typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
	typemod = ((Form_pg_type) GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);

	sqlite_type = sqlite_from_pgtyp(pgtyp);
	col_type = sqlite3_column_type(stmt, attnum);

	if (sqlite_type != col_type && col_type == SQLITE3_TEXT)
		elog(ERROR, "invalid input syntax for type =%d, column type =%d", sqlite_type, col_type);

	switch (pgtyp)
	{
			/*
			 * Sqlite gives BIT / BIT(n) data type as decimal value. The only
			 * way to retrieve this value is to use BIN, OCT or HEX function
			 * in Sqlite, otherwise sqlite client shows the actual decimal
			 * value, which could be a non - printable character. For exmple
			 * in Sqlite
			 *
			 * CREATE TABLE t (b BIT(8)); INSERT INTO t SET b = b'1001';
			 * SELECT BIN(b) FROM t; +--------+ | BIN(b) | +--------+ | 1001 |
			 * +--------+
			 *
			 * PostgreSQL expacts all binary data to be composed of either '0'
			 * or '1'. Sqlite gives value 9 hence PostgreSQL reports error.
			 * The solution is to convert the decimal number into equivalent
			 * binary string.
			 */

		case BYTEAOID:
			{
				int			blobsize = sqlite3_column_bytes(stmt, attnum);

				value_datum = (Datum) palloc0(blobsize + VARHDRSZ);
				memcpy(VARDATA(value_datum), sqlite3_column_blob(stmt, attnum), blobsize);
				SET_VARSIZE(value_datum, blobsize + VARHDRSZ);
				return PointerGetDatum(value_datum);
			}
		case INT2OID:
			{
				int			value = sqlite3_column_int(stmt, attnum);

				return Int16GetDatum(value);
			}
		case INT4OID:
			{
				int			value = sqlite3_column_int(stmt, attnum);

				return Int32GetDatum(value);
			}
		case INT8OID:
			{
				sqlite3_int64 value = sqlite3_column_int64(stmt, attnum);

				return Int64GetDatum(value);
			}
		case FLOAT4OID:
			{
				double		value = sqlite3_column_double(stmt, attnum);

				return Float4GetDatum((float4) value);
				break;
			}
		case FLOAT8OID:
			{
				double		value = sqlite3_column_double(stmt, attnum);

				return Float8GetDatum((float8) value);
				break;
			}
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case NAMEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
			{
				/*
				 * We add this conversion to allow add INTEGER/FLOAT SQLite
				 * Columns be added as TimeStamp in PostgreSQL. We just
				 * calling PostgreSQL function "to_timestamp(double value)""
				 * to convert each registry returned from INT/FLOAT value to
				 * TimeStamp string, so PosgtreSQL can handle/show without
				 * problems. If it's a TEXT SQLite column...we let them to the
				 * "regular" process because its already implemented and
				 * working properly.
				 */
				int			sqlitetype = sqlite3_column_type(stmt, attnum);

				if (sqlitetype == SQLITE_INTEGER || sqlitetype == SQLITE_FLOAT)
				{
					double		value = sqlite3_column_double(stmt, attnum);

					return DirectFunctionCall1(float8_timestamptz, Float8GetDatum((float8) value));
				}
				else
				{
					valueDatum = CStringGetDatum((char *) sqlite3_column_text(stmt, attnum));
					return OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(InvalidOid), Int32GetDatum(typemod));
				}
				break;
			}
		case NUMERICOID:
			{
				double		value = sqlite3_column_double(stmt, attnum);

				valueDatum = CStringGetDatum((char *) DirectFunctionCall1(float8out, Float8GetDatum((float8) value)));
				return OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(InvalidOid), Int32GetDatum(typemod));
			}
		default:
			valueDatum = CStringGetDatum((char *) sqlite3_column_text(stmt, attnum));
	}
	/* convert string value to appropriate type value */
	value_datum = InputFunctionCall(&attinmeta->attinfuncs[attnum],
									(char *) valueDatum,
									attinmeta->attioparams[attnum],
									attinmeta->atttypmods[attnum]);
	return value_datum;
}

/*
 * bind_sql_var:
 * Bind the values provided as DatumBind the values and nulls to modify the target table (INSERT/UPDATE)
 */
void
sqlite_bind_sql_var(Oid type, int attnum, Datum value, sqlite3_stmt * stmt, bool *isnull)
{
	int			ret = SQLITE_OK;

	attnum++;
	elog(DEBUG2, "duckdb_fdw : %s %d type=%u ", __func__, attnum, type);

	if (*isnull)
	{
		ret = sqlite3_bind_null(stmt, attnum);
		if (ret != SQLITE_OK)
			elog(ERROR, "sqlite3_bind_null failed with rc=%d", ret);
		return;
	}

	switch (type)
	{
		case INT2OID:
			{
				int16		dat = DatumGetInt16(value);

				ret = sqlite3_bind_int(stmt, attnum, dat);
				break;
			}
		case INT4OID:
			{
				int32		dat = DatumGetInt32(value);

				ret = sqlite3_bind_int(stmt, attnum, dat);
				break;
			}
		case INT8OID:
			{
				int64		dat = DatumGetInt64(value);

				ret = sqlite3_bind_int64(stmt, attnum, dat);
				break;
			}

		case FLOAT4OID:

			{
				float4		dat = DatumGetFloat4(value);

				ret = sqlite3_bind_double(stmt, attnum, (double) dat);
				break;
			}
		case FLOAT8OID:
			{
				float8		dat = DatumGetFloat8(value);

				ret = sqlite3_bind_double(stmt, attnum, dat);
				break;
			}

		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8, value);
				float8		dat = DatumGetFloat8(valueDatum);

				ret = sqlite3_bind_double(stmt, attnum, dat);
				break;
			}
		case BOOLOID:
			{
				int32		dat = DatumGetInt32(value);

				ret = sqlite3_bind_int(stmt, attnum, dat);
				break;
			}

		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case NAMEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
			{
				/* Bind as text because SQLite does not have these types */
				char	   *outputString = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);
				ret = sqlite3_bind_text(stmt, attnum, outputString, -1, SQLITE_TRANSIENT);
				break;
			}
		case BYTEAOID:
			{
				int			len;
				char	   *dat = NULL;
				char	   *result = DatumGetPointer(value);

				if (VARATT_IS_1B(result))
				{
					len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					dat = VARDATA_1B(result);
				}
				else
				{
					len = VARSIZE_4B(result) - VARHDRSZ;
					dat = VARDATA_4B(result);
				}
				ret = sqlite3_bind_blob(stmt, attnum, dat, len, SQLITE_TRANSIENT);
				break;
			}

		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("cannot convert constant value to Sqlite value %u", type),
								errhint("Constant value data type: %u", type)));
				break;
			}
	}
	if (ret != SQLITE_OK)
		ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						errmsg("Can't convert constant value to Sqlite: %s",
							   sqlite3_errmsg(sqlite3_db_handle(stmt))),
						errhint("Constant value data type: %u", type)));

}

/*
 * Give SQLite data type from PG type
 */
static int32
sqlite_from_pgtyp(Oid type)
{
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case BOOLOID:
			return SQLITE_INTEGER;
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			return SQLITE_FLOAT;
		case BYTEAOID:
			return SQLITE_BLOB;
		default:
			return SQLITE3_TEXT;
	}
}
