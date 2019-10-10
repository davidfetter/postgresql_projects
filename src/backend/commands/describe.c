/*--------------------------------------------------------------------
 * describe.h
 *
 * Support for the DESCRIBE command.
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 * Written by David Fetter <david@fetter.org>
 *
 * IDENTIFICATION
 *	 src/backend/commands/describe.c
 *
 *--------------------------------------------------------------------
 */

#include "postgres.h"
#include "strings.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "commands/describe.h"
#include "executor/executor.h"
#include "tcop/dest.h"
#include "utils/syscache.h"

#include "executor/spi.h"
#include "utils/builtins.h"

void
GetPGObject(ObjectType objtype, List *object_name, DestReceiver *dest)
{
	int				i;
	TupOutputState *tstate;
	TupleDesc		tupdesc;

	switch(objtype)
	{
		case OBJECT_TABLE:
			{
				Datum			values[4];
				bool			isnull[4] = {false, false, false, false};
				const char *sql = "SELECT\n"
					"    a.attname,\n"
					"    pg_catalog.format_type(a.atttypid, a.atttypmod),\n"
					"    a.attnotnull,\n"
					"    pg_catalog.col_description(a.attrelid, a.attnum)\n"
					"FROM\n"
					"    pg_catalog.pg_class c\n"
					"JOIN\n"
					"    pg_catalog.pg_attribute a\n"
					"    ON (\n"
					"        a.attnum > 0 AND\n"
					"        NOT a.attisdropped AND\n"
					"        a.attrelid = c.oid AND\n"
					"        c.relname = $1\n"
					"    )\n"
					"ORDER BY a.attnum\n";
				/*
				 * Loop over the results
				tstate = begin_tup_output_tupdesc(dest, GetPGObjectResultDesc(objtype), &TTSOpsVirtual);
				while()
				{
					do_tup_output(tstate, values, isnull);
				}
				end_tup_output;
				*/
			}
			break;
		default:
			/* Error out with "not implemented" */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DESCRIBE of this type not yet implemented")));
			break;
	}
}

TupleDesc
GetPGObjectResultDesc(ObjectType objtype)
{
	TupleDesc	tupdesc;

	switch(objtype)
	{
		case OBJECT_TABLE:
			/* Need a tuple descriptor representing eight columns */
			tupdesc = CreateTemplateTupleDesc(4);
			TupleDescInitEntry(tupdesc, (AttrNumber) 1, "Column", TEXTOID, -1, 0);
			TupleDescInitEntry(tupdesc, (AttrNumber) 2, "Type", TEXTOID, -1, 0);
			TupleDescInitEntry(tupdesc, (AttrNumber) 3, "Nullable", BOOLOID, -1, 0);
			TupleDescInitEntry(tupdesc, (AttrNumber) 4, "Description", TEXTOID, -1, 0);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("DESCRIBE of this type not yet implemented")));
			break;
	}
	return tupdesc;
}
