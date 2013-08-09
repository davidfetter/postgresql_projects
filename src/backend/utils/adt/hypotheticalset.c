/*-------------------------------------------------------------------------
 *
 * hypotheticalset.c
 *	  Hypothetical set functions.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/hypotheticalset.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include <string.h>
#include <math.h>

#include "utils/tuplesort.h"
#include "catalog/pg_type.h"
#include "utils/datetime.h"
#include "utils/builtins.h"

/*
 * rank(float8)  - discrete (nearest) percentile
 */

Datum hypothetical_rank_final(PG_FUNCTION_ARGS);

Datum
hypothetical_rank_final(PG_FUNCTION_ARGS)
{
	Tuplesortstate *sorter = NULL;
	TupleDesc tupdesc = NULL;
    TupleTableSlot *slot = NULL;
	Oid datumtype = InvalidOid;
	int i;

	AggSetGetSortInfo(fcinfo, &sorter, &tupdesc, &slot, &datumtype);

	for (i = 0; i < PG_NARGS(); ++i)
	{
		elog(NOTICE,"arg %d type %s", i+1, format_type_be(get_fn_expr_argtype(fcinfo->flinfo, i)));
	}

	if (!tupdesc)
	{
		elog(NOTICE,"sort col 1 type %s", format_type_be(datumtype));
	}
	else
	{
		for (i = 0; i < tupdesc->natts; ++i)
		{
			elog(NOTICE,"sort col %d type %s", i+1, format_type_be(tupdesc->attrs[i]->atttypid));
		}
	}

	elog(WARNING,"Not implemented yet");
	PG_RETURN_NULL();
}
