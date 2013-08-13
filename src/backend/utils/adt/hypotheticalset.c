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
	int nargs = PG_NARGS();
	int i;
	int64 rank = 1;

	AggSetGetSortInfo(fcinfo, &sorter, &tupdesc, &slot, &datumtype);

#if 0
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

	{
		AttrNumber *colidx;
		Oid *ops;
		int nsort;

		nsort = AggSetGetSortOperators(fcinfo, &colidx, &ops, NULL, NULL, NULL);

		for (i = 0; i < nsort; ++i)
			elog(NOTICE,"sort col %d idx %d op %u", i, (int) colidx[i], (unsigned) ops[i]);
	}
#endif

	/* Sanity-check args. */

	if (!tupdesc
		|| (nargs + 1) != tupdesc->natts
		|| tupdesc->attrs[nargs]->atttypid != BOOLOID)
		elog(ERROR, "type mismatch in rank()");

	for (i = 0; i < nargs; ++i)
		if (get_fn_expr_argtype(fcinfo->flinfo,i) != tupdesc->attrs[i]->atttypid)
			elog(ERROR, "type mismatch in rank()");

	/* insert the hypothetical row into the sort */

	ExecClearTuple(slot);
	for (i = 0; i < nargs; ++i)
	{
		slot->tts_values[i] = PG_GETARG_DATUM(i);
		slot->tts_isnull[i] = PG_ARGISNULL(i);
	}
	slot->tts_values[nargs] = BoolGetDatum(true);
	slot->tts_isnull[nargs] = false;
	ExecStoreVirtualTuple(slot);

	tuplesort_puttupleslot(sorter, slot);

	tuplesort_performsort(sorter);

	while (tuplesort_gettupleslot(sorter, true, slot))
	{
		bool isnull;
		Datum d = slot_getattr(slot, nargs + 1, &isnull);

		if (!isnull && DatumGetBool(d))
			break;

		++rank;
	}

	PG_RETURN_INT64(rank);
}
