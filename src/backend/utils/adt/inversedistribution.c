/*-------------------------------------------------------------------------
 *
 * inversedistribution.c
 *	  Inverse distribution functions.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/inversedistribution.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include <string.h>
#include <math.h>

#include "utils/tuplesort.h"
#include "catalog/pg_type.h"

/*
 * percentile_disc(float8)  - discrete (nearest) percentile
 */

Datum percentile_disc(PG_FUNCTION_ARGS);

Datum
percentile_disc(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented yet");
}

Datum percentile_disc_final(PG_FUNCTION_ARGS);

Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
	float8 percentile = PG_GETARG_FLOAT8(0);
	int64 rowcount = AggSetGetRowCount(fcinfo);
	Tuplesortstate *sorter;
	Oid datumtype;
	Datum val;
	bool isnull;
	int64 skiprows;

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);

	Assert(datumtype == FLOAT8OID);

	if (percentile < 0 || percentile > 1)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1", percentile)));

	if (rowcount < 1)
		PG_RETURN_NULL();

	tuplesort_performsort(sorter);

	/*
	 * percentile_disc is defined in terms of cume_dist(), but this seems
	 * to be entirely superfluous. Just counting rows instead should give
	 * the same result in all cases (even in the presence of peer rows).
	 *
	 * We need the smallest K such that (K/N) >= percentile. K starts at 1.
	 * Therefore K >= N*percentile
	 * Therefore K = ceil(N*percentile)
	 * So we skip K-1 rows (if K>0) and return the next row fetched.
	 */

	skiprows = (int64) ceil(percentile * rowcount);
	Assert(skiprows <= rowcount);

	while (--skiprows > 0)
		if (!tuplesort_getdatum(sorter, true, NULL, NULL))
			elog(ERROR,"missing row in percentile_disc");

	if (!tuplesort_getdatum(sorter, true, &val, &isnull))
		elog(ERROR,"missing row in percentile_disc");

	/* if float8 is by-ref, "val" already points to palloced space in
	 * our memory context. No need to do more.
	 */

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}

/*
 * percentile_cont(float8)  - continuous (nearest) percentile
 */
Datum percentile_cont(PG_FUNCTION_ARGS);

Datum
percentile_cont(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented yet");
}

Datum percentile_cont_final(PG_FUNCTION_ARGS);

Datum
percentile_cont_final(PG_FUNCTION_ARGS)
{
	text *test_text;
	elog(WARNING,"test percentile_cont");

	test_text = PG_GETARG_TEXT_P(0);
	PG_RETURN_TEXT_P(test_text);
}
