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
#include "utils/datetime.h"

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
	float8 percentile = PG_GETARG_FLOAT8(0);
	int64 rowcount = AggSetGetRowCount(fcinfo);
	Tuplesortstate *sorter;
	Oid datumtype;
	Datum val;
	Datum first_row;
	Datum second_row;
	float8 first_row_val;
	float8 second_row_val;
	float8 proportion;
	bool isnull;
	int64 skiprows;
	int64 lower_row = 0;
	int64 higher_row = 0;

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);

	Assert(datumtype == FLOAT8OID);

	if (percentile < 0 || percentile > 1)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1", percentile)));

	if (rowcount < 1)
		PG_RETURN_NULL();

	tuplesort_performsort(sorter);

	lower_row = floor(percentile * (rowcount -1));
	higher_row = ceil(percentile * (rowcount -1));

	Assert(lower_row < rowcount);

	for (skiprows = lower_row; skiprows > 0; --skiprows)
		if (!tuplesort_getdatum(sorter, true, NULL, NULL))
			elog(ERROR,"missing row in percentile_cont");

	if (!tuplesort_getdatum(sorter, true, &first_row, &isnull))
			elog(ERROR,"missing row in percentile_cont");

	if (lower_row == higher_row)
	{

		val = first_row;
	}
	else
	{
		if (!tuplesort_getdatum(sorter, true, &second_row, &isnull))
			elog(ERROR,"missing row in percentile_cont");

		if (isnull)
			PG_RETURN_NULL();

		proportion = (percentile * (rowcount-1)) - lower_row;
		first_row_val = DatumGetFloat8(first_row);
		second_row_val = DatumGetFloat8(second_row); 
		val = Float8GetDatum(first_row_val + (proportion *(second_row_val - first_row_val)));
	}

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}
/*
 * percentile_interval_cont(Interval)  - continuous (nearest) percentile for Interval
 */
Datum percentile_interval_cont(PG_FUNCTION_ARGS);

Datum
percentile_interval_cont(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented yet");
}

Datum percentile_interval_cont_final(PG_FUNCTION_ARGS);

Datum
percentile_interval_cont_final(PG_FUNCTION_ARGS)
{
	float8 percentile = PG_GETARG_FLOAT8(0);
	int64 rowcount = AggSetGetRowCount(fcinfo);
	Tuplesortstate *sorter;
	Oid datumtype;
	Datum val;
	Datum first_row;
	Datum second_row;
	float8 proportion;
	bool isnull;
	int64 skiprows;
	int64 lower_row = 0;
	int64 higher_row = 0;
	Datum diff_result;
	Datum mul_result;
	Datum add_result;

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);

	Assert(datumtype == INTERVALOID);

	if (percentile < 0 || percentile > 1)
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g is not between 0 and 1", percentile)));

	if (rowcount < 1)
		PG_RETURN_NULL();

	tuplesort_performsort(sorter);

	lower_row = floor(percentile * (rowcount -1));
	higher_row = ceil(percentile * (rowcount -1));

	Assert(lower_row < rowcount);

	for (skiprows = lower_row; skiprows > 0; --skiprows)
		if (!tuplesort_getdatum(sorter, true, NULL, NULL))
			elog(ERROR,"missing row in percentile_cont");

	if (!tuplesort_getdatum(sorter, true, &first_row, &isnull))
			elog(ERROR,"missing row in percentile_cont");

	if (lower_row == higher_row)
	{

		val = first_row;
	}
	else
	{
		if (!tuplesort_getdatum(sorter, true, &second_row, &isnull))
			elog(ERROR,"missing row in percentile_cont");

		if (isnull)
			PG_RETURN_NULL();

		proportion = (percentile * (rowcount-1)) - lower_row;

		diff_result = DirectFunctionCall2(interval_mi, second_row, IntervalPGetDatum(first_row));
		mul_result = DirectFunctionCall2(interval_mul, diff_result, Float8GetDatumFast(proportion));
		add_result = DirectFunctionCall2(interval_pl, mul_result, first_row);
		val = add_result;
	}

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}


