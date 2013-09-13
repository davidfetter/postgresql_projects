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
#include "utils/lsyscache.h"
#include "utils/array.h"

/*
 * percentile_disc(float8)  - discrete percentile
 */

Datum percentile_disc_final(PG_FUNCTION_ARGS);

Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
	float8		percentile;
	Tuplesortstate *sorter;
	Oid			datumtype;
	Datum		val;
	bool		isnull;
	int64		skiprows;
	int64		rowcount   = AggSetGetRowCount(fcinfo);

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	percentile = PG_GETARG_FLOAT8(0);

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);

	if (percentile < 0 || percentile > 1 || isnan(percentile))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g must be between 0 and 1", percentile)));

	if (rowcount < 1)
		PG_RETURN_NULL();

	tuplesort_performsort(sorter);

	/*
	 * We need the smallest K such that (K/N) >= percentile. K starts at 1.
	 * Therefore K >= N*percentile
	 * Therefore K = ceil(N*percentile)
	 * So we skip K-1 rows (if K>0) and return the next row fetched.
	 *
	 * We don't actually expect to see nulls in the input, our strict flag
	 * should have filtered them out, but we're required to not crash if
	 * there is one.
	 */

	skiprows = (int64) ceil(percentile * rowcount);
	Assert(skiprows <= rowcount);

	while (--skiprows > 0)
		if (!tuplesort_getdatum(sorter, true, NULL, NULL))
			elog(ERROR,"missing row in percentile_disc");

	if (!tuplesort_getdatum(sorter, true, &val, &isnull))
		elog(ERROR,"missing row in percentile_disc");

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}


/*
 * For percentile_cont, we need a way to interpolate between consecutive
 * values. Use a helper function for that, so that we can share the rest
 * of the code between types.
 */

static Datum float8_lerp(Datum lo, Datum hi, float8 pct)
{
	float8 loval = DatumGetFloat8(lo);
	float8 hival = DatumGetFloat8(hi); 
	return Float8GetDatum(loval + (pct * (hival - loval)));
}

static Datum interval_lerp(Datum lo, Datum hi, float8 pct)
{
	Datum diff_result = DirectFunctionCall2(interval_mi, hi, lo);
	Datum mul_result  = DirectFunctionCall2(interval_mul,
											diff_result,
											Float8GetDatumFast(pct));
	return DirectFunctionCall2(interval_pl, mul_result, lo);
}

typedef Datum (*LerpFunc)(Datum lo, Datum hi, float8 pct);

static Datum
percentile_cont_final_common(FunctionCallInfo fcinfo,
							 Oid expect_type,
							 LerpFunc lerpfunc)
{
	float8		percentile;
	int64		rowcount = AggSetGetRowCount(fcinfo);
	Tuplesortstate *sorter;
	Oid			datumtype;
	Datum		val;
	Datum		first_row;
	Datum		second_row;
	float8		proportion;
	bool		isnull;
	int64		skiprows;
	int64		lower_row = 0;
	int64		higher_row = 0;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	percentile = PG_GETARG_FLOAT8(0);

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);

	Assert(datumtype == expect_type);

	if (percentile < 0 || percentile > 1 || isnan(percentile))
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("percentile value %g must be between 0 and 1", percentile)));

	if (rowcount < 1)
		PG_RETURN_NULL();

	tuplesort_performsort(sorter);

	lower_row = floor(percentile * (rowcount - 1));
	higher_row = ceil(percentile * (rowcount - 1));

	Assert(lower_row < rowcount);

	for (skiprows = lower_row; skiprows > 0; --skiprows)
		if (!tuplesort_getdatum(sorter, true, NULL, NULL))
			elog(ERROR,"missing row in percentile_cont");

	if (!tuplesort_getdatum(sorter, true, &first_row, &isnull))
		elog(ERROR,"missing row in percentile_cont");
	if (isnull)
		PG_RETURN_NULL();

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
		val = lerpfunc(first_row, second_row, proportion);
	}

	if (isnull)
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(val);
}



/*
 * percentile_cont(float8)  - continuous percentile
 */

Datum percentile_cont_float8_final(PG_FUNCTION_ARGS);
Datum percentile_cont_interval_final(PG_FUNCTION_ARGS);

Datum
percentile_cont_float8_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, FLOAT8OID, float8_lerp);
}

/*
 * percentile_interval_cont(Interval)  - continuous percentile for Interval
 */

Datum
percentile_cont_interval_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_final_common(fcinfo, INTERVALOID, interval_lerp);
}


/*
 * mode() - most common value
 */

Datum mode_final(PG_FUNCTION_ARGS);

Datum
mode_final(PG_FUNCTION_ARGS)
{
	Tuplesortstate *sorter;
	Oid			datumtype;
	bool		isnull;
	Datum		val;
	Datum		last_val;
	bool		last_val_is_mode = false;
	int64		val_freq = 0;
	Datum		mode_val;
	int64		mode_freq = 0;
	FmgrInfo   *equalfn;
	bool		shouldfree;

	struct mode_type_info {
		Oid typid;
		int16 typLen;
		bool typByVal;
	} *typinfo = fcinfo->flinfo->fn_extra;

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);
	AggSetGetDistinctInfo(fcinfo, NULL, NULL, &equalfn);

	if (!typinfo || typinfo->typid != datumtype)
	{
		if (typinfo)
			pfree(typinfo);
		typinfo = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
									 sizeof(struct mode_type_info));
		typinfo->typid = datumtype;
		get_typlenbyval(datumtype, &typinfo->typLen, &typinfo->typByVal);
	}

	shouldfree = !(typinfo->typByVal);

	tuplesort_performsort(sorter);

	while (tuplesort_getdatum(sorter, true, &val, &isnull))
	{
		if (isnull)
			continue;

		if (val_freq == 0)
		{
			/* first value - assume modal until shown otherwise */
			mode_val = last_val = val;
			mode_freq = val_freq = 1;
			last_val_is_mode = true;
		}
		else if (DatumGetBool(FunctionCall2(equalfn, val, last_val)))
		{
			/* value equal to previous value */
			if (last_val_is_mode)
				++mode_freq;
			else if (++val_freq > mode_freq)
			{
				if (shouldfree)
				{
					pfree(DatumGetPointer(mode_val));
					pfree(DatumGetPointer(val));
				}

				mode_val = last_val;
				mode_freq = val_freq;
				last_val_is_mode = true;
			}
			else if (shouldfree)
				pfree(DatumGetPointer(val));
		}
		else
		{
			if (shouldfree && !last_val_is_mode)
				pfree(DatumGetPointer(last_val));

			last_val_is_mode = false;
			last_val = val;
			val_freq = 1;
		}
	}

	if (shouldfree && !last_val_is_mode)
		pfree(DatumGetPointer(last_val));

	if (mode_freq)
		PG_RETURN_DATUM(mode_val);
	else
		PG_RETURN_NULL();
}



/*
 * percentile_disc(float8[])  - discrete percentiles
 */

Datum percentile_disc_multi_final(PG_FUNCTION_ARGS);

struct pct_info {
	int64	first_row;
	int64	second_row;
	float8	proportion;
	int		idx;
};

static int pct_info_cmp(const void *pa, const void *pb)
{
	const struct pct_info *a = pa;
	const struct pct_info *b = pb;
	if (a->first_row == b->first_row)
		return (a->second_row < b->second_row) ? -1 : (a->second_row == b->second_row) ? 0 : 1;
	else
		return (a->first_row < b->first_row) ? -1 : 1;
}

static struct pct_info *setup_pct_info(int num_percentiles,
									   Datum *percentiles_datum,
									   bool *percentiles_null,
									   int64 rowcount,
									   bool continuous)
{
	struct pct_info *pct_info = palloc(num_percentiles * sizeof(struct pct_info));
	int		i;

	for (i = 0; i < num_percentiles; i++)
	{
		pct_info[i].idx = i;

		if (percentiles_null[i])
		{
			pct_info[i].first_row = 0;
			pct_info[i].second_row = 0;
			pct_info[i].proportion = 0;
		}
		else
		{
			float8 p = DatumGetFloat8(percentiles_datum[i]);

			if (p < 0 || p > 1 || isnan(p))
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("percentile value %g must be between 0 and 1", p)));

			if (continuous)
			{
				pct_info[i].first_row = 1 + floor(p * (rowcount - 1));
				pct_info[i].second_row = 1 + ceil(p * (rowcount - 1));
				pct_info[i].proportion = (p * (rowcount-1)) - floor(p * (rowcount-1));
			}
			else
			{
				/*
				 * We need the smallest K such that (K/N) >= percentile. K starts at 1.
				 * Therefore K >= N*percentile
				 * Therefore K = ceil(N*percentile), minimum 1
				 */

				pct_info[i].first_row = Max(1, (int64) ceil(rowcount * p));
				pct_info[i].second_row = 0;
				pct_info[i].proportion = 0;
			}
		}
	}

	qsort(pct_info, num_percentiles, sizeof(struct pct_info), pct_info_cmp);

	return pct_info;
}

Datum
percentile_disc_multi_final(PG_FUNCTION_ARGS)
{
	ArrayType  *param;
	Datum	   *percentiles_datum;
	bool	   *percentiles_null;
	int			num_percentiles;
	int64		rowcount = AggSetGetRowCount(fcinfo);
	int64		rownum = 0;
	Tuplesortstate *sorter;
	Oid			datumtype;
	Datum		val;
	bool		isnull;
	Datum	   *result_datum;
	bool	   *result_isnull;
	int			i;
	struct pct_info *pct_info;

	struct mode_type_info {
		Oid typid;
		int16 typLen;
		bool typByVal;
		char typAlign;
	} *typinfo = fcinfo->flinfo->fn_extra;

	if (PG_ARGISNULL(0) || rowcount < 1)
		PG_RETURN_NULL();

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);

	if (!typinfo || typinfo->typid != datumtype)
	{
		if (typinfo)
			pfree(typinfo);
		typinfo = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
									 sizeof(struct mode_type_info));
		typinfo->typid = datumtype;
		get_typlenbyvalalign(datumtype,
							 &typinfo->typLen,
							 &typinfo->typByVal,
							 &typinfo->typAlign);
	}

	param = PG_GETARG_ARRAYTYPE_P(0);

	deconstruct_array(param, FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
					  &percentiles_datum, &percentiles_null, &num_percentiles);

	if (num_percentiles == 0)
		PG_RETURN_POINTER(construct_empty_array(datumtype));

	result_datum = palloc0(num_percentiles * sizeof(Datum));
	result_isnull = palloc0(num_percentiles * sizeof(bool));

	pct_info = setup_pct_info(num_percentiles,
							  percentiles_datum,
							  percentiles_null,
							  rowcount,
							  false);

	/*
	 * Start by dealing with any nulls in the param array - those are
	 * sorted to the front on row=0, so set the corresponding result
	 * indexes to null
	 */
	for (i = 0; i < num_percentiles; ++i)
	{
		int idx = pct_info[i].idx;

		if (pct_info[i].first_row > 0)
			break;

		result_datum[idx] = (Datum) 0;
		result_isnull[idx] = true;
	}

	/*
	 * If there's anything left after doing the nulls, then grind the
	 * input and extract the needed values
	 */
	if (i < num_percentiles)
	{
		tuplesort_performsort(sorter);

		for (; i < num_percentiles; ++i)
		{
			int64 target_row = pct_info[i].first_row;
			int idx = pct_info[i].idx;

			if (target_row > rownum)
			{
				while (target_row > ++rownum)
				{
					if (!tuplesort_getdatum(sorter, true, NULL, NULL))
						elog(ERROR,"missing row in percentile_disc");
				}

				if (!tuplesort_getdatum(sorter, true, &val, &isnull))
					elog(ERROR,"missing row in percentile_disc");
			}

			result_datum[idx] = val;
			result_isnull[idx] = isnull;
		}
	}

	/* We make the output array the same shape as the input */

	PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
										 ARR_NDIM(param),
										 ARR_DIMS(param), ARR_LBOUND(param),
										 datumtype,
										 typinfo->typLen,
										 typinfo->typByVal,
										 typinfo->typAlign));
}

static Datum
percentile_cont_multi_final_common(FunctionCallInfo fcinfo,
								   Oid expect_type,
								   int16 typLen, bool typByVal, char typAlign,
								   LerpFunc lerpfunc)
{
	ArrayType  *param;
	Datum	   *percentiles_datum;
	bool	   *percentiles_null;
	int			num_percentiles;
	int64		rowcount = AggSetGetRowCount(fcinfo);
	int64		rownum = 0;
	int64		rownum_second = 0;
	Tuplesortstate *sorter;
	Oid			datumtype;
	Datum		first_val;
	Datum		second_val;
	bool		isnull;
	Datum	   *result_datum;
	bool	   *result_isnull;
	int			i;
	struct pct_info *pct_info;

	if (PG_ARGISNULL(0) || rowcount < 1)
		PG_RETURN_NULL();

	AggSetGetSortInfo(fcinfo, &sorter, NULL, NULL, &datumtype);
	Assert(datumtype == expect_type);

	param = PG_GETARG_ARRAYTYPE_P(0);

	deconstruct_array(param, FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
					  &percentiles_datum, &percentiles_null, &num_percentiles);

	if (num_percentiles == 0)
		PG_RETURN_POINTER(construct_empty_array(datumtype));

	result_datum = palloc0(num_percentiles * sizeof(Datum));
	result_isnull = palloc0(num_percentiles * sizeof(bool));

	pct_info = setup_pct_info(num_percentiles,
							  percentiles_datum,
							  percentiles_null,
							  rowcount,
							  true);

	/*
	 * Start by dealing with any nulls in the param array - those are
	 * sorted to the front on row=0, so set the corresponding result
	 * indexes to null
	 */
	for (i = 0; i < num_percentiles; ++i)
	{
		int idx = pct_info[i].idx;

		if (pct_info[i].first_row > 0)
			break;

		result_datum[idx] = (Datum) 0;
		result_isnull[idx] = true;
	}

	/*
	 * If there's anything left after doing the nulls, then grind the
	 * input and extract the needed values
	 */
	if (i < num_percentiles)
	{
		tuplesort_performsort(sorter);

		for (; i < num_percentiles; ++i)
		{
			int64 target_row = pct_info[i].first_row;
			bool need_lerp = pct_info[i].second_row > target_row;
			int idx = pct_info[i].idx;

			if (target_row > rownum_second)
			{
				rownum = rownum_second;

				while (target_row > ++rownum)
				{
					if (!tuplesort_getdatum(sorter, true, NULL, NULL))
						elog(ERROR,"missing row in percentile_cont");
				}

				if (!tuplesort_getdatum(sorter, true, &first_val, &isnull) || isnull)
					elog(ERROR,"missing row in percentile_cont");

				rownum_second = rownum;

				if (need_lerp)
				{
					if (!tuplesort_getdatum(sorter, true, &second_val, &isnull) || isnull)
						elog(ERROR,"missing row in percentile_cont");
					++rownum_second;
				}
			}
			else if (target_row == rownum_second)
			{
				first_val = second_val;
				rownum = rownum_second;

				if (need_lerp)
				{
					if (!tuplesort_getdatum(sorter, true, &second_val, &isnull) || isnull)
						elog(ERROR,"missing row in percentile_cont");
					++rownum_second;
				}
			}

			if (need_lerp)
			{
				result_datum[idx] = lerpfunc(first_val, second_val, pct_info[i].proportion);
			}
			else
				result_datum[idx] = first_val;

			result_isnull[idx] = false;
		}
	}

	/* We make the output array the same shape as the input */

	PG_RETURN_POINTER(construct_md_array(result_datum, result_isnull,
										 ARR_NDIM(param),
										 ARR_DIMS(param), ARR_LBOUND(param),
										 expect_type,
										 typLen,
										 typByVal,
										 typAlign));
}


/*
 * percentile_cont(float8[]) within group (float8)  - continuous percentiles
 */

Datum percentile_cont_float8_multi_final(PG_FUNCTION_ARGS);
Datum percentile_cont_interval_multi_final(PG_FUNCTION_ARGS);

Datum
percentile_cont_float8_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  FLOAT8OID, 8, FLOAT8PASSBYVAL, 'd',
											  float8_lerp);
}

/*
 * percentile_cont(float8[]) within group (Interval)  - continuous percentiles
 */

Datum
percentile_cont_interval_multi_final(PG_FUNCTION_ARGS)
{
	return percentile_cont_multi_final_common(fcinfo,
											  INTERVALOID, 16, false, 'd',
											  interval_lerp);
}
