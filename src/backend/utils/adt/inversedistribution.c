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
	text *test_text;
	elog(WARNING,"test percentile_disc");

	test_text = PG_GETARG_TEXT_P(0);
	PG_RETURN_TEXT_P(test_text);
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
