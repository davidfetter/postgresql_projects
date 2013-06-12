<<<<<<< HEAD
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
=======
>>>>>>> 2f3bbf6... First cut at patch.
#include "postgres.h"
#include "fmgr.h"
#include <string.h>

<<<<<<< HEAD
/*
 * percentile_disc(float8)  - discrete (nearest) percentile
 */

Datum percentile_disc(PG_FUNCTION_ARGS);

Datum
percentile_disc(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented yet");
}

=======
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum percentile_disc(PG_FUNCTION_ARGS);

>>>>>>> 2f3bbf6... First cut at patch.
PG_FUNCTION_INFO_V1(percentile_disc);

Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
	text *test_text;
	elog(WARNING,"test percentile_disc");

	test_text = PG_GETARG_TEXT_P(0);
	PG_RETURN_TEXT_P(test_text);
}
<<<<<<< HEAD

/*
 * percentile_cont(float8)  - continuous (nearest) percentile
 */
Datum
percentile_cont(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented yet");
}

PG_FUNCTION_INFO_V1(percentile_disc);

Datum
percentile_cont_final(PG_FUNCTION_ARGS)
{
	text *test_text;
	elog(WARNING,"test percentile_cont");

	test_text = PG_GETARG_TEXT_P(0);
	PG_RETURN_TEXT_P(test_text);
}
=======
>>>>>>> 2f3bbf6... First cut at patch.
