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

/*
 * percentile_disc(float8)  - discrete (nearest) percentile
 */

Datum
percentile_disc(PG_FUNCTION_ARGS)
{
	elog(ERROR, "not implemented yet");
}

