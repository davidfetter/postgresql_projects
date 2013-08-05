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

/*
 * rank(float8)  - discrete (nearest) percentile
 */

Datum hypothetical_rank_final(PG_FUNCTION_ARGS);

Datum
hypothetical_rank_final(PG_FUNCTION_ARGS)
{
	elog(WARNING,"Not implemented yet");
	PG_RETURN_NULL();
}
