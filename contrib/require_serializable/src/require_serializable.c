/*
 * --------------------------------------------------------------------------
 *
 * require_serializable.c
 *
 * Copyright (C) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 			contrib/require_serializable/require_serializable.c
 *
 * --------------------------------------------------------------------------
 */
#include "postgres.h"

#include "parser/analyze.h"
#include "access/xact.h"

#include "utils/elog.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

static		post_parse_analyze_hook_type original_post_parse_analyze_hook = NULL;

/*
 * This module makes simple UPDATE and DELETE statements require a WHERE clause
 * and complains when this is not present.
 */
static void
require_serializable_check(ParseState *pstate, Query *query)
{
	if (query->commandType != CMD_UTILITY && XactIsoLevel != XACT_SERIALIZABLE)
	{
		/* First, warn about serializable isolation for everything */
		if (query->commandType != CMD_SELECT || (query->commandType == CMD_SELECT && XactDeferrable))
			ereport(ERROR,
					(errcode(ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION),
					 errmsg("You must use serializable transaction isolation when the \"require_serializable\" extension is loaded."),
					 errhint("It might be simplest to ALTER SYSTEM SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE")));
		else if (query->commandType == CMD_SELECT && !XactDeferrable)
			ereport(ERROR,
					(errcode(ERRCODE_INAPPROPRIATE_ISOLATION_LEVEL_FOR_BRANCH_TRANSACTION),
					 errmsg("You must use DEFERRABLE for reads when the \"require_serializable\" extension is loaded."),
					 errhint("BEGIN DEFERRABLE may help.")));
	}
}

void
_PG_init(void)
{
	original_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = require_serializable_check;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = original_post_parse_analyze_hook;
}
