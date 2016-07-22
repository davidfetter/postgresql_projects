/*
 * --------------------------------------------------------------------------
 *
 * require_where.c
 *
 * Copyright (C) 2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 			contrib/require_where/require_where.c
 *
 * --------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "parser/analyze.h"

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
require_where_check(ParseState *pstate, Query *query)
{

	if (query->commandType == CMD_UPDATE || query->commandType == CMD_DELETE)
	{
		/* Make sure there's something to look at. */
		Assert(query->jointree != NULL);
		if (query->jointree->quals == NULL)
		{
			if (query->commandType == CMD_UPDATE)
				ereport(ERROR,
						(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
						 errmsg("UPDATE requires a WHERE clause when the \"require_where\" extension is loaded."),
						 errhint("To update all rows, use \"WHERE true\" or similar.")));
			else
				ereport(ERROR,
						(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
						 errmsg("DELETE requires a WHERE clause when the \"require_where\" extension is loaded."),
						 errhint("To delete all rows, use \"WHERE true\" or similar.")));
		}
	}

}

void
_PG_init(void)
{
	original_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = require_where_check;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = original_post_parse_analyze_hook;
}
