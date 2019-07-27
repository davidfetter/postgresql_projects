/*-----------------------------------------------------------
 * pg_describe.c
 *
 * Support for DESCRIBE
 *
 *
 * Copyright (c) 2019, PostgreSQL Global Development Group
 * Written by David Fetter <david@fetter.org>
 *
 * IDENTIFICATION
 *    src/backend/utils/misc/pg_describe.c
 *-----------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>
#include <limits.h>
#include <unistd.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif

#include "utils/pg_describe.h"

/*
 * DESCRIBE command
 */
TupleDesc
PGDescribe(const char *type, const char *name)
{
	TupleDesc	tupdesc;

	if (pg_strcasecmp(type, "access method") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(4);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "access method type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "handler function",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "description",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "aggregate") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(4);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "sfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "stype",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "sspace",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "finalfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "finalfunc_extra",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "combinefunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "serialfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "deserialfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "initcond",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "msfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "minvfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "mstype",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "msspace",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "mfinalfunc",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 16, "mfinalfunc_extra",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 17, "minitcond",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 18, "sortop",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 19, "parallel",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "cast") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(6);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "source_type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "target_type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "function",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "assignment",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "implicit",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "inout",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "collation") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(7);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "locale",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "lc_collate",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "lc_ctype",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "provider",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "deterministic",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "version",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "conversion") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_default",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "source_encoding",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "dest_encoding",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "function_name",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "database") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(10);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "owner",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "template",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "encoding",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "lc_collate",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "lc_ctype",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "tablespace",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "allowconn",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "connlimit",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "istemplate",
						   BOOLOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "domain") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "date_type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "collate",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "default",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "constraint",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "event trigger") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(5);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "event",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "filter",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "routine_type",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "routine_name",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "extension") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(3);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "schema",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "version",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "foreign data wrapper") == 0)
	{
		tupdesc = CreateTemplateTupleDesc(3);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "handler",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "validator",
						   TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "options",
						   TEXTOID, -1, 0);
	}
	else if (pg_strcasecmp(type, "foreign table") == 0)
	{
	}
	else if (pg_strcasecmp(type, "function") == 0)
	{
	}
	else if (pg_strcasecmp(type, "index") == 0)
	{
	}
	else if (pg_strcasecmp(type, "language") == 0)
	{
	}
	else if (pg_strcasecmp(type, "large object") == 0)
	{
	}
	else if (pg_strcasecmp(type, "materialized view") == 0)
	{
	}
	else if (pg_strcasecmp(type, "operator") == 0)
	{
	}
	else if (pg_strcasecmp(type, "operator class") == 0)
	{
	}
	else if (pg_strcasecmp(type, "operator family") == 0)
	{
	}
	else if (pg_strcasecmp(type, "policy") == 0)
	{
	}
	else if (pg_strcasecmp(type, "procedure") == 0)
	{
	}
	else if (pg_strcasecmp(type, "publication") == 0)
	{
	}
	else if (pg_strcasecmp(type, "role") == 0)
	{
	}
	else if (pg_strcasecmp(type, "routine") == 0)
	{
	}
	else if (pg_strcasecmp(type, "rule") == 0)
	{
	}
	else if (pg_strcasecmp(type, "schema") == 0)
	{
	}
	else if (pg_strcasecmp(type, "sequence") == 0)
	{
	}
	else if (pg_strcasecmp(type, "server") == 0)
	{
	}
	else if (pg_strcasecmp(type, "statistics") == 0)
	{
	}
	else if (pg_strcasecmp(type, "subscription") == 0)
	{
	}
	else if (pg_strcasecmp(type, "table") == 0)
	{
	}
	else if (pg_strcasecmp(type, "tablespace") == 0)
	{
	}
	else if (pg_strcasecmp(type, "text search configuration") == 0)
	{
	}
	else if (pg_strcasecmp(type, "text search dictionary") == 0)
	{
	}
	else if (pg_strcasecmp(type, "text search parser") == 0)
	{
	}
	else if (pg_strcasecmp(type, "text search template") == 0)
	{
	}
	else if (pg_strcasecmp(type, "transform for") == 0)
	{
	}
	else if (pg_strcasecmp(type, "trigger") == 0)
	{
	}
	else if (pg_strcasecmp(type, "type") == 0)
	{
	}
	else /* (pg_strcasecmp(type, "view") == 0) */
	{
	}
}

static void
(const char *type, const char *name, DestReceiver *dest)
{
	TupOutputState *tstate;
	TupleDesc		tupdesc;
	const char	   *typename;
	const char	   *typeval;
	const char	   *objname;
	const char	   *objval;

	/* Get the value and canonical spelling of type */
	typeval = GetTypeByName(type, &typename, false);

	/* Get the value and canonical spelling of name */
	typeval = GetObjectByName(name, &objname, false);

	typdesc =
