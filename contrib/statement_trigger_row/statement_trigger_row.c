/*-------------------------------------------------------------------------
 *
 * statement_trigger_row.c
 * 		statement_trigger_row support for PostgreSQL
 *
 * Portions Copyright (c) 2011-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 * 		contrib/statement_trigger_row.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/spi.h"
#include "commands/trigger.h"
#include "utils/rel.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum statement_trigger_row(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(statement_trigger_row);

Datum
statement_trigger_row(PG_FUNCTION_ARGS)
{
	TriggerData		*trigdata = (TriggerData *) fcinfo->context;
	TupleDesc		tupdesc;
	TupleTableSlot	*slot;
	Tuplestorestate	*new_tuplestore;
	Tuplestorestate	*old_tuplestore;
	int64			delta = 0;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("statement_trigger_row: not called by trigger manager")));
	}

	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event))
	{
		ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("statement_trigger_row: not called by AFTER trigger")));
	}

	if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
	{
		ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("You may not call this function in a TRUNCATE trigger.")));
	}

	tupdesc = trigdata->tg_relation->rd_att;

	slot = MakeSingleTupleTableSlot(tupdesc);

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
	{
		if (trigdata->tg_newdelta == NULL)
			ereport(ERROR,
					(errmsg("You must include NEW TABLE AS in your CREATE TRIGGER statement")));

		new_tuplestore = trigdata->tg_newdelta;
		/*
		 * Ensure that we're at the right place in the tuplestore, as
		 * other triggers may have messed with the state.
		 */
		tuplestore_rescan(new_tuplestore);

		/* Iterate through the new tuples, adding. */
		while (tuplestore_gettupleslot(new_tuplestore, true, false, slot)) {
			bool	isnull;
			Datum	val = slot_getattr(slot, 1, &isnull);
			if (!isnull)
				delta += DatumGetInt32(val);
		}
	}
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
	{
		if (trigdata->tg_olddelta == NULL)
			ereport(ERROR,
					(errmsg("You must include OLD TABLE AS in your CREATE TRIGGER statement")));

		old_tuplestore = trigdata->tg_olddelta;
		tuplestore_rescan(old_tuplestore);
		/* Iterate through the old tuples, subtracting. */
		while (tuplestore_gettupleslot(old_tuplestore, true, false, slot)) {
			bool	isnull;
			Datum	val = slot_getattr(slot, 1, &isnull);
			if (!isnull)
				delta -= DatumGetInt32(val);
		}
	}
	else /* It's an UPDATE */
	{
		if (trigdata->tg_olddelta == NULL)
			ereport(ERROR,
					(errmsg("You must include OLD TABLE AS in your CREATE TRIGGER statement")));
		if (trigdata->tg_newdelta == NULL)
			ereport(ERROR,
					(errmsg("You must include NEW TABLE AS in your CREATE TRIGGER statement")));

		old_tuplestore = trigdata->tg_olddelta;
		new_tuplestore = trigdata->tg_newdelta;

		tuplestore_rescan(old_tuplestore);
		tuplestore_rescan(new_tuplestore);

		/* Iterate through both the new and old tuples, incrementing
		 * or decrementing as needed. */
		while (tuplestore_gettupleslot(new_tuplestore, true, false, slot)) {
			bool	isnull;
			Datum	val = slot_getattr(slot, 1, &isnull);
			if (!isnull)
				delta += DatumGetInt32(val);
		}

		while (tuplestore_gettupleslot(old_tuplestore, true, false, slot)) {
			bool	isnull;
			Datum	val = slot_getattr(slot, 1, &isnull);
			if (!isnull)
				delta -= DatumGetInt32(val);
		}

	}

	ExecDropSingleTupleTableSlot(slot);

	ereport(NOTICE, (errmsg("Total change: " INT64_FORMAT, delta)));

	return PointerGetDatum(NULL);

}
