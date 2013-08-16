/*-------------------------------------------------------------------------
 *
 * nodeFunctionscan.c
 *	  Support routines for scanning RangeFunctions (functions in rangetable).
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeFunctionscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecFunctionScan		scans a function.
 *		ExecFunctionNext		retrieve next tuple in sequential order.
 *		ExecInitFunctionScan	creates and initializes a functionscan node.
 *		ExecEndFunctionScan		releases any storage allocated.
 *		ExecReScanFunctionScan	rescans the function
 *		ExecFunctionScanMarkPos	marks position in the result set
 *		ExecFunctionScanRestrPos	restores the marked position
 */
#include "postgres.h"

#include "executor/nodeFunctionscan.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "catalog/pg_type.h"

static TupleTableSlot *FunctionNext(FunctionScanState *node);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		FunctionNext
 *
 *		This is a workhorse for ExecFunctionScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
FunctionNext(FunctionScanState *node)
{
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *scanslot;
	TupleTableSlot **funcslots;
	ListCell   *lc;
	int         funcno = 0;
	int         att = 0;
	bool        alldone = true;
	int64      *rowcounts = node->rowcounts;
	int64       oldpos;

	if (node->func_slots)
	{
		/*
		 * ORDINALITY or multiple functions case:
		 *
		 * We fetch the function results into FUNCSLOTs (which match the
		 * function return types), and then copy the values to SCANSLOT (which
		 * matches the scan result type), setting the ordinal column in the
		 * process.
		 *
		 * Clear scanslot here for simplicity.
		 */

		funcslots = node->func_slots;
		scanslot = node->ss.ss_ScanTupleSlot;
		ExecClearTuple(scanslot);
	}
	else
	{
		/*
		 * trivial case: the function return type and scan result type are the
		 * same, so we fetch the function result straight into the scan result
		 * slot.
		 *
		 * We treat ss_ScanTupleSlot as an array of one element so that the
		 * code in the loop below works for both cases seamlessly.
		 */

		funcslots = &node->ss.ss_ScanTupleSlot;
		scanslot = NULL;
	}

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;

	/*
	 * increment or decrement before checking for end-of-data, so that we can
	 * move off either end of the result by 1 (and no more than 1) without
	 * losing correct count. See PortalRunSelect for why we assume that we
	 * won't be called repeatedly in the end-of-data state.
	 */

	if (ScanDirectionIsForward(direction))
		oldpos = node->ordinal++;
	else
		oldpos = node->ordinal--;

	/*
	 * Main loop over functions.
	 *
	 * func_tupdescs, funcslots, tuplestorestates and rowcounts are all arrays
	 * sized by number of functions. However, in the simple case of one
	 * function and no ordinality, rowcounts will be NULL and funcslots will
	 * point at ss_ScanTupleSlot; we bail out of the function in the simple
	 * case before this becomes an issue.
	 */

	foreach(lc, node->funcexprs)
	{
		TupleTableSlot *slot = funcslots[funcno];
		Tuplestorestate *tstore = node->tuplestorestates[funcno];
		int i, natts;

		/*
		 * If first time through, read all tuples from function and put them in a
		 * tuplestore. Subsequent calls just fetch tuples from tuplestore.
		 */
		if (tstore == NULL)
		{
			node->tuplestorestates[funcno]
				= tstore
				= ExecMakeTableFunctionResult(lfirst(lc),
											  node->ss.ps.ps_ExprContext,
											  node->func_tupdescs[funcno],
											  node->eflags & EXEC_FLAG_BACKWARD);
			/*
			 * paranoia - cope if the function, which may have constructed the
			 * tuplestore itself, didn't leave it pointing at the start. This
			 * call is fast, so the overhead shouldn't be an issue.
			 */
			tuplestore_rescan(tstore);

			/*
			 * If doing mark/restore, allocate a read pointer for the mark.
			 */
			if (node->eflags & EXEC_FLAG_MARK)
			{
				int ptrno = tuplestore_alloc_read_pointer(tstore, EXEC_FLAG_REWIND);

				/*
				 * We can't tolerate having the main tuplestore read pointer
				 * not be number 0 (there's no way to query which pointer is
				 * current). So we bail out if any additional read pointers
				 * exist. (This can't be just an assert, as in the similar
				 * case in nodeMaterial, because the function might have
				 * allocated the tuplestore itself in materialize mode.)
				 */

				if (ptrno != 1)
					elog(ERROR,"table function must not allocate tuplestore read pointers");
			}
		}

		/*
		 * Get the next tuple from tuplestore.
		 *
		 * If we have a rowcount for the function, and we know the previous
		 * read position was out of bounds, don't try the read. This allows
		 * backward scan to work when there are mixed row counts present.
		 */
		if (rowcounts && rowcounts[funcno] != -1 && rowcounts[funcno] < oldpos)
			ExecClearTuple(slot);
		else
			(void) tuplestore_gettupleslot(tstore,
										   ScanDirectionIsForward(direction),
										   false,
										   slot);

		/* bail on the simple case now */

		if (!scanslot)
			return slot;

		natts = node->func_tupdescs[funcno]->natts;

		/*
		 * If we ran out of data for this function in the forward direction
		 * then we now know how many rows it returned. We need to know this
		 * in order to handle backwards scans. The row count we store is
		 * actually 1+ the actual number, because we have to position the
		 * tuplestore 1 off its end sometimes.
		 */

		Assert(rowcounts);

		if (TupIsNull(slot))
		{
			if (ScanDirectionIsForward(direction) && rowcounts[funcno] == -1)
				rowcounts[funcno] = node->ordinal;

			for (i = 0; i < natts; ++i, ++att)
			{
				scanslot->tts_values[att] = (Datum) 0;
				scanslot->tts_isnull[att] = true;
			}
		}
		else
		{
			slot_getallattrs(slot);

			for (i = 0; i < natts; ++i, ++att)
			{
				scanslot->tts_values[att] = slot->tts_values[i];
				scanslot->tts_isnull[att] = slot->tts_isnull[i];
			}

			alldone = false;
		}

		++funcno;
	}

	if (node->ordinality)
	{
		scanslot->tts_values[att] = Int64GetDatumFast(node->ordinal);
		scanslot->tts_isnull[att] = false;
	}

	if (!alldone)
		ExecStoreVirtualTuple(scanslot);

	return scanslot;
}

/*
 * FunctionRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
FunctionRecheck(FunctionScanState *node, TupleTableSlot *slot)
{
	/* nothing to check */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecFunctionScan(node)
 *
 *		Scans the function sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecFunctionScan(FunctionScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) FunctionNext,
					(ExecScanRecheckMtd) FunctionRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitFunctionScan
 * ----------------------------------------------------------------
 */
FunctionScanState *
ExecInitFunctionScan(FunctionScan *node, EState *estate, int eflags)
{
	FunctionScanState *scanstate;
	Oid			funcrettype;
	TypeFuncClass functypclass;
	TupleDesc  *func_tupdescs = NULL;
	TupleDesc	scan_tupdesc = NULL;
	int         nfuncs = list_length(node->funcexprs);
	bool        ordinality = node->funcordinality;
	int         ntupdescs = nfuncs + (ordinality ? 1 : 0);
	int         i, atts_done;
	ListCell   *lc;

	/*
	 * FunctionScan should not have any children.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create new ScanState for node
	 */
	scanstate = makeNode(FunctionScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->eflags = eflags;

	scanstate->ordinality = ordinality;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	/*
	 * Set up to initialize a tupdesc for each function, plus one for the
	 * ordinality column if any. We need this even for the one-function case.
	 */

	scanstate->func_tupdescs
		= func_tupdescs
		= palloc(ntupdescs * sizeof(TupleDesc));

	i = 0;
	atts_done = 0;
	foreach(lc, node->funcexprs)
	{
		TupleDesc tupdesc;

		/*
		 * Determine if this function returns a simple or composite type, and
		 * build an appropriate tupdesc. This tupdesc is the one that matches
		 * the shape of the function result, no extra columns.
		 */
		functypclass = get_expr_result_type(lfirst(lc),
											&funcrettype,
											&tupdesc);

		if (functypclass == TYPEFUNC_COMPOSITE)
		{
			/* Composite data type, e.g. a table's row type */
			Assert(tupdesc);

			/*
			 * XXX
			 * Existing behaviour is a bit inconsistent with regard to aliases
			 * and whole-row Vars of the function result. If the function
			 * returns a composite type, then the whole-row Var will refer to
			 * this tupdesc, which has the type's own column names rather than
			 * the alias column names given in the query. This affects the
			 * output of constructs like row_to_json which read the column
			 * names from the passed-in values.
			 */

			/* Must copy it out of typcache for safety (?) */
			tupdesc = CreateTupleDescCopy(tupdesc);

			atts_done += tupdesc->natts;
		}
		else if (functypclass == TYPEFUNC_SCALAR)
		{
			/* Base data type, i.e. scalar */
			char	   *attname = strVal(list_nth(node->funccolnames, atts_done));

			tupdesc = CreateTemplateTupleDesc(1, false);
			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   attname,
							   funcrettype,
							   -1,
							   0);
			TupleDescInitEntryCollation(tupdesc,
										(AttrNumber) 1,
										exprCollation(lfirst(lc)));

			++atts_done;
		}
		else if (functypclass == TYPEFUNC_RECORD)
		{
			/*
			 * Unspecified RECORD as the return type, with a column definition
			 * list supplied in the call; must be the only function present
			 */
			Assert(atts_done == 0);
			Assert(lnext(lc) == NULL);

			tupdesc = BuildDescFromLists(node->funccolnames,
										 node->funccoltypes,
										 node->funccoltypmods,
										 node->funccolcollations);
		}
		else
		{
			/* crummy error message, but parser should have caught this */
			elog(ERROR, "function in FROM has unsupported return type");
		}

		func_tupdescs[i++] = tupdesc;
	}

	/*
	 * If doing ordinality, we need a new tupdesc with one additional column
	 * tacked on, always of type "bigint". The name to use has already been
	 * recorded by the parser as the last element of funccolnames.
	 *
	 * Without ordinality or multiple functions, the scan result tupdesc is
	 * the same as the function result tupdesc. (No need to make a copy.)
	 */
	if (ntupdescs > 1)
	{
		if (ordinality)
		{
			TupleDesc tupdesc = CreateTemplateTupleDesc(1, false);

			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   strVal(llast(node->funccolnames)),
							   INT8OID,
							   -1,
							   0);

			func_tupdescs[nfuncs] = tupdesc;
		}

		scan_tupdesc = CreateTupleDescCopyMany(func_tupdescs, ntupdescs);
	}
	else
		scan_tupdesc = func_tupdescs[0];

	/*
	 * We didn't necessarily bless all the individual function tupdescs, but
	 * we have to ensure that the scan result tupdesc is, regardless of where
	 * it came from.
	 */
	BlessTupleDesc(scan_tupdesc);

	scanstate->scan_tupdesc = scan_tupdesc;

	ExecAssignScanType(&scanstate->ss, scan_tupdesc);

	/*
	 * We only need separate slots for the function results if we are doing
	 * ordinality or multiple functions; otherwise, we fetch function
	 * results directly into the scan slot. Same for rowcounts.
	 *
	 * However, we don't need a slot for the ordinality col, even though we
	 * made a tupdesc for it.
	 */
	if (ntupdescs > 1)
	{
		scanstate->func_slots = palloc(nfuncs * sizeof(TupleTableSlot *));
		scanstate->rowcounts = palloc(nfuncs * sizeof(int64));

		for (i = 0; i < nfuncs; ++i)
		{
			scanstate->rowcounts[i] = -1;
			scanstate->func_slots[i] = ExecInitExtraTupleSlot(estate);
			ExecSetSlotDescriptor(scanstate->func_slots[i], func_tupdescs[i]);
		}
	}
	else
	{
		scanstate->func_slots = NULL;
		scanstate->rowcounts = NULL;
	}

	/*
	 * Other node-specific setup
	 */
	scanstate->ordinal = 0;
	scanstate->mark_ordinal = -1;

	scanstate->tuplestorestates = palloc(nfuncs * sizeof(Tuplestorestate *));
	for (i = 0; i < nfuncs; ++i)
		scanstate->tuplestorestates[i] = NULL;

	scanstate->funcexprs = (List *) ExecInitExpr((Expr *) node->funcexprs,
												 (PlanState *) scanstate);

	scanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndFunctionScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndFunctionScan(FunctionScanState *node)
{
	int i;
	int nfuncs = list_length(node->funcexprs);

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	if (node->func_slots)
		for (i = 0; i < nfuncs; ++i)
			ExecClearTuple(node->func_slots[i]);

	/*
	 * Release tuplestore resources
	 */
	for (i = 0; i < nfuncs; ++i)
	{
		if (node->tuplestorestates[i] != NULL)
			tuplestore_end(node->tuplestorestates[i]);
		node->tuplestorestates[i] = NULL;
	}
}

/* ----------------------------------------------------------------
 *		ExecFunctionScanMarkPos
 *
 *		Calls tuplestore to save the current position in the stored file.
 * ----------------------------------------------------------------
 */
void
ExecFunctionScanMarkPos(FunctionScanState *node)
{
	int i;
	int nfuncs = list_length(node->funcexprs);

	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't materialized yet, return, but note that we marked
	 * the non-materialized state.
	 */
	if (!node->tuplestorestates[0])
	{
		node->mark_ordinal = -1;
		return;
	}

	node->mark_ordinal = node->ordinal;

	for (i = 0; i < nfuncs; ++i)
		tuplestore_copy_read_pointer(node->tuplestorestates[i], 0, 1);
}

/* ----------------------------------------------------------------
 *		ExecFunctionScanRestrPos
 *
 *		Calls tuplestore to restore the last saved file position.
 * ----------------------------------------------------------------
 */
void
ExecFunctionScanRestrPos(FunctionScanState *node)
{
	int i;
	int nfuncs = list_length(node->funcexprs);

	Assert(node->eflags & EXEC_FLAG_MARK);

	/*
	 * if we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestates[0])
		return;

	/*
	 * If we marked the pre-materialized state, then treat it as
	 * a rewind. In that case, also re-mark the state in the
	 * rewound position.
	 */
	if (node->mark_ordinal == -1)
	{
		for (i = 0; i < nfuncs; ++i)
		{
			tuplestore_rescan(node->tuplestorestates[i]);
			tuplestore_copy_read_pointer(node->tuplestorestates[i], 0, 1);
		}
		node->mark_ordinal = 0;
	}
	else
	{
		/*
		 * copy the mark to the active read pointer.
		 */
		for (i = 0; i < nfuncs; ++i)
			tuplestore_copy_read_pointer(node->tuplestorestates[i], 1, 0);
	}

	node->ordinal = node->mark_ordinal;
}

/* ----------------------------------------------------------------
 *		ExecReScanFunctionScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanFunctionScan(FunctionScanState *node)
{
	int i;
	int nfuncs = list_length(node->funcexprs);

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	if (node->func_slots)
		for (i = 0; i < nfuncs; ++i)
			ExecClearTuple(node->func_slots[i]);

	ExecScanReScan(&node->ss);

	node->ordinal = 0;

	/*
	 * If we haven't materialized yet, just return.
	 */
	if (!node->tuplestorestates[0])
		return;

	node->mark_ordinal = -1;

	/*
	 * Here we have a choice whether to drop the tuplestores (and recompute the
	 * function outputs) or just rescan them.  We must recompute if the
	 * expression contains parameters, else we rescan.	XXX maybe we should
	 * recompute if the function is volatile?  Work out what params belong
	 * to what functions?
	 */
	for (i = 0; i < nfuncs; ++i)
	{
		if (node->ss.ps.chgParam != NULL)
		{
			if (node->tuplestorestates[i] != NULL)
				tuplestore_end(node->tuplestorestates[i]);
			node->tuplestorestates[i] = NULL;
			if (node->rowcounts)
				node->rowcounts[i] = -1;
		}
		else
		{
			tuplestore_rescan(node->tuplestorestates[i]);
			if (node->eflags & EXEC_FLAG_MARK)
				tuplestore_copy_read_pointer(node->tuplestorestates[i], 0, 1);
		}
	}
}
