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
	Tuplestorestate **tuplestorestatep = node->tuplestorestates;
	TupleTableSlot *scanslot;
	TupleTableSlot **funcslotp;
	TupleDesc  *func_tupdescp = node->func_tupdescs;
	ListCell   *lc;
	int         att = 0;
	bool        alldone = true;
	int64      *rowcountp = node->rowcounts;
	int64       oldpos;

	if (node->func_slots)
	{
		/*
		 * ORDINALITY or multiple functions case:
		 *
		 * We fetch the function result into FUNCSLOT (which matches the
		 * function return type), and then copy the values to SCANSLOT
		 * (which matches the scan result type), setting the ordinal
		 * column in the process.
		 */

		funcslotp = node->func_slots;
		scanslot = node->ss.ss_ScanTupleSlot;
		ExecClearTuple(scanslot);
	}
	else
	{
		/*
		 * trivial case: the function return type and scan result
		 * type are the same, so we fetch the function result straight
		 * into the scan result slot.
		 */

		funcslotp = &node->ss.ss_ScanTupleSlot;
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

	foreach(lc, node->funcexprs)
	{
		TupleDesc tupdesc = *func_tupdescp++;
		TupleTableSlot *slot = *funcslotp++;
		int i, natts;

		/*
		 * If first time through, read all tuples from function and put them in a
		 * tuplestore. Subsequent calls just fetch tuples from tuplestore.
		 */
		if (*tuplestorestatep == NULL)
		{
			*tuplestorestatep =
				ExecMakeTableFunctionResult(lfirst(lc),
											node->ss.ps.ps_ExprContext,
											tupdesc,
											node->eflags & EXEC_FLAG_BACKWARD);
		}

		/*
		 * Get the next tuple from tuplestore. Return NULL if no more tuples.
		 */
		if (!rowcountp || *rowcountp == -1 || *rowcountp >= oldpos)
			(void) tuplestore_gettupleslot(*tuplestorestatep,
										   ScanDirectionIsForward(direction),
										   false,
										   slot);
		else
			ExecClearTuple(slot);

		/* bail on the simple case now */
		if (!scanslot)
			return slot;

		natts = tupdesc->natts;

		/*
		 * If we ran out of data for this function in the forward direction
		 * then we now know how many rows it returned. We need to know this
		 * in order to handle backwards scans. The row count we store is
		 * actually 1+ the actual number, because we have to position the
		 * tuplestore 1 off its end sometimes.
		 */

		Assert(rowcountp);

		if (TupIsNull(slot))
		{
			if (ScanDirectionIsForward(direction) && *rowcountp == -1)
				*rowcountp = node->ordinal;

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

		++rowcountp;
		++tuplestorestatep;
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
	int         i, atts_done;
	ListCell   *lc;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

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
	 * We only need separate slots for the function results if we are doing
	 * ordinality or multiple functions; otherwise, we fetch function
	 * results directly into the scan slot. Same for rowcounts.
	 */
	if (ordinality || nfuncs > 1)
	{
		scanstate->func_slots = palloc(nfuncs * sizeof(TupleTableSlot *));
		scanstate->rowcounts = palloc(nfuncs * sizeof(int64));

		for (i = 0; i < nfuncs; ++i)
		{
			scanstate->func_slots[i] = ExecInitExtraTupleSlot(estate);
			scanstate->rowcounts[i] = -1;
		}
	}
	else
	{
		scanstate->func_slots = NULL;
		scanstate->rowcounts = NULL;
	}

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->scan.plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->scan.plan.qual,
					 (PlanState *) scanstate);

	scanstate->func_tupdescs
		= func_tupdescs
		= palloc((ordinality ? (nfuncs + 1) : nfuncs) * sizeof(TupleDesc));

	i = 0;
	atts_done = 0;
	foreach(lc, node->funcexprs)
	{
		TupleDesc tupdesc;

		/*
		 * Now determine if the function returns a simple or composite
		 * type, and build an appropriate tupdesc. This tupdesc
		 * (func_tupdesc) is the one that matches the shape of the
		 * function result, no extra columns.
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
			 * Existing behaviour is a bit inconsistent with regard to aliases and
			 * whole-row Vars of the function result. If the function returns a
			 * composite type, then the whole-row Var will refer to this tupdesc,
			 * which has the type's own column names rather than the alias column
			 * names given in the query. This affects the output of constructs like
			 * row_to_json which read the column names from the passed-in values.
			 */

			/* Must copy it out of typcache for safety */
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

		/*
		 * For RECORD results, make sure a typmod has been assigned.  (The
		 * function should do this for itself, but let's cover things in case it
		 * doesn't.)
		 */
		BlessTupleDesc(tupdesc);

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
	if (ordinality || nfuncs > 1)
	{
		int ntupdescs = nfuncs;

		if (ordinality)
		{
			TupleDesc tupdesc = CreateTemplateTupleDesc(1, false);

			TupleDescInitEntry(tupdesc,
							   1,
							   strVal(llast(node->funccolnames)),
							   INT8OID,
							   -1,
							   0);

			func_tupdescs[ntupdescs++] = tupdesc;
		}

		scan_tupdesc = CreateTupleDescCopyMany(func_tupdescs, ntupdescs);

		BlessTupleDesc(scan_tupdesc);
	}
	else
		scan_tupdesc = func_tupdescs[0];

	scanstate->scan_tupdesc = scan_tupdesc;

	ExecAssignScanType(&scanstate->ss, scan_tupdesc);

	if (scanstate->func_slots)
		for (i = 0; i < nfuncs; ++i)
			ExecSetSlotDescriptor(scanstate->func_slots[i], func_tupdescs[i]);

	/*
	 * Other node-specific setup
	 */
	scanstate->ordinal = 0;

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
			tuplestore_rescan(node->tuplestorestates[i]);
	}
}
