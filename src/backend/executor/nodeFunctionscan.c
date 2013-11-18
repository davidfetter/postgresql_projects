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

/*
 * Runtime data for each function being scanned.
 */
typedef struct FunctionScanPerFuncState
{
	ExprState  *funcexpr;		/* state of the FuncExpr being evaluated */
	TupleDesc	tupdesc;		/* desc of the function result type */
	Tuplestorestate *tstore;	/* holds the function result set */
	int64		rowcount;		/* number of rows in tstore, -1 if not known */
	TupleTableSlot *func_slot;	/* function result slot (or NULL) */
} FunctionScanPerFuncState;

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
	bool		alldone;
	int64		oldpos;
	int			funcno;
	int			att;

	/*
	 * get information from the estate and scan state
	 */
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	scanslot = node->ss.ss_ScanTupleSlot;

	if (node->simple)
	{
		/*
		 * Fast path for the trivial case: the function return type and scan
		 * result type are the same, so we fetch the function result straight
		 * into the scan result slot. No need to update ordinality or
		 * rowcounts either.
		 */
		Tuplestorestate *tstore = node->funcstates[0].tstore;

		/*
		 * If first time through, read all tuples from function and put them
		 * in a tuplestore. Subsequent calls just fetch tuples from
		 * tuplestore.
		 */
		if (tstore == NULL)
		{
			node->funcstates[0].tstore = tstore =
				ExecMakeTableFunctionResult(node->funcstates[0].funcexpr,
											node->ss.ps.ps_ExprContext,
											node->funcstates[0].tupdesc,
										  node->eflags & EXEC_FLAG_BACKWARD);

			/*
			 * paranoia - cope if the function, which may have constructed the
			 * tuplestore itself, didn't leave it pointing at the start. This
			 * call is fast, so the overhead shouldn't be an issue.
			 */
			tuplestore_rescan(tstore);
		}

		/*
		 * Get the next tuple from tuplestore.
		 *
		 * If we have a rowcount for the function, and we know the previous
		 * read position was out of bounds, don't try the read. This allows
		 * backward scan to work when there are mixed row counts present.
		 */
		(void) tuplestore_gettupleslot(tstore,
									   ScanDirectionIsForward(direction),
									   false,
									   scanslot);
		return scanslot;
	}

	/*
	 * increment or decrement before checking for end-of-data, so that we can
	 * move off either end of the result by 1 (and no more than 1) without
	 * losing correct count. See PortalRunSelect for why we assume that we
	 * won't be called repeatedly in the end-of-data state.
	 */
	oldpos = node->ordinal;
	if (ScanDirectionIsForward(direction))
		oldpos = node->ordinal++;
	else
		oldpos = node->ordinal--;

	/*
	 * Main loop over functions.
	 *
	 * We fetch the function results into FUNCSLOTs (which match the function
	 * return types), and then copy the values to SCANSLOT (which matches the
	 * scan result type), setting the ordinal column (if any) in the process.
	 */
	att = 0;
	alldone = true;
	ExecClearTuple(scanslot);
	for (funcno = 0; funcno < node->nfuncs; funcno++)
	{
		FunctionScanPerFuncState *fs = &node->funcstates[funcno];
		int			i;

		/*
		 * If first time through, read all tuples from function and put them
		 * in a tuplestore. Subsequent calls just fetch tuples from
		 * tuplestore.
		 */
		if (fs->tstore == NULL)
		{
			fs->tstore =
				ExecMakeTableFunctionResult(fs->funcexpr,
											node->ss.ps.ps_ExprContext,
											fs->tupdesc,
										  node->eflags & EXEC_FLAG_BACKWARD);

			/*
			 * paranoia - cope if the function, which may have constructed the
			 * tuplestore itself, didn't leave it pointing at the start. This
			 * call is fast, so the overhead shouldn't be an issue.
			 */
			tuplestore_rescan(fs->tstore);
		}

		/*
		 * Get the next tuple from tuplestore.
		 *
		 * If we have a rowcount for the function, and we know the previous
		 * read position was out of bounds, don't try the read. This allows
		 * backward scan to work when there are mixed row counts present.
		 */
		if (fs->rowcount != -1 && fs->rowcount < oldpos)
			ExecClearTuple(fs->func_slot);
		else
			(void) tuplestore_gettupleslot(fs->tstore,
										   ScanDirectionIsForward(direction),
										   false,
										   fs->func_slot);

		if (TupIsNull(fs->func_slot))
		{
			/*
			 * If we ran out of data for this function in the forward
			 * direction then we now know how many rows it returned. We need
			 * to know this in order to handle backwards scans. The row count
			 * we store is actually 1+ the actual number, because we have to
			 * position the tuplestore 1 off its end sometimes.
			 */
			if (ScanDirectionIsForward(direction) && fs->rowcount == -1)
				fs->rowcount = node->ordinal;

			/*
			 * populate our result cols with null
			 */
			for (i = 0; i < fs->tupdesc->natts; i++)
			{
				scanslot->tts_values[att] = (Datum) 0;
				scanslot->tts_isnull[att] = true;
				att++;
			}
		}
		else
		{
			/*
			 * we have a result, so just copy it to the result cols.
			 */

			slot_getallattrs(fs->func_slot);

			for (i = 0; i < fs->tupdesc->natts; i++)
			{
				scanslot->tts_values[att] = fs->func_slot->tts_values[i];
				scanslot->tts_isnull[att] = fs->func_slot->tts_isnull[i];
				att++;
			}

			/*
			 * We're not done until every function result is exhausted; we pad
			 * the shorter results with nulls until then.
			 */
			alldone = false;
		}
	}

	/*
	 * ordinal col is always last, per spec.
	 */
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
	TupleDesc	scan_tupdesc = NULL;
	int			nfuncs = list_length(node->funcexprs);
	int			i,
				natts;
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

	/*
	 * are we adding an ordinality column?
	 */
	scanstate->ordinality = node->funcordinality;

	scanstate->nfuncs = nfuncs;
	if (nfuncs == 1 && !node->funcordinality)
		scanstate->simple = true;
	else
		scanstate->simple = false;

	/*
	 * Ordinal 0 represents the "before the first row" position.
	 *
	 * We need to track ordinal position even when not adding an ordinality
	 * column to the result, in order to handle backwards scanning properly
	 * with multiple functions with different result sizes. (We can't position
	 * any individual function's tuplestore any more than 1 place beyond its
	 * end, so when scanning backwards, we need to know when to start
	 * including the function in the scan again.)
	 */
	scanstate->ordinal = 0;

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

	scanstate->funcstates = palloc(nfuncs * sizeof(FunctionScanPerFuncState));

	i = 0;
	natts = 0;

	foreach(lc, node->funcexprs)
	{
		Expr	   *funcexpr = (Expr *) lfirst(lc);
		FunctionScanPerFuncState *fs = &scanstate->funcstates[i];
		TupleDesc	tupdesc;

		fs->funcexpr = ExecInitExpr(funcexpr, (PlanState *) scanstate);

		/*
		 * don't allocate the tuplestores; the actual call to the function
		 * does that. NULL flags that we have not called the function yet (or
		 * need to call it again after a rescan).
		 */
		fs->tstore = NULL;
		fs->rowcount = -1;

		/*
		 * Determine if this function returns a simple or composite type, and
		 * build an appropriate tupdesc. This tupdesc is the one that matches
		 * the shape of the function result, no extra columns.
		 */
		functypclass = get_expr_result_type((Node *) funcexpr,
											&funcrettype,
											&tupdesc);

		if (functypclass == TYPEFUNC_COMPOSITE)
		{
			/* Composite data type, e.g. a table's row type */
			Assert(tupdesc);

			/*
			 * XXX Existing behaviour is a bit inconsistent with regard to
			 * aliases and whole-row Vars of the function result. If the
			 * function returns a composite type, then the whole-row Var will
			 * refer to this tupdesc, which has the type's own column names
			 * rather than the alias column names given in the query. This
			 * affects the output of constructs like row_to_json which read
			 * the column names from the passed-in values.
			 */

			/* Must copy it out of typcache for safety (?) */
			tupdesc = CreateTupleDescCopy(tupdesc);

			natts += tupdesc->natts;
		}
		else if (functypclass == TYPEFUNC_SCALAR)
		{
			/* Base data type, i.e. scalar */
			char	   *attname = strVal(list_nth(node->funccolnames, natts));

			tupdesc = CreateTemplateTupleDesc(1, false);
			TupleDescInitEntry(tupdesc,
							   (AttrNumber) 1,
							   attname,
							   funcrettype,
							   -1,
							   0);
			TupleDescInitEntryCollation(tupdesc,
										(AttrNumber) 1,
										exprCollation((Node *) funcexpr));
			natts++;
		}
		else
		{
			/* crummy error message, but parser should have caught this */
			elog(ERROR, "function in FROM has unsupported return type");
		}
		fs->tupdesc = tupdesc;

		/*
		 * We only need separate slots for the function results if we are
		 * doing ordinality or multiple functions; otherwise, we fetch
		 * function results directly into the scan slot.
		 */
		if (!scanstate->simple)
		{
			fs->func_slot = ExecInitExtraTupleSlot(estate);
			ExecSetSlotDescriptor(fs->func_slot, fs->tupdesc);
		}
		else
			fs->func_slot = NULL;

		i++;
	}

	/*
	 * Create the combined TupleDesc
	 *
	 * If there is just one function without ordinality, the scan result
	 * tupdesc is the same as the function result tupdesc. (No need to make a
	 * copy.)
	 */
	if (scanstate->simple)
	{
		scan_tupdesc = scanstate->funcstates[0].tupdesc;
	}
	else
	{
		AttrNumber	attno = 0;

		if (node->funcordinality)
			natts++;

		scan_tupdesc = CreateTemplateTupleDesc(natts, false);

		for (i = 0; i < nfuncs; i++)
		{
			TupleDesc	tupdesc = scanstate->funcstates[i].tupdesc;
			int			j;

			for (j = 1; j <= tupdesc->natts; j++)
				TupleDescCopyEntry(scan_tupdesc, ++attno, tupdesc, j);
		}

		if (node->funcordinality)
		{
			/*
			 * If doing ordinality, add a column of type "bigint" at the end.
			 * The column name to use has already been recorded by the parser
			 * as the last element of funccolnames.
			 */
			TupleDescInitEntry(scan_tupdesc,
							   ++attno,
							   strVal(llast(node->funccolnames)),
							   INT8OID,
							   -1,
							   0);
		}

		Assert(attno == natts);
	}

	/*
	 * We didn't necessarily bless all the individual function tupdescs, but
	 * we have to ensure that the scan result tupdesc is, regardless of where
	 * it came from.
	 */
	BlessTupleDesc(scan_tupdesc);

	scanstate->scan_tupdesc = scan_tupdesc;

	ExecAssignScanType(&scanstate->ss, scan_tupdesc);

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	scanstate->ss.ps.ps_TupFromTlist = false;

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
	int			i;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * Release slots and tuplestore resources
	 */
	for (i = 0; i < node->nfuncs; i++)
	{
		FunctionScanPerFuncState *fs = &node->funcstates[i];

		if (fs->func_slot)
			ExecClearTuple(fs->func_slot);

		if (fs->tstore != NULL)
		{
			tuplestore_end(node->funcstates[i].tstore);
			fs->tstore = NULL;
		}
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
	FunctionScan *scan = (FunctionScan *) node->ss.ps.plan;
	int			i;
	Bitmapset  *chgparam = node->ss.ps.chgParam;

	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	for (i = 0; i < node->nfuncs; ++i)
	{
		FunctionScanPerFuncState *fs = &node->funcstates[i];

		if (fs->func_slot)
			ExecClearTuple(fs->func_slot);
	}

	ExecScanReScan(&node->ss);

	node->ordinal = 0;

	/*
	 * Here we have a choice whether to drop the tuplestores (and recompute
	 * the function outputs) or just rescan them.  We must recompute if the
	 * expression contains changed parameters, else we rescan.
	 *
	 * Note that if chgparam is NULL, it's possible that the funcparams list
	 * may be empty (if there never were any params and so finalize_plan was
	 * never called), so we have to be careful about iterating or referencing
	 * it.
	 *
	 * XXX maybe we should recompute if the function is volatile?
	 */
	if (chgparam)
	{
		ListCell   *lc;

		i = 0;
		foreach(lc, scan->funcparams)
		{
			if (bms_overlap(chgparam, lfirst(lc)))
			{
				if (node->funcstates[i].tstore != NULL)
				{
					tuplestore_end(node->funcstates[i].tstore);
					node->funcstates[i].tstore = NULL;
				}
				node->funcstates[i].rowcount = -1;
			}
			i++;
		}
	}
	for (i = 0; i < node->nfuncs; ++i)
	{
		if (node->funcstates[i].tstore != NULL)
			tuplestore_rescan(node->funcstates[i].tstore);
	}
}
