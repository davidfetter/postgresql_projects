/*-------------------------------------------------------------------------
 *
 * nodeAgg.c
 *	  Routines to handle aggregate nodes.
 *
 *	  ExecAgg evaluates each aggregate in the following steps:
 *
 *		 transvalue = initcond
 *		 foreach input_tuple do
 *			transvalue = transfunc(transvalue, input_value(s))
 *		 result = finalfunc(transvalue, direct_argument(s))
 *
 *	  If a finalfunc is not supplied then the result is just the ending
 *	  value of transvalue.
 *
 *	  If a normal aggregate call specifies DISTINCT or ORDER BY, we sort the
 *	  input tuples and eliminate duplicates (if required) before performing
 *	  the above-depicted process.  (However, we don't do that for ordered-set
 *	  aggregates; their "ORDER BY" inputs are ordinary aggregate arguments
 *	  so far as this module is concerned.)
 *
 *	  If transfunc is marked "strict" in pg_proc and initcond is NULL,
 *	  then the first non-NULL input_value is assigned directly to transvalue,
 *	  and transfunc isn't applied until the second non-NULL input_value.
 *	  The agg's first input type and transtype must be the same in this case!
 *
 *	  If transfunc is marked "strict" then NULL input_values are skipped,
 *	  keeping the previous transvalue.  If transfunc is not strict then it
 *	  is called for every input tuple and must deal with NULL initcond
 *	  or NULL input_values for itself.
 *
 *	  If finalfunc is marked "strict" then it is not called when the
 *	  ending transvalue is NULL, instead a NULL result is created
 *	  automatically (this is just the usual handling of strict functions,
 *	  of course).  A non-strict finalfunc can make its own choice of
 *	  what to return for a NULL ending transvalue.
 *
 *	  Ordered-set aggregates are treated specially in one other way: we
 *	  evaluate any "direct" arguments and pass them to the finalfunc along
 *	  with the transition value.
 *
 *	  A finalfunc can have additional arguments beyond the transvalue and
 *	  any "direct" arguments, corresponding to the input arguments of the
 *	  aggregate.  These are always just passed as NULL.  Such arguments may be
 *	  needed to allow resolution of a polymorphic aggregate's result type.
 *
 *	  We compute aggregate input expressions and run the transition functions
 *	  in a temporary econtext (aggstate->tmpcontext).  This is reset at
 *	  least once per input tuple, so when the transvalue datatype is
 *	  pass-by-reference, we have to be careful to copy it into a longer-lived
 *	  memory context, and free the prior value to avoid memory leakage.
 *	  We store transvalues in the memory context aggstate->aggcontext,
 *	  which is also used for the hashtable structures in AGG_HASHED mode.
 *	  The node's regular econtext (aggstate->ss.ps.ps_ExprContext)
 *	  is used to run finalize functions and compute the output tuple;
 *	  this context can be reset once per output tuple.
 *
 *	  The executor's AggState node is passed as the fmgr "context" value in
 *	  all transfunc and finalfunc calls.  It is not recommended that the
 *	  transition functions look at the AggState node directly, but they can
 *	  use AggCheckCallContext() to verify that they are being called by
 *	  nodeAgg.c (and not as ordinary SQL functions).  The main reason a
 *	  transition function might want to know this is so that it can avoid
 *	  palloc'ing a fixed-size pass-by-ref transition value on every call:
 *	  it can instead just scribble on and return its left input.  Ordinarily
 *	  it is completely forbidden for functions to modify pass-by-ref inputs,
 *	  but in the aggregate case we know the left input is either the initial
 *	  transition value or a previous function result, and in either case its
 *	  value need not be preserved.  See int8inc() for an example.  Notice that
 *	  advance_transition_function() is coded to avoid a data copy step when
 *	  the previous transition value pointer is returned.  Also, some
 *	  transition functions want to store working state in addition to the
 *	  nominal transition value; they can use the memory context returned by
 *	  AggCheckCallContext() to do that.
 *
 *	  Note: AggCheckCallContext() is available as of PostgreSQL 9.0.  The
 *	  AggState is available as context in earlier releases (back to 8.1),
 *	  but direct examination of the node is needed to use it before 9.0.
 *
 *	  As of 9.4, aggregate transition functions can also use AggGetAggref()
 *	  to get hold of the Aggref expression node for their aggregate call.
 *	  This is mainly intended for ordered-set aggregates, which are not
 *	  supported as window functions.  (A regular aggregate function would
 *	  need some fallback logic to use this, since there's no Aggref node
 *	  for a window function.)
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeAgg.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "executor/executor.h"
#include "executor/nodeAgg.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"


/*
 * AggStatePerAggData - per-aggregate working state for the Agg scan
 */
typedef struct AggStatePerAggData
{
	/*
	 * These values are set up during ExecInitAgg() and do not change
	 * thereafter:
	 */

	/* Links to Aggref expr and state nodes this working state is for */
	AggrefExprState *aggrefstate;
	Aggref	   *aggref;

	/*
	 * Nominal number of arguments for aggregate function.  For plain aggs,
	 * this excludes any ORDER BY expressions.  For ordered-set aggs, this
	 * counts both the direct and aggregated (ORDER BY) arguments.
	 */
	int			numArguments;

	/*
	 * Number of aggregated input columns.  This includes ORDER BY expressions
	 * in both the plain-agg and ordered-set cases.  Ordered-set direct args
	 * are not counted, though.
	 */
	int			numInputs;

	/*
	 * Number of aggregated input columns to pass to the transfn.  This
	 * includes the ORDER BY columns for ordered-set aggs, but not for plain
	 * aggs.  (This doesn't count the transition state value!)
	 */
	int			numTransInputs;

	/*
	 * Number of arguments to pass to the finalfn.  This is always at least 1
	 * (the transition state value) plus any ordered-set direct args. If the
	 * finalfn wants extra args then we pass nulls corresponding to the
	 * aggregated input columns.
	 */
	int			numFinalArgs;

	/* Oids of transfer functions */
	Oid			transfn_oid;
	Oid			finalfn_oid;	/* may be InvalidOid */

	/*
	 * fmgr lookup data for transfer functions --- only valid when
	 * corresponding oid is not InvalidOid.  Note in particular that fn_strict
	 * flags are kept here.
	 */
	FmgrInfo	transfn;
	FmgrInfo	finalfn;

	/* Input collation derived for aggregate */
	Oid			aggCollation;

	/* number of sorting columns */
	int			numSortCols;

	/* number of sorting columns to consider in DISTINCT comparisons */
	/* (this is either zero or the same as numSortCols) */
	int			numDistinctCols;

	/* deconstructed sorting information (arrays of length numSortCols) */
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirst;

	/*
	 * fmgr lookup data for input columns' equality operators --- only
	 * set/used when aggregate has DISTINCT flag.  Note that these are in
	 * order of sort column index, not parameter index.
	 */
	FmgrInfo   *equalfns;		/* array of length numDistinctCols */

	/*
	 * initial value from pg_aggregate entry
	 */
	Datum		initValue;
	bool		initValueIsNull;

	/*
	 * We need the len and byval info for the agg's input, result, and
	 * transition data types in order to know how to copy/delete values.
	 *
	 * Note that the info for the input type is used only when handling
	 * DISTINCT aggs with just one argument, so there is only one input type.
	 */
	int16		inputtypeLen,
				resulttypeLen,
				transtypeLen;
	bool		inputtypeByVal,
				resulttypeByVal,
				transtypeByVal;

	/*
	 * Stuff for evaluation of inputs.  We used to just use ExecEvalExpr, but
	 * with the addition of ORDER BY we now need at least a slot for passing
	 * data to the sort object, which requires a tupledesc, so we might as
	 * well go whole hog and use ExecProject too.
	 */
	TupleDesc	evaldesc;		/* descriptor of input tuples */
	ProjectionInfo *evalproj;	/* projection machinery */

	/*
	 * Slots for holding the evaluated input arguments.  These are set up
	 * during ExecInitAgg() and then used for each input row.
	 */
	TupleTableSlot *evalslot;	/* current input tuple */
	TupleTableSlot *uniqslot;	/* used for multi-column DISTINCT */

	/*
	 * These values are working state that is initialized at the start of an
	 * input tuple group and updated for each input tuple.
	 *
	 * For a simple (non DISTINCT/ORDER BY) aggregate, we just feed the input
	 * values straight to the transition function.  If it's DISTINCT or
	 * requires ORDER BY, we pass the input values into a Tuplesort object;
	 * then at completion of the input tuple group, we scan the sorted values,
	 * eliminate duplicates if needed, and run the transition function on the
	 * rest.
	 */

	Tuplesortstate **sortstate;	/* sort object, if DISTINCT or ORDER BY */

	/*
	 * This field is a pre-initialized FunctionCallInfo struct used for
	 * calling this aggregate's transfn.  We save a few cycles per row by not
	 * re-initializing the unchanging fields; which isn't much, but it seems
	 * worth the extra space consumption.
	 */
	FunctionCallInfoData transfn_fcinfo;
}	AggStatePerAggData;

/*
 * AggStatePerGroupData - per-aggregate-per-group working state
 *
 * These values are working state that is initialized at the start of
 * an input tuple group and updated for each input tuple.
 *
 * In AGG_PLAIN and AGG_SORTED modes, we have a single array of these
 * structs (pointed to by aggstate->pergroup); we re-use the array for
 * each input group, if it's AGG_SORTED mode.  In AGG_HASHED mode, the
 * hash table contains an array of these structs for each tuple group.
 *
 * Logically, the sortstate field belongs in this struct, but we do not
 * keep it here for space reasons: we don't support DISTINCT aggregates
 * in AGG_HASHED mode, so there's no reason to use up a pointer field
 * in every entry of the hashtable.
 */
typedef struct AggStatePerGroupData
{
	Datum		transValue;		/* current transition value */
	bool		transValueIsNull;

	bool		noTransValue;	/* true if transValue not set yet */

	/*
	 * Note: noTransValue initially has the same value as transValueIsNull,
	 * and if true both are cleared to false at the same time.  They are not
	 * the same though: if transfn later returns a NULL, we want to keep that
	 * NULL and not auto-replace it with a later input value. Only the first
	 * non-NULL input will be auto-substituted.
	 */
} AggStatePerGroupData;

/*
 * To implement hashed aggregation, we need a hashtable that stores a
 * representative tuple and an array of AggStatePerGroup structs for each
 * distinct set of GROUP BY column values.  We compute the hash key from
 * the GROUP BY columns.
 */
typedef struct AggHashEntryData *AggHashEntry;

typedef struct AggHashEntryData
{
	TupleHashEntryData shared;	/* common header for hash table entries */
	/* per-aggregate transition status array - must be last! */
	AggStatePerGroupData pergroup[1];	/* VARIABLE LENGTH ARRAY */
}	AggHashEntryData;	/* VARIABLE LENGTH STRUCT */


static void initialize_aggregates(AggState *aggstate,
					  AggStatePerAgg peragg,
					  AggStatePerGroup pergroup,
					  int numReinitialize);
static void advance_transition_function(AggState *aggstate,
							AggStatePerAgg peraggstate,
							AggStatePerGroup pergroupstate);
static void advance_aggregates(AggState *aggstate, AggStatePerGroup pergroup);
static void process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerAgg peraggstate,
								 AggStatePerGroup pergroupstate);
static void process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerAgg peraggstate,
								AggStatePerGroup pergroupstate);
static void finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peraggstate,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull);
static Bitmapset *find_unaggregated_cols(AggState *aggstate);
static bool find_unaggregated_cols_walker(Node *node, Bitmapset **colnos);
static void build_hash_table(AggState *aggstate);
static AggHashEntry lookup_hash_entry(AggState *aggstate,
				  TupleTableSlot *inputslot);
static TupleTableSlot *agg_retrieve_direct(AggState *aggstate);
static TupleTableSlot *agg_retrieve_chained(AggState *aggstate);
static void agg_fill_hash_table(AggState *aggstate);
static TupleTableSlot *agg_retrieve_hash_table(AggState *aggstate);
static Datum GetAggInitVal(Datum textInitVal, Oid transtype);


/*
 * Initialize all aggregates for a new group of input values.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
initialize_aggregates(AggState *aggstate,
					  AggStatePerAgg peragg,
					  AggStatePerGroup pergroup,
					  int numReinitialize)
{
	int			aggno;
	int         numGroupingSets = Max(aggstate->numsets, 1);
	int         i = 0;

	if (numReinitialize < 1)
		numReinitialize = numGroupingSets;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &peragg[aggno];

		/*
		 * Start a fresh sort operation for each DISTINCT/ORDER BY aggregate.
		 */
		if (peraggstate->numSortCols > 0)
		{
			for (i = 0; i < numReinitialize; i++)
			{
				/*
				 * In case of rescan, maybe there could be an uncompleted sort
				 * operation?  Clean it up if so.
				 */
				if (peraggstate->sortstate[i])
					tuplesort_end(peraggstate->sortstate[i]);

				/*
				 * We use a plain Datum sorter when there's a single input column;
				 * otherwise sort the full tuple.  (See comments for
				 * process_ordered_aggregate_single.)
				 */
				peraggstate->sortstate[i] =
					(peraggstate->numInputs == 1) ?
					tuplesort_begin_datum(peraggstate->evaldesc->attrs[0]->atttypid,
										  peraggstate->sortOperators[0],
										  peraggstate->sortCollations[0],
										  peraggstate->sortNullsFirst[0],
										  work_mem, false) :
					tuplesort_begin_heap(peraggstate->evaldesc,
										 peraggstate->numSortCols,
										 peraggstate->sortColIdx,
										 peraggstate->sortOperators,
										 peraggstate->sortCollations,
										 peraggstate->sortNullsFirst,
										 work_mem, false);
			}
		}

		/* If ROLLUP is present, we need to iterate over all the groups
		 * that are present with the current aggstate. If ROLLUP is not
		 * present, we only have one groupstate associated with the
		 * current aggstate.
		 */

		for (i = 0; i < numReinitialize; i++)
		{
			AggStatePerGroup pergroupstate = &pergroup[aggno + (i * (aggstate->numaggs))];

			/*
			 * (Re)set transValue to the initial value.
			 *
			 * Note that when the initial value is pass-by-ref, we must copy it
			 * (into the aggcontext) since we will pfree the transValue later.
			 */
			if (peraggstate->initValueIsNull)
				pergroupstate->transValue = peraggstate->initValue;
			else
			{
				MemoryContext oldContext;

				oldContext = MemoryContextSwitchTo(aggstate->aggcontext[i]->ecxt_per_tuple_memory);
				pergroupstate->transValue = datumCopy(peraggstate->initValue,
													  peraggstate->transtypeByVal,
													  peraggstate->transtypeLen);
				MemoryContextSwitchTo(oldContext);
			}
			pergroupstate->transValueIsNull = peraggstate->initValueIsNull;

			/*
			 * If the initial value for the transition state doesn't exist in the
			 * pg_aggregate table then we will let the first non-NULL value
			 * returned from the outer procNode become the initial value. (This is
			 * useful for aggregates like max() and min().) The noTransValue flag
			 * signals that we still need to do this.
			 */
			pergroupstate->noTransValue = peraggstate->initValueIsNull;
		}
	}
}

/*
 * Given new input value(s), advance the transition function of one aggregate
 * within one grouping set only (already set in aggstate->current_set)
 *
 * The new values (and null flags) have been preloaded into argument positions
 * 1 and up in peraggstate->transfn_fcinfo, so that we needn't copy them again
 * to pass to the transition function.  We also expect that the static fields
 * of the fcinfo are already initialized; that was done by ExecInitAgg().
 *
 * It doesn't matter which memory context this is called in.
 */
static void
advance_transition_function(AggState *aggstate,
							AggStatePerAgg peraggstate,
							AggStatePerGroup pergroupstate)
{
	FunctionCallInfo fcinfo = &peraggstate->transfn_fcinfo;
	MemoryContext oldContext;
	Datum		newVal;

	if (peraggstate->transfn.fn_strict)
	{
		/*
		 * For a strict transfn, nothing happens when there's a NULL input; we
		 * just keep the prior transValue.
		 */
		int			numTransInputs = peraggstate->numTransInputs;
		int			i;

		for (i = 1; i <= numTransInputs; i++)
		{
			if (fcinfo->argnull[i])
				return;
		}
		if (pergroupstate->noTransValue)
		{
			/*
			 * transValue has not been initialized. This is the first non-NULL
			 * input value. We use it as the initial value for transValue. (We
			 * already checked that the agg's input type is binary-compatible
			 * with its transtype, so straight copy here is OK.)
			 *
			 * We must copy the datum into aggcontext if it is pass-by-ref. We
			 * do not need to pfree the old transValue, since it's NULL.
			 */
			oldContext = MemoryContextSwitchTo(aggstate->aggcontext[aggstate->current_set]->ecxt_per_tuple_memory);
			pergroupstate->transValue = datumCopy(fcinfo->arg[1],
												  peraggstate->transtypeByVal,
												  peraggstate->transtypeLen);
			pergroupstate->transValueIsNull = false;
			pergroupstate->noTransValue = false;
			MemoryContextSwitchTo(oldContext);
			return;
		}
		if (pergroupstate->transValueIsNull)
		{
			/*
			 * Don't call a strict function with NULL inputs.  Note it is
			 * possible to get here despite the above tests, if the transfn is
			 * strict *and* returned a NULL on a prior cycle. If that happens
			 * we will propagate the NULL all the way to the end.
			 */
			return;
		}
	}

	/* We run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	/* set up aggstate->curperagg for AggGetAggref() */
	aggstate->curperagg = peraggstate;

	/*
	 * OK to call the transition function
	 */
	fcinfo->arg[0] = pergroupstate->transValue;
	fcinfo->argnull[0] = pergroupstate->transValueIsNull;
	fcinfo->isnull = false;		/* just in case transfn doesn't set it */

	newVal = FunctionCallInvoke(fcinfo);

	aggstate->curperagg = NULL;

	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * pfree the prior transValue.  But if transfn returned a pointer to its
	 * first input, we don't need to do anything.
	 */
	if (!peraggstate->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
	{
		if (!fcinfo->isnull)
		{
			MemoryContextSwitchTo(aggstate->aggcontext[aggstate->current_set]->ecxt_per_tuple_memory);
			newVal = datumCopy(newVal,
							   peraggstate->transtypeByVal,
							   peraggstate->transtypeLen);
		}
		if (!pergroupstate->transValueIsNull)
			pfree(DatumGetPointer(pergroupstate->transValue));
	}

	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/*
 * Advance all the aggregates for one input tuple.  The input tuple
 * has been stored in tmpcontext->ecxt_outertuple, so that it is accessible
 * to ExecEvalExpr.  pergroup is the array of per-group structs to use
 * (this might be in a hashtable entry).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
advance_aggregates(AggState *aggstate, AggStatePerGroup pergroup)
{
	int			aggno;
	int         groupno = 0;
	int         numGroupingSets = Max(aggstate->numsets, 1);
	int         numAggs = aggstate->numaggs;

	for (aggno = 0; aggno < numAggs; aggno++)
	{
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		ExprState  *filter = peraggstate->aggrefstate->aggfilter;
		int			numTransInputs = peraggstate->numTransInputs;
		int			i;
		TupleTableSlot *slot;

		/* Skip anything FILTERed out */
		if (filter)
		{
			Datum		res;
			bool		isnull;

			res = ExecEvalExprSwitchContext(filter, aggstate->tmpcontext,
											&isnull, NULL);
			if (isnull || !DatumGetBool(res))
				continue;
		}

		/* Evaluate the current input expressions for this aggregate */
		slot = ExecProject(peraggstate->evalproj, NULL);

		if (peraggstate->numSortCols > 0)
		{
			/* DISTINCT and/or ORDER BY case */
			Assert(slot->tts_nvalid == peraggstate->numInputs);

			/*
			 * If the transfn is strict, we want to check for nullity before
			 * storing the row in the sorter, to save space if there are a lot
			 * of nulls.  Note that we must only check numTransInputs columns,
			 * not numInputs, since nullity in columns used only for sorting
			 * is not relevant here.
			 */
			if (peraggstate->transfn.fn_strict)
			{
				for (i = 0; i < numTransInputs; i++)
				{
					if (slot->tts_isnull[i])
						break;
				}
				if (i < numTransInputs)
					continue;
			}

			for (groupno = 0; groupno < numGroupingSets; groupno++)
			{
				/* OK, put the tuple into the tuplesort object */
				if (peraggstate->numInputs == 1)
					tuplesort_putdatum(peraggstate->sortstate[groupno],
									   slot->tts_values[0],
									   slot->tts_isnull[0]);
				else
					tuplesort_puttupleslot(peraggstate->sortstate[groupno], slot);
			}
		}
		else
		{
			/* We can apply the transition function immediately */
			FunctionCallInfo fcinfo = &peraggstate->transfn_fcinfo;

			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			Assert(slot->tts_nvalid >= numTransInputs);
			for (i = 0; i < numTransInputs; i++)
			{
				fcinfo->arg[i + 1] = slot->tts_values[i];
				fcinfo->argnull[i + 1] = slot->tts_isnull[i];
			}

			for (groupno = 0; groupno < numGroupingSets; groupno++)
			{
				AggStatePerGroup pergroupstate = &pergroup[aggno + (groupno * numAggs)];

				aggstate->current_set = groupno;

				advance_transition_function(aggstate, peraggstate, pergroupstate);
			}
		}
	}
}


/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with only one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * Note that the strictness of the transition function was checked when
 * entering the values into the sort, so we don't check it again here;
 * we just apply standard SQL DISTINCT logic.
 *
 * The one-input case is handled separately from the multi-input case
 * for performance reasons: for single by-value inputs, such as the
 * common case of count(distinct id), the tuplesort_getdatum code path
 * is around 300% faster.  (The speedup for by-reference types is less
 * but still noticeable.)
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_single(AggState *aggstate,
								 AggStatePerAgg peraggstate,
								 AggStatePerGroup pergroupstate)
{
	Datum		oldVal = (Datum) 0;
	bool		oldIsNull = true;
	bool		haveOldVal = false;
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	MemoryContext oldContext;
	bool		isDistinct = (peraggstate->numDistinctCols > 0);
	FunctionCallInfo fcinfo = &peraggstate->transfn_fcinfo;
	Datum	   *newVal;
	bool	   *isNull;

	Assert(peraggstate->numDistinctCols < 2);

	tuplesort_performsort(peraggstate->sortstate[aggstate->current_set]);

	/* Load the column into argument 1 (arg 0 will be transition value) */
	newVal = fcinfo->arg + 1;
	isNull = fcinfo->argnull + 1;

	/*
	 * Note: if input type is pass-by-ref, the datums returned by the sort are
	 * freshly palloc'd in the per-query context, so we must be careful to
	 * pfree them when they are no longer needed.
	 */

	while (tuplesort_getdatum(peraggstate->sortstate[aggstate->current_set], true,
							  newVal, isNull))
	{
		/*
		 * Clear and select the working context for evaluation of the equality
		 * function and transition function.
		 */
		MemoryContextReset(workcontext);
		oldContext = MemoryContextSwitchTo(workcontext);

		/*
		 * If DISTINCT mode, and not distinct from prior, skip it.
		 *
		 * Note: we assume equality functions don't care about collation.
		 */
		if (isDistinct &&
			haveOldVal &&
			((oldIsNull && *isNull) ||
			 (!oldIsNull && !*isNull &&
			  DatumGetBool(FunctionCall2(&peraggstate->equalfns[0],
										 oldVal, *newVal)))))
		{
			/* equal to prior, so forget this one */
			if (!peraggstate->inputtypeByVal && !*isNull)
				pfree(DatumGetPointer(*newVal));
		}
		else
		{
			advance_transition_function(aggstate, peraggstate, pergroupstate);
			/* forget the old value, if any */
			if (!oldIsNull && !peraggstate->inputtypeByVal)
				pfree(DatumGetPointer(oldVal));
			/* and remember the new one for subsequent equality checks */
			oldVal = *newVal;
			oldIsNull = *isNull;
			haveOldVal = true;
		}

		MemoryContextSwitchTo(oldContext);
	}

	if (!oldIsNull && !peraggstate->inputtypeByVal)
		pfree(DatumGetPointer(oldVal));

	tuplesort_end(peraggstate->sortstate[aggstate->current_set]);
	peraggstate->sortstate[aggstate->current_set] = NULL;
}

/*
 * Run the transition function for a DISTINCT or ORDER BY aggregate
 * with more than one input.  This is called after we have completed
 * entering all the input values into the sort object.  We complete the
 * sort, read out the values in sorted order, and run the transition
 * function on each value (applying DISTINCT if appropriate).
 *
 * This function handles only one grouping set (already set in
 * aggstate->current_set).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static void
process_ordered_aggregate_multi(AggState *aggstate,
								AggStatePerAgg peraggstate,
								AggStatePerGroup pergroupstate)
{
	MemoryContext workcontext = aggstate->tmpcontext->ecxt_per_tuple_memory;
	FunctionCallInfo fcinfo = &peraggstate->transfn_fcinfo;
	TupleTableSlot *slot1 = peraggstate->evalslot;
	TupleTableSlot *slot2 = peraggstate->uniqslot;
	int			numTransInputs = peraggstate->numTransInputs;
	int			numDistinctCols = peraggstate->numDistinctCols;
	bool		haveOldValue = false;
	int			i;

	tuplesort_performsort(peraggstate->sortstate[aggstate->current_set]);

	ExecClearTuple(slot1);
	if (slot2)
		ExecClearTuple(slot2);

	while (tuplesort_gettupleslot(peraggstate->sortstate[aggstate->current_set], true, slot1))
	{
		/*
		 * Extract the first numTransInputs columns as datums to pass to the
		 * transfn.  (This will help execTuplesMatch too, so we do it
		 * immediately.)
		 */
		slot_getsomeattrs(slot1, numTransInputs);

		if (numDistinctCols == 0 ||
			!haveOldValue ||
			!execTuplesMatch(slot1, slot2,
							 numDistinctCols,
							 peraggstate->sortColIdx,
							 peraggstate->equalfns,
							 workcontext))
		{
			/* Load values into fcinfo */
			/* Start from 1, since the 0th arg will be the transition value */
			for (i = 0; i < numTransInputs; i++)
			{
				fcinfo->arg[i + 1] = slot1->tts_values[i];
				fcinfo->argnull[i + 1] = slot1->tts_isnull[i];
			}

			advance_transition_function(aggstate, peraggstate, pergroupstate);

			if (numDistinctCols > 0)
			{
				/* swap the slot pointers to retain the current tuple */
				TupleTableSlot *tmpslot = slot2;

				slot2 = slot1;
				slot1 = tmpslot;
				haveOldValue = true;
			}
		}

		/* Reset context each time, unless execTuplesMatch did it for us */
		if (numDistinctCols == 0)
			MemoryContextReset(workcontext);

		ExecClearTuple(slot1);
	}

	if (slot2)
		ExecClearTuple(slot2);

	tuplesort_end(peraggstate->sortstate[aggstate->current_set]);
	peraggstate->sortstate[aggstate->current_set] = NULL;
}

/*
 * Compute the final value of one aggregate.
 *
 * The finalfunction will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
static void
finalize_aggregate(AggState *aggstate,
				   AggStatePerAgg peraggstate,
				   AggStatePerGroup pergroupstate,
				   Datum *resultVal, bool *resultIsNull)
{
	FunctionCallInfoData fcinfo;
	bool		anynull = false;
	MemoryContext oldContext;
	int			i;
	ListCell   *lc;

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Evaluate any direct arguments.  We do this even if there's no finalfn
	 * (which is unlikely anyway), so that side-effects happen as expected.
	 * The direct arguments go into arg positions 1 and up, leaving position 0
	 * for the transition state value.
	 */
	i = 1;
	foreach(lc, peraggstate->aggrefstate->aggdirectargs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		fcinfo.arg[i] = ExecEvalExpr(expr,
									 aggstate->ss.ps.ps_ExprContext,
									 &fcinfo.argnull[i],
									 NULL);
		anynull |= fcinfo.argnull[i];
		i++;
	}

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peraggstate->finalfn_oid))
	{
		int			numFinalArgs = peraggstate->numFinalArgs;

		/* set up aggstate->curperagg for AggGetAggref() */
		aggstate->curperagg = peraggstate;

		InitFunctionCallInfoData(fcinfo, &peraggstate->finalfn,
								 numFinalArgs,
								 peraggstate->aggCollation,
								 (void *) aggstate, NULL);

		/* Fill in the transition state value */
		fcinfo.arg[0] = pergroupstate->transValue;
		fcinfo.argnull[0] = pergroupstate->transValueIsNull;
		anynull |= pergroupstate->transValueIsNull;

		/* Fill any remaining argument positions with nulls */
		for (; i < numFinalArgs; i++)
		{
			fcinfo.arg[i] = (Datum) 0;
			fcinfo.argnull[i] = true;
			anynull = true;
		}

		if (fcinfo.flinfo->fn_strict && anynull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(&fcinfo);
			*resultIsNull = fcinfo.isnull;
		}
		aggstate->curperagg = NULL;
	}
	else
	{
		*resultVal = pergroupstate->transValue;
		*resultIsNull = pergroupstate->transValueIsNull;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peraggstate->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContains(CurrentMemoryContext,
							   DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
							   peraggstate->resulttypeByVal,
							   peraggstate->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * find_unaggregated_cols
 *	  Construct a bitmapset of the column numbers of un-aggregated Vars
 *	  appearing in our targetlist and qual (HAVING clause)
 */
static Bitmapset *
find_unaggregated_cols(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset  *colnos;

	colnos = NULL;
	(void) find_unaggregated_cols_walker((Node *) node->plan.targetlist,
										 &colnos);
	(void) find_unaggregated_cols_walker((Node *) node->plan.qual,
										 &colnos);
	return colnos;
}

static bool
find_unaggregated_cols_walker(Node *node, Bitmapset **colnos)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;

		/* setrefs.c should have set the varno to OUTER_VAR */
		Assert(var->varno == OUTER_VAR);
		Assert(var->varlevelsup == 0);
		*colnos = bms_add_member(*colnos, var->varattno);
		return false;
	}
	if (IsA(node, Aggref) || IsA(node, GroupingFunc))
		/* do not descend into aggregate exprs */
		return false;
	return expression_tree_walker(node, find_unaggregated_cols_walker,
								  (void *) colnos);
}

/*
 * Initialize the hash table to empty.
 *
 * The hash table always lives in the aggcontext memory context.
 */
static void
build_hash_table(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	MemoryContext tmpmem = aggstate->tmpcontext->ecxt_per_tuple_memory;
	Size		entrysize;

	Assert(node->aggstrategy == AGG_HASHED);
	Assert(node->numGroups > 0);

	entrysize = sizeof(AggHashEntryData) +
		(aggstate->numaggs - 1) * sizeof(AggStatePerGroupData);

	aggstate->hashtable = BuildTupleHashTable(node->numCols,
											  node->grpColIdx,
											  aggstate->eqfunctions,
											  aggstate->hashfunctions,
											  node->numGroups,
											  entrysize,
											  aggstate->aggcontext[0]->ecxt_per_tuple_memory,
											  tmpmem);
}

/*
 * Create a list of the tuple columns that actually need to be stored in
 * hashtable entries.  The incoming tuples from the child plan node will
 * contain grouping columns, other columns referenced in our targetlist and
 * qual, columns used to compute the aggregate functions, and perhaps just
 * junk columns we don't use at all.  Only columns of the first two types
 * need to be stored in the hashtable, and getting rid of the others can
 * make the table entries significantly smaller.  To avoid messing up Var
 * numbering, we keep the same tuple descriptor for hashtable entries as the
 * incoming tuples have, but set unwanted columns to NULL in the tuples that
 * go into the table.
 *
 * To eliminate duplicates, we build a bitmapset of the needed columns, then
 * convert it to an integer list (cheaper to scan at runtime). The list is
 * in decreasing order so that the first entry is the largest;
 * lookup_hash_entry depends on this to use slot_getsomeattrs correctly.
 * Note that the list is preserved over ExecReScanAgg, so we allocate it in
 * the per-query context (unlike the hash table itself).
 *
 * Note: at present, searching the tlist/qual is not really necessary since
 * the parser should disallow any unaggregated references to ungrouped
 * columns.  However, the search will be needed when we add support for
 * SQL99 semantics that allow use of "functionally dependent" columns that
 * haven't been explicitly grouped by.
 */
static List *
find_hash_columns(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	Bitmapset  *colnos;
	List	   *collist;
	int			i;

	/* Find Vars that will be needed in tlist and qual */
	colnos = find_unaggregated_cols(aggstate);
	/* Add in all the grouping columns */
	for (i = 0; i < node->numCols; i++)
		colnos = bms_add_member(colnos, node->grpColIdx[i]);
	/* Convert to list, using lcons so largest element ends up first */
	collist = NIL;
	while ((i = bms_first_member(colnos)) >= 0)
		collist = lcons_int(i, collist);
	bms_free(colnos);

	return collist;
}

/*
 * Estimate per-hash-table-entry overhead for the planner.
 *
 * Note that the estimate does not include space for pass-by-reference
 * transition data values, nor for the representative tuple of each group.
 */
Size
hash_agg_entry_size(int numAggs)
{
	Size		entrysize;

	/* This must match build_hash_table */
	entrysize = sizeof(AggHashEntryData) +
		(numAggs - 1) * sizeof(AggStatePerGroupData);
	entrysize = MAXALIGN(entrysize);
	/* Account for hashtable overhead (assuming fill factor = 1) */
	entrysize += 3 * sizeof(void *);
	return entrysize;
}

/*
 * Find or create a hashtable entry for the tuple group containing the
 * given tuple.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
static AggHashEntry
lookup_hash_entry(AggState *aggstate, TupleTableSlot *inputslot)
{
	TupleTableSlot *hashslot = aggstate->hashslot;
	ListCell   *l;
	AggHashEntry entry;
	bool		isnew;

	/* if first time through, initialize hashslot by cloning input slot */
	if (hashslot->tts_tupleDescriptor == NULL)
	{
		ExecSetSlotDescriptor(hashslot, inputslot->tts_tupleDescriptor);
		/* Make sure all unused columns are NULLs */
		ExecStoreAllNullTuple(hashslot);
	}

	/* transfer just the needed columns into hashslot */
	slot_getsomeattrs(inputslot, linitial_int(aggstate->hash_needed));
	foreach(l, aggstate->hash_needed)
	{
		int			varNumber = lfirst_int(l) - 1;

		hashslot->tts_values[varNumber] = inputslot->tts_values[varNumber];
		hashslot->tts_isnull[varNumber] = inputslot->tts_isnull[varNumber];
	}

	/* find or create the hashtable entry using the filtered tuple */
	entry = (AggHashEntry) LookupTupleHashEntry(aggstate->hashtable,
												hashslot,
												&isnew);

	if (isnew)
	{
		/* initialize aggregates for new tuple group */
		initialize_aggregates(aggstate, aggstate->peragg, entry->pergroup, 0);
	}

	return entry;
}

/*
 * ExecAgg -
 *
 *	  ExecAgg receives tuples from its outer subplan and aggregates over
 *	  the appropriate attribute for each aggregate function use (Aggref
 *	  node) appearing in the targetlist or qual of the node.  The number
 *	  of tuples to aggregate over depends on whether grouped or plain
 *	  aggregation is selected.  In grouped aggregation, we produce a result
 *	  row for each group; in plain aggregation there's a single result row
 *	  for the whole query.  In either case, the value of each aggregate is
 *	  stored in the expression context to be used when ExecProject evaluates
 *	  the result tuple.
 */
TupleTableSlot *
ExecAgg(AggState *node)
{
	TupleTableSlot *result;

	/*
	 * Check to see if we're still projecting out tuples from a previous agg
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->ss.ps.ps_TupFromTlist)
	{
		ExprDoneCond isDone;

		result = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return result;
		/* Done with that source tuple... */
		node->ss.ps.ps_TupFromTlist = false;
	}

	/*
	 * (We must do the ps_TupFromTlist check first, because in some cases
	 * agg_done gets set before we emit the final aggregate tuple, and we have
	 * to finish running SRFs for it.)
	 */

	if (!node->agg_done)
	{
		/* Dispatch based on strategy */
		switch (((Agg *) node->ss.ps.plan)->aggstrategy)
		{
			case AGG_HASHED:
				if (!node->table_filled)
					agg_fill_hash_table(node);
				result = agg_retrieve_hash_table(node);
				break;
			case AGG_CHAINED:
				result = agg_retrieve_chained(node);
				break;
			default:
				result = agg_retrieve_direct(node);
				break;
		}

		if (!TupIsNull(result))
			return result;
	}

	if (!node->chain_done)
	{
		Assert(node->chain_tuplestore);
		result = node->ss.ps.ps_ResultTupleSlot;
		ExecClearTuple(result);
		if (tuplestore_gettupleslot(node->chain_tuplestore,
									true, false, result))
			return result;
		node->chain_done = true;
	}

	return NULL;
}

/*
 * ExecAgg for non-hashed case
 */
static TupleTableSlot *
agg_retrieve_direct(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	PlanState  *outerPlan;
	ExprContext *econtext;
	ExprContext *tmpcontext;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleTableSlot *outerslot;
	TupleTableSlot *firstSlot;
	int			   aggno;
	bool           hasRollup = aggstate->numsets > 0;
	int            numGroupingSets = Max(aggstate->numsets, 1);
	int            currentGroupingSet = 0;
	int            currentGSSize = 0;
	int            numReset = 1;
	int            i;

	/*
	 * get state info from node
	 */
	outerPlan = outerPlanState(aggstate);
	/* econtext is the per-output-tuple expression context */
	econtext = aggstate->ss.ps.ps_ExprContext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;
	/* tmpcontext is the per-input-tuple expression context */
	tmpcontext = aggstate->tmpcontext;
	peragg = aggstate->peragg;
	pergroup = aggstate->pergroup;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one matching
	 * aggstate->ss.ps.qual
	 *
	 * For grouping sets, we have the invariant that aggstate->projected_set is
	 * either -1 (initial call) or the index (starting from 0) in gset_lengths
	 * for the group we just completed (either by projecting a row or by
	 * discarding it in the qual).
	 */
	while (!aggstate->agg_done)
	{
		/*
		 * Clear the per-output-tuple context for each group, as well as
		 * aggcontext (which contains any pass-by-ref transvalues of the old
		 * group).  We also clear any child contexts of the aggcontext; some
		 * aggregate functions store working state in such contexts.
		 *
		 * We use ReScanExprContext not just ResetExprContext because we want
		 * any registered shutdown callbacks to be called.	That allows
		 * aggregate functions to ensure they've cleaned up any non-memory
		 * resources.
		 */
		ReScanExprContext(econtext);

		if (aggstate->projected_set >= 0 && aggstate->projected_set < numGroupingSets)
			numReset = aggstate->projected_set + 1;
		else
			numReset = numGroupingSets;

		for (i = 0; i < numReset; i++)
		{
			ReScanExprContext(aggstate->aggcontext[i]);
			MemoryContextDeleteChildren(aggstate->aggcontext[i]->ecxt_per_tuple_memory);
		}

		/* Check if input is complete and there are no more groups to project. */
		if (aggstate->input_done == true
			&& aggstate->projected_set >= (numGroupingSets - 1))
		{
			aggstate->agg_done = true;
			break;
		}

		if (aggstate->projected_set >= 0 && aggstate->projected_set < (numGroupingSets - 1))
			currentGSSize = aggstate->gset_lengths[aggstate->projected_set + 1];
		else
			currentGSSize = 0;

		/*-
		 * If a subgroup for the current grouping set is present, project it.
		 *
		 * We have a new group if:
		 *  - we're out of input but haven't projected all grouping sets
		 *    (checked above)
		 * OR
		 *    - we already projected a row that wasn't from the last grouping
		 *      set
		 *    AND
		 *    - the next grouping set has at least one grouping column (since
		 *      empty grouping sets project only once input is exhausted)
		 *    AND
		 *    - the previous and pending rows differ on the grouping columns
		 *      of the next grouping set
		 */
		if (aggstate->input_done
			|| (node->aggstrategy == AGG_SORTED
				&& aggstate->projected_set != -1
				&& aggstate->projected_set < (numGroupingSets - 1)
				&& currentGSSize > 0
				&& !execTuplesMatch(econtext->ecxt_outertuple,
									tmpcontext->ecxt_outertuple,
									currentGSSize,
									node->grpColIdx,
									aggstate->eqfunctions,
									tmpcontext->ecxt_per_tuple_memory)))
		{
			++aggstate->projected_set;

			Assert(aggstate->projected_set < numGroupingSets);
			Assert(currentGSSize > 0 || aggstate->input_done);
		}
		else
		{
			/*
			 * We no longer care what group we just projected, the next
			 * projection will always be the first (or only) grouping set
			 * (unless the input proves to be empty).
			 */
			aggstate->projected_set = 0;

			/*
			 * If we don't already have the first tuple of the new group, fetch
			 * it from the outer plan.
			 */
			if (aggstate->grp_firstTuple == NULL)
			{
				outerslot = ExecProcNode(outerPlan);
				if (!TupIsNull(outerslot))
				{
					/*
					 * Make a copy of the first input tuple; we will use this for
					 * comparisons (in group mode) and for projection.
					 */
					aggstate->grp_firstTuple = ExecCopySlotTuple(outerslot);
				}
				else
				{
					/* outer plan produced no tuples at all */
					if (hasRollup)
					{
						/*
						 * If there was no input at all, we need to project
						 * rows only if there are grouping sets of size 0.
						 * Note that this implies that there can't be any
						 * references to ungrouped Vars, which would otherwise
						 * cause issues with the empty output slot.
						 */
						aggstate->input_done = true;

						while (aggstate->gset_lengths[aggstate->projected_set] > 0)
						{
							aggstate->projected_set += 1;
							if (aggstate->projected_set >= numGroupingSets)
							{
								aggstate->agg_done = true;
								return NULL;
							}
						}
					}
					else
					{
						aggstate->agg_done = true;
						/* If we are grouping, we should produce no tuples too */
						if (node->aggstrategy != AGG_PLAIN)
							return NULL;
					}
				}
			}

			/*
			 * Initialize working state for a new input tuple group.
			 */
			initialize_aggregates(aggstate, peragg, pergroup, numReset);

			if (aggstate->grp_firstTuple != NULL)
			{
				/*
				 * Store the copied first input tuple in the tuple table slot
				 * reserved for it.  The tuple will be deleted when it is cleared
				 * from the slot.
				 */
				ExecStoreTuple(aggstate->grp_firstTuple,
							   firstSlot,
							   InvalidBuffer,
							   true);
				aggstate->grp_firstTuple = NULL;	/* don't keep two pointers */

				/* set up for first advance_aggregates call */
				tmpcontext->ecxt_outertuple = firstSlot;

				/*
				 * Process each outer-plan tuple, and then fetch the next one,
				 * until we exhaust the outer plan or cross a group boundary.
				 */
				for (;;)
				{
					advance_aggregates(aggstate, pergroup);

					/* Reset per-input-tuple context after each tuple */
					ResetExprContext(tmpcontext);

					outerslot = ExecProcNode(outerPlan);
					if (TupIsNull(outerslot))
					{
						/* no more outer-plan tuples available */
						if (hasRollup)
						{
							aggstate->input_done = true;
							break;
						}
						else
						{
							aggstate->agg_done = true;
							break;
						}
					}
					/* set up for next advance_aggregates call */
					tmpcontext->ecxt_outertuple = outerslot;

					/*
					 * If we are grouping, check whether we've crossed a group
					 * boundary.
					 */
					if (node->aggstrategy == AGG_SORTED)
					{
						if (!execTuplesMatch(firstSlot,
											 outerslot,
											 node->numCols,
											 node->grpColIdx,
											 aggstate->eqfunctions,
											 tmpcontext->ecxt_per_tuple_memory))
						{
							aggstate->grp_firstTuple = ExecCopySlotTuple(outerslot);
							break;
						}
					}
				}
			}

			/*
			 * Use the representative input tuple for any references to
			 * non-aggregated input columns in aggregate direct args, the node
			 * qual, and the tlist.  (If we are not grouping, and there are no
			 * input rows at all, we will come here with an empty firstSlot ...
			 * but if not grouping, there can't be any references to
			 * non-aggregated input columns, so no problem.)
			 */
			econtext->ecxt_outertuple = firstSlot;
		}

		Assert(aggstate->projected_set >= 0);

		aggstate->current_set = currentGroupingSet = aggstate->projected_set;

		if (hasRollup)
			econtext->grouped_cols = aggstate->grouped_cols[currentGroupingSet];

		for (aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &peragg[aggno];
			AggStatePerGroup pergroupstate;

			pergroupstate = &pergroup[aggno + (currentGroupingSet * (aggstate->numaggs))];

			if (peraggstate->numSortCols > 0)
			{
				if (peraggstate->numInputs == 1)
					process_ordered_aggregate_single(aggstate,
													 peraggstate,
													 pergroupstate);
				else
					process_ordered_aggregate_multi(aggstate,
													peraggstate,
													pergroupstate);
			}

			finalize_aggregate(aggstate, peraggstate, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
		}

		/*
		 * Check the qual (HAVING clause); if the group does not match, ignore
		 * it and loop back to try to process another group.
		 */
		if (ExecQual(aggstate->ss.ps.qual, econtext, false))
		{
			/*
			 * Form and return a projection tuple using the aggregate results
			 * and the representative input tuple.
			 */
			TupleTableSlot *result;
			ExprDoneCond isDone;

			result = ExecProject(aggstate->ss.ps.ps_ProjInfo, &isDone);

			if (isDone != ExprEndResult)
			{
				aggstate->ss.ps.ps_TupFromTlist =
					(isDone == ExprMultipleResult);
				return result;
			}
		}
		else
			InstrCountFiltered1(aggstate, 1);
	}

	/* No more groups */
	return NULL;
}


/*
 * ExecAgg for chained case (pullthrough mode)
 */
static TupleTableSlot *
agg_retrieve_chained(AggState *aggstate)
{
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	ExprContext *tmpcontext = aggstate->tmpcontext;
	Datum	   *aggvalues = econtext->ecxt_aggvalues;
	bool	   *aggnulls = econtext->ecxt_aggnulls;
	AggStatePerAgg peragg = aggstate->peragg;
	AggStatePerGroup pergroup = aggstate->pergroup;
	TupleTableSlot *outerslot;
	TupleTableSlot *firstSlot = aggstate->ss.ss_ScanTupleSlot;
	int			   aggno;
	int            numGroupingSets = Max(aggstate->numsets, 1);
	int            currentSet = 0;

	/*
	 * The invariants here are:
	 *
	 *  - when called, we've already projected every result that
	 * might have been generated by previous rows, and if this is not
	 * the first row, then grp_firsttuple has the representative input
	 * row.
	 *
	 *  - we must pull the outer plan exactly once and return that tuple. If
	 * the outer plan ends, we project whatever needs projecting.
	 */

	outerslot = ExecProcNode(outerPlanState(aggstate));

	/*
	 * If this is the first row and it's empty, nothing to do.
	 */

	if (TupIsNull(firstSlot) && TupIsNull(outerslot))
	{
		aggstate->agg_done = true;
		return outerslot;
	}

	/*
	 * See if we need to project anything. (We don't need to worry about
	 * grouping sets of size 0, the planner doesn't give us those.)
	 */

	econtext->ecxt_outertuple = firstSlot;

	while (!TupIsNull(firstSlot)
		   && (TupIsNull(outerslot)
			   || !execTuplesMatch(firstSlot,
								   outerslot,
								   aggstate->gset_lengths[currentSet],
								   node->grpColIdx,
								   aggstate->eqfunctions,
								   tmpcontext->ecxt_per_tuple_memory)))
	{
		aggstate->current_set = aggstate->projected_set = currentSet;

		econtext->grouped_cols = aggstate->grouped_cols[currentSet];

		for (aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &peragg[aggno];
			AggStatePerGroup pergroupstate;

			pergroupstate = &pergroup[aggno + (currentSet * (aggstate->numaggs))];

			if (peraggstate->numSortCols > 0)
			{
				if (peraggstate->numInputs == 1)
					process_ordered_aggregate_single(aggstate,
													 peraggstate,
													 pergroupstate);
				else
					process_ordered_aggregate_multi(aggstate,
													peraggstate,
													pergroupstate);
			}

			finalize_aggregate(aggstate, peraggstate, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
		}

		/*
		 * Check the qual (HAVING clause); if the group does not match, ignore
		 * it.
		 */
		if (ExecQual(aggstate->ss.ps.qual, econtext, false))
		{
			/*
			 * Form a projection tuple using the aggregate results
			 * and the representative input tuple.
			 */
			TupleTableSlot *result;
			ExprDoneCond isDone;

			do
			{
				result = ExecProject(aggstate->ss.ps.ps_ProjInfo, &isDone);

				if (isDone != ExprEndResult)
				{
					tuplestore_puttupleslot(aggstate->chain_tuplestore,
											result);
				}
			}
			while (isDone == ExprMultipleResult);
		}
		else
			InstrCountFiltered1(aggstate, 1);

		ReScanExprContext(tmpcontext);
		ReScanExprContext(econtext);
		ReScanExprContext(aggstate->aggcontext[currentSet]);
		MemoryContextDeleteChildren(aggstate->aggcontext[currentSet]->ecxt_per_tuple_memory);
		if (++currentSet >= numGroupingSets)
			break;
	}

	if (TupIsNull(outerslot))
	{
		aggstate->agg_done = true;

		/*
		 * We're out of input, so the calling node has all the data it needs
		 * and (if it's a Sort) is about to sort it. We preemptively request a
		 * rescan of our input plan here, so that Sort nodes containing data
		 * that is no longer needed will free their memory.  The intention here
		 * is to bound the peak memory requirement for the whole chain to
		 * 2*work_mem if REWIND was not requested, or 3*work_mem if REWIND was
		 * requested and we had to supply a Sort node for the original data
		 * source plan.
		 */

		ExecReScan(outerPlanState(aggstate));

		return NULL;
	}

	/*
	 * If this is the first tuple, store it and initialize everything.
	 * Otherwise re-init any aggregates we projected above.
	 */

	if (TupIsNull(firstSlot))
	{
		ExecCopySlot(firstSlot, outerslot);
		initialize_aggregates(aggstate, peragg, pergroup, numGroupingSets);
	}
	else if (currentSet > 0)
	{
		ExecCopySlot(firstSlot, outerslot);
		initialize_aggregates(aggstate, peragg, pergroup, currentSet);
	}

	tmpcontext->ecxt_outertuple = outerslot;

	advance_aggregates(aggstate, pergroup);

	/* Reset per-input-tuple context after each tuple */
	ResetExprContext(tmpcontext);

	return outerslot;
}

/*
 * ExecAgg for hashed case: phase 1, read input and build hash table
 */
static void
agg_fill_hash_table(AggState *aggstate)
{
	PlanState  *outerPlan;
	ExprContext *tmpcontext;
	AggHashEntry entry;
	TupleTableSlot *outerslot;

	/*
	 * get state info from node
	 */
	outerPlan = outerPlanState(aggstate);
	/* tmpcontext is the per-input-tuple expression context */
	tmpcontext = aggstate->tmpcontext;

	/*
	 * Process each outer-plan tuple, and then fetch the next one, until we
	 * exhaust the outer plan.
	 */
	for (;;)
	{
		outerslot = ExecProcNode(outerPlan);
		if (TupIsNull(outerslot))
			break;
		/* set up for advance_aggregates call */
		tmpcontext->ecxt_outertuple = outerslot;

		/* Find or build hashtable entry for this tuple's group */
		entry = lookup_hash_entry(aggstate, outerslot);

		/* Advance the aggregates */
		advance_aggregates(aggstate, entry->pergroup);

		/* Reset per-input-tuple context after each tuple */
		ResetExprContext(tmpcontext);
	}

	aggstate->table_filled = true;
	/* Initialize to walk the hash table */
	ResetTupleHashIterator(aggstate->hashtable, &aggstate->hashiter);
}

/*
 * ExecAgg for hashed case: phase 2, retrieving groups from hash table
 */
static TupleTableSlot *
agg_retrieve_hash_table(AggState *aggstate)
{
	ExprContext *econtext;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	AggHashEntry entry;
	TupleTableSlot *firstSlot;
	int			aggno;

	/*
	 * get state info from node
	 */
	/* econtext is the per-output-tuple expression context */
	econtext = aggstate->ss.ps.ps_ExprContext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;
	peragg = aggstate->peragg;
	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	while (!aggstate->agg_done)
	{
		/*
		 * Find the next entry in the hash table
		 */
		entry = (AggHashEntry) ScanTupleHashTable(&aggstate->hashiter);
		if (entry == NULL)
		{
			/* No more entries in hashtable, so done */
			aggstate->agg_done = TRUE;
			return NULL;
		}

		/*
		 * Clear the per-output-tuple context for each group
		 *
		 * We intentionally don't use ReScanExprContext here; if any aggs have
		 * registered shutdown callbacks, they mustn't be called yet, since we
		 * might not be done with that agg.
		 */
		ResetExprContext(econtext);

		/*
		 * Store the copied first input tuple in the tuple table slot reserved
		 * for it, so that it can be used in ExecProject.
		 */
		ExecStoreMinimalTuple(entry->shared.firstTuple,
							  firstSlot,
							  false);

		pergroup = entry->pergroup;

		/*
		 * Finalize each aggregate calculation, and stash results in the
		 * per-output-tuple context.
		 */
		for (aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &peragg[aggno];
			AggStatePerGroup pergroupstate = &pergroup[aggno];

			Assert(peraggstate->numSortCols == 0);
			finalize_aggregate(aggstate, peraggstate, pergroupstate,
							   &aggvalues[aggno], &aggnulls[aggno]);
		}

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_outertuple = firstSlot;

		/*
		 * Check the qual (HAVING clause); if the group does not match, ignore
		 * it and loop back to try to process another group.
		 */
		if (ExecQual(aggstate->ss.ps.qual, econtext, false))
		{
			/*
			 * Form and return a projection tuple using the aggregate results
			 * and the representative input tuple.
			 */
			TupleTableSlot *result;
			ExprDoneCond isDone;

			result = ExecProject(aggstate->ss.ps.ps_ProjInfo, &isDone);

			if (isDone != ExprEndResult)
			{
				aggstate->ss.ps.ps_TupFromTlist =
					(isDone == ExprMultipleResult);
				return result;
			}
		}
		else
			InstrCountFiltered1(aggstate, 1);
	}

	/* No more groups */
	return NULL;
}

/* -----------------
 * ExecInitAgg
 *
 *	Creates the run-time information for the agg node produced by the
 *	planner and initializes its outer subtree
 * -----------------
 */
AggState *
ExecInitAgg(Agg *node, EState *estate, int eflags)
{
	AggState   *aggstate;
	AggState   *save_chain_head = NULL;
	AggStatePerAgg peragg;
	Plan	   *outerPlan;
	ExprContext *econtext;
	int			numaggs,
				aggno;
	ListCell   *l;
	int			numGroupingSets = 1;
	int			currentsortno = 0;
	int			i = 0;
	int			j = 0;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	aggstate = makeNode(AggState);
	aggstate->ss.ps.plan = (Plan *) node;
	aggstate->ss.ps.state = estate;

	aggstate->aggs = NIL;
	aggstate->numaggs = 0;
	aggstate->numsets = 0;
	aggstate->eqfunctions = NULL;
	aggstate->hashfunctions = NULL;
	aggstate->projected_set = -1;
	aggstate->current_set = 0;
	aggstate->peragg = NULL;
	aggstate->curperagg = NULL;
	aggstate->agg_done = false;
	aggstate->input_done = false;
	aggstate->chain_done = true;
	aggstate->pergroup = NULL;
	aggstate->grp_firstTuple = NULL;
	aggstate->hashtable = NULL;
	aggstate->chain_depth = 0;
	aggstate->chain_rescan = 0;
	aggstate->chain_eflags = eflags;
	aggstate->chain_top = false;
	aggstate->chain_head = NULL;
	aggstate->chain_tuplestore = NULL;

	if (node->groupingSets)
	{
		Assert(node->aggstrategy != AGG_HASHED);

		numGroupingSets = list_length(node->groupingSets);
		aggstate->numsets = numGroupingSets;
		aggstate->gset_lengths = palloc(numGroupingSets * sizeof(int));
		aggstate->grouped_cols = palloc(numGroupingSets * sizeof(Bitmapset *));

		i = 0;
		foreach(l, node->groupingSets)
		{
			int current_length = list_length(lfirst(l));
			Bitmapset *cols = NULL;

			/* planner forces this to be correct */
			for (j = 0; j < current_length; ++j)
				cols = bms_add_member(cols, node->grpColIdx[j]);

			aggstate->grouped_cols[i] = cols;
			aggstate->gset_lengths[i] = current_length;
			++i;
		}
	}

	aggstate->aggcontext = (ExprContext **) palloc0(sizeof(ExprContext *) * numGroupingSets);

	/*
	 * Create expression contexts.  We need three or more, one for
	 * per-input-tuple processing, one for per-output-tuple processing, and one
	 * for each grouping set.  The per-tuple memory context of the
	 * per-grouping-set ExprContexts (aggcontexts) replaces the standalone
	 * memory context formerly used to hold transition values.  We cheat a
	 * little by using ExecAssignExprContext() to build all of them.
	 *
	 * NOTE: the details of what is stored in aggcontexts and what is stored in
	 * the regular per-query memory context are driven by a simple decision: we
	 * want to reset the aggcontext at group boundaries (if not hashing) and in
	 * ExecReScanAgg to recover no-longer-wanted space.
	 */
	ExecAssignExprContext(estate, &aggstate->ss.ps);
	aggstate->tmpcontext = aggstate->ss.ps.ps_ExprContext;

	for (i = 0; i < numGroupingSets; ++i)
	{
		ExecAssignExprContext(estate, &aggstate->ss.ps);
		aggstate->aggcontext[i] = aggstate->ss.ps.ps_ExprContext;
	}

	ExecAssignExprContext(estate, &aggstate->ss.ps);

	/*
	 * tuple table initialization
	 */
	ExecInitScanTupleSlot(estate, &aggstate->ss);
	ExecInitResultTupleSlot(estate, &aggstate->ss.ps);
	aggstate->hashslot = ExecInitExtraTupleSlot(estate);

	/*
	 * initialize child expressions
	 *
	 * Note: ExecInitExpr finds Aggrefs for us, and also checks that no aggs
	 * contain other agg calls in their arguments.  This would make no sense
	 * under SQL semantics anyway (and it's forbidden by the spec). Because
	 * that is true, we don't need to worry about evaluating the aggs in any
	 * particular order.
	 */
	if (node->aggstrategy == AGG_CHAINED)
	{
		AggState   *chain_head = estate->agg_chain_head;
		Agg		   *chain_head_plan;

		Assert(chain_head);

		aggstate->chain_head = chain_head;
		chain_head->chain_depth++;

		chain_head_plan = (Agg *) chain_head->ss.ps.plan;

		/*
		 * If we reached the originally declared depth, we must be the "top"
		 * (furthest from plan root) node in the chain.
		 */
		if (chain_head_plan->chain_depth == chain_head->chain_depth)
			aggstate->chain_top = true;

		/*
		 * Snarf the real targetlist and qual from the chain head node
		 */
		aggstate->ss.ps.targetlist = (List *)
			ExecInitExpr((Expr *) chain_head_plan->plan.targetlist,
						 (PlanState *) aggstate);
		aggstate->ss.ps.qual = (List *)
			ExecInitExpr((Expr *) chain_head_plan->plan.qual,
						 (PlanState *) aggstate);
	}
	else
	{
		aggstate->ss.ps.targetlist = (List *)
			ExecInitExpr((Expr *) node->plan.targetlist,
						 (PlanState *) aggstate);
		aggstate->ss.ps.qual = (List *)
			ExecInitExpr((Expr *) node->plan.qual,
						 (PlanState *) aggstate);
	}

	if (node->chain_depth > 0)
	{
		save_chain_head = estate->agg_chain_head;
		estate->agg_chain_head = aggstate;
		aggstate->chain_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		aggstate->chain_done = false;
	}

	/*
	 * initialize child nodes
	 *
	 * If we are doing a hashed aggregation then the child plan does not need
	 * to handle REWIND efficiently; see ExecReScanAgg.
	 *
	 * If we have more than one associated ChainAggregate node, then we turn
	 * off REWIND and restore it in the chain top, so that the intermediate
	 * Sort nodes will discard their data on rescan.  This lets us put an upper
	 * bound on the memory usage, even when we have a long chain of sorts (at
	 * the cost of having to re-sort on rewind, which is why we don't do it
	 * for only one node where no memory would be saved).
	 */
	if (aggstate->chain_top)
		eflags = aggstate->chain_head->chain_eflags;
	else if (node->aggstrategy == AGG_HASHED || node->chain_depth > 1)
		eflags &= ~EXEC_FLAG_REWIND;
	outerPlan = outerPlan(node);
	outerPlanState(aggstate) = ExecInitNode(outerPlan, estate, eflags);

	if (node->chain_depth > 0)
	{
		estate->agg_chain_head = save_chain_head;
	}

	/*
	 * initialize source tuple type.
	 */
	ExecAssignScanTypeFromOuterPlan(&aggstate->ss);

	/*
	 * Initialize result tuple type and projection info.
	 */
	if (node->aggstrategy == AGG_CHAINED)
	{
		PlanState  *head_ps = &aggstate->chain_head->ss.ps;
		bool		hasoid;

		/*
		 * We must calculate this the same way that the chain head does,
		 * regardless of intermediate nodes, for consistency
		 */
		if (!ExecContextForcesOids(head_ps, &hasoid))
			hasoid = false;

		ExecAssignResultType(&aggstate->ss.ps, ExecGetScanType(&aggstate->ss));
		ExecSetSlotDescriptor(aggstate->hashslot,
							  ExecTypeFromTL(head_ps->plan->targetlist, hasoid));
		aggstate->ss.ps.ps_ProjInfo =
			ExecBuildProjectionInfo(aggstate->ss.ps.targetlist,
									aggstate->ss.ps.ps_ExprContext,
									aggstate->hashslot,
									NULL);

		aggstate->chain_tuplestore = aggstate->chain_head->chain_tuplestore;
		Assert(aggstate->chain_tuplestore);
	}
	else
	{
		ExecAssignResultTypeFromTL(&aggstate->ss.ps);
		ExecAssignProjectionInfo(&aggstate->ss.ps, NULL);
	}

	aggstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * get the count of aggregates in targetlist and quals
	 */
	numaggs = aggstate->numaggs;
	Assert(numaggs == list_length(aggstate->aggs));
	if (numaggs <= 0)
	{
		/*
		 * This is not an error condition: we might be using the Agg node just
		 * to do hash-based grouping.  Even in the regular case,
		 * constant-expression simplification could optimize away all of the
		 * Aggrefs in the targetlist and qual.  So keep going, but force local
		 * copy of numaggs positive so that palloc()s below don't choke.
		 */
		numaggs = 1;
	}

	/*
	 * If we are grouping, precompute fmgr lookup data for inner loop. We need
	 * both equality and hashing functions to do it by hashing, but only
	 * equality if not hashing.
	 */
	if (node->numCols > 0)
	{
		if (node->aggstrategy == AGG_HASHED)
			execTuplesHashPrepare(node->numCols,
								  node->grpOperators,
								  &aggstate->eqfunctions,
								  &aggstate->hashfunctions);
		else
			aggstate->eqfunctions =
				execTuplesMatchPrepare(node->numCols,
									   node->grpOperators);
	}

	/*
	 * Set up aggregate-result storage in the output expr context, and also
	 * allocate my private per-agg working storage
	 */
	econtext = aggstate->ss.ps.ps_ExprContext;
	econtext->ecxt_aggvalues = (Datum *) palloc0(sizeof(Datum) * numaggs);
	econtext->ecxt_aggnulls = (bool *) palloc0(sizeof(bool) * numaggs);

	peragg = (AggStatePerAgg) palloc0(sizeof(AggStatePerAggData) * numaggs);
	aggstate->peragg = peragg;

	if (node->aggstrategy == AGG_HASHED)
	{
		build_hash_table(aggstate);
		aggstate->table_filled = false;
		/* Compute the columns we actually need to hash on */
		aggstate->hash_needed = find_hash_columns(aggstate);
	}
	else
	{
		AggStatePerGroup pergroup;

		pergroup = (AggStatePerGroup) palloc0(sizeof(AggStatePerGroupData) * numaggs * numGroupingSets);

		aggstate->pergroup = pergroup;
	}

	/*
	 * Perform lookups of aggregate function info, and initialize the
	 * unchanging fields of the per-agg data.  We also detect duplicate
	 * aggregates (for example, "SELECT sum(x) ... HAVING sum(x) > 0"). When
	 * duplicates are detected, we only make an AggStatePerAgg struct for the
	 * first one.  The clones are simply pointed at the same result entry by
	 * giving them duplicate aggno values.
	 */
	aggno = -1;
	foreach(l, aggstate->aggs)
	{
		AggrefExprState *aggrefstate = (AggrefExprState *) lfirst(l);
		Aggref	   *aggref = (Aggref *) aggrefstate->xprstate.expr;
		AggStatePerAgg peraggstate;
		Oid			inputTypes[FUNC_MAX_ARGS];
		int			numArguments;
		int			numDirectArgs;
		int			numInputs;
		int			numSortCols;
		int			numDistinctCols;
		List	   *sortlist;
		HeapTuple	aggTuple;
		Form_pg_aggregate aggform;
		Oid			aggtranstype;
		AclResult	aclresult;
		Oid			transfn_oid,
					finalfn_oid;
		Expr	   *transfnexpr,
				   *finalfnexpr;
		Datum		textInitVal;
		int			i;
		ListCell   *lc;

		/* Planner should have assigned aggregate to correct level */
		Assert(aggref->agglevelsup == 0);

		/* Look for a previous duplicate aggregate */
		for (i = 0; i <= aggno; i++)
		{
			if (equal(aggref, peragg[i].aggref) &&
				!contain_volatile_functions((Node *) aggref))
				break;
		}
		if (i <= aggno)
		{
			/* Found a match to an existing entry, so just mark it */
			aggrefstate->aggno = i;
			continue;
		}

		/* Nope, so assign a new PerAgg record */
		peraggstate = &peragg[++aggno];

		/* Mark Aggref state node with assigned index in the result array */
		aggrefstate->aggno = aggno;

		/* Begin filling in the peraggstate data */
		peraggstate->aggrefstate = aggrefstate;
		peraggstate->aggref = aggref;
		peraggstate->sortstate = (Tuplesortstate**) palloc0(sizeof(Tuplesortstate*) * numGroupingSets);

		for (currentsortno = 0; currentsortno < numGroupingSets; currentsortno++)
			peraggstate->sortstate[currentsortno] = NULL;

		/* Fetch the pg_aggregate row */
		aggTuple = SearchSysCache1(AGGFNOID,
								   ObjectIdGetDatum(aggref->aggfnoid));
		if (!HeapTupleIsValid(aggTuple))
			elog(ERROR, "cache lookup failed for aggregate %u",
				 aggref->aggfnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);

		/* Check permission to call aggregate function */
		aclresult = pg_proc_aclcheck(aggref->aggfnoid, GetUserId(),
									 ACL_EXECUTE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, ACL_KIND_PROC,
						   get_func_name(aggref->aggfnoid));
		InvokeFunctionExecuteHook(aggref->aggfnoid);

		peraggstate->transfn_oid = transfn_oid = aggform->aggtransfn;
		peraggstate->finalfn_oid = finalfn_oid = aggform->aggfinalfn;

		/* Check that aggregate owner has permission to call component fns */
		{
			HeapTuple	procTuple;
			Oid			aggOwner;

			procTuple = SearchSysCache1(PROCOID,
										ObjectIdGetDatum(aggref->aggfnoid));
			if (!HeapTupleIsValid(procTuple))
				elog(ERROR, "cache lookup failed for function %u",
					 aggref->aggfnoid);
			aggOwner = ((Form_pg_proc) GETSTRUCT(procTuple))->proowner;
			ReleaseSysCache(procTuple);

			aclresult = pg_proc_aclcheck(transfn_oid, aggOwner,
										 ACL_EXECUTE);
			if (aclresult != ACLCHECK_OK)
				aclcheck_error(aclresult, ACL_KIND_PROC,
							   get_func_name(transfn_oid));
			InvokeFunctionExecuteHook(transfn_oid);
			if (OidIsValid(finalfn_oid))
			{
				aclresult = pg_proc_aclcheck(finalfn_oid, aggOwner,
											 ACL_EXECUTE);
				if (aclresult != ACLCHECK_OK)
					aclcheck_error(aclresult, ACL_KIND_PROC,
								   get_func_name(finalfn_oid));
				InvokeFunctionExecuteHook(finalfn_oid);
			}
		}

		/*
		 * Get actual datatypes of the (nominal) aggregate inputs.  These
		 * could be different from the agg's declared input types, when the
		 * agg accepts ANY or a polymorphic type.
		 */
		numArguments = get_aggregate_argtypes(aggref, inputTypes);
		peraggstate->numArguments = numArguments;

		/* Count the "direct" arguments, if any */
		numDirectArgs = list_length(aggref->aggdirectargs);

		/* Count the number of aggregated input columns */
		numInputs = list_length(aggref->args);
		peraggstate->numInputs = numInputs;

		/* Detect how many arguments to pass to the transfn */
		if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
			peraggstate->numTransInputs = numInputs;
		else
			peraggstate->numTransInputs = numArguments;

		/* Detect how many arguments to pass to the finalfn */
		if (aggform->aggfinalextra)
			peraggstate->numFinalArgs = numArguments + 1;
		else
			peraggstate->numFinalArgs = numDirectArgs + 1;

		/* resolve actual type of transition state, if polymorphic */
		aggtranstype = resolve_aggregate_transtype(aggref->aggfnoid,
												   aggform->aggtranstype,
												   inputTypes,
												   numArguments);

		/* build expression trees using actual argument & result types */
		build_aggregate_fnexprs(inputTypes,
								numArguments,
								numDirectArgs,
								peraggstate->numFinalArgs,
								aggref->aggvariadic,
								aggtranstype,
								aggref->aggtype,
								aggref->inputcollid,
								transfn_oid,
								InvalidOid,		/* invtrans is not needed here */
								finalfn_oid,
								&transfnexpr,
								NULL,
								&finalfnexpr);

		/* set up infrastructure for calling the transfn and finalfn */
		fmgr_info(transfn_oid, &peraggstate->transfn);
		fmgr_info_set_expr((Node *) transfnexpr, &peraggstate->transfn);

		if (OidIsValid(finalfn_oid))
		{
			fmgr_info(finalfn_oid, &peraggstate->finalfn);
			fmgr_info_set_expr((Node *) finalfnexpr, &peraggstate->finalfn);
		}

		peraggstate->aggCollation = aggref->inputcollid;

		InitFunctionCallInfoData(peraggstate->transfn_fcinfo,
								 &peraggstate->transfn,
								 peraggstate->numTransInputs + 1,
								 peraggstate->aggCollation,
								 (void *) aggstate, NULL);

		/* get info about relevant datatypes */
		get_typlenbyval(aggref->aggtype,
						&peraggstate->resulttypeLen,
						&peraggstate->resulttypeByVal);
		get_typlenbyval(aggtranstype,
						&peraggstate->transtypeLen,
						&peraggstate->transtypeByVal);

		/*
		 * initval is potentially null, so don't try to access it as a struct
		 * field. Must do it the hard way with SysCacheGetAttr.
		 */
		textInitVal = SysCacheGetAttr(AGGFNOID, aggTuple,
									  Anum_pg_aggregate_agginitval,
									  &peraggstate->initValueIsNull);

		if (peraggstate->initValueIsNull)
			peraggstate->initValue = (Datum) 0;
		else
			peraggstate->initValue = GetAggInitVal(textInitVal,
												   aggtranstype);

		/*
		 * If the transfn is strict and the initval is NULL, make sure input
		 * type and transtype are the same (or at least binary-compatible), so
		 * that it's OK to use the first aggregated input value as the initial
		 * transValue.  This should have been checked at agg definition time,
		 * but we must check again in case the transfn's strictness property
		 * has been changed.
		 */
		if (peraggstate->transfn.fn_strict && peraggstate->initValueIsNull)
		{
			if (numArguments <= numDirectArgs ||
				!IsBinaryCoercible(inputTypes[numDirectArgs], aggtranstype))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
						 errmsg("aggregate %u needs to have compatible input type and transition type",
								aggref->aggfnoid)));
		}

		/*
		 * Get a tupledesc corresponding to the aggregated inputs (including
		 * sort expressions) of the agg.
		 */
		peraggstate->evaldesc = ExecTypeFromTL(aggref->args, false);

		/* Create slot we're going to do argument evaluation in */
		peraggstate->evalslot = ExecInitExtraTupleSlot(estate);
		ExecSetSlotDescriptor(peraggstate->evalslot, peraggstate->evaldesc);

		/* Set up projection info for evaluation */
		peraggstate->evalproj = ExecBuildProjectionInfo(aggrefstate->args,
														aggstate->tmpcontext,
														peraggstate->evalslot,
														NULL);

		/*
		 * If we're doing either DISTINCT or ORDER BY for a plain agg, then we
		 * have a list of SortGroupClause nodes; fish out the data in them and
		 * stick them into arrays.  We ignore ORDER BY for an ordered-set agg,
		 * however; the agg's transfn and finalfn are responsible for that.
		 *
		 * Note that by construction, if there is a DISTINCT clause then the
		 * ORDER BY clause is a prefix of it (see transformDistinctClause).
		 */
		if (AGGKIND_IS_ORDERED_SET(aggref->aggkind))
		{
			sortlist = NIL;
			numSortCols = numDistinctCols = 0;
		}
		else if (aggref->aggdistinct)
		{
			sortlist = aggref->aggdistinct;
			numSortCols = numDistinctCols = list_length(sortlist);
			Assert(numSortCols >= list_length(aggref->aggorder));
		}
		else
		{
			sortlist = aggref->aggorder;
			numSortCols = list_length(sortlist);
			numDistinctCols = 0;
		}

		peraggstate->numSortCols = numSortCols;
		peraggstate->numDistinctCols = numDistinctCols;

		if (numSortCols > 0)
		{
			/*
			 * We don't implement DISTINCT or ORDER BY aggs in the HASHED case
			 * (yet)
			 */
			Assert(node->aggstrategy != AGG_HASHED);

			/* If we have only one input, we need its len/byval info. */
			if (numInputs == 1)
			{
				get_typlenbyval(inputTypes[numDirectArgs],
								&peraggstate->inputtypeLen,
								&peraggstate->inputtypeByVal);
			}
			else if (numDistinctCols > 0)
			{
				/* we will need an extra slot to store prior values */
				peraggstate->uniqslot = ExecInitExtraTupleSlot(estate);
				ExecSetSlotDescriptor(peraggstate->uniqslot,
									  peraggstate->evaldesc);
			}

			/* Extract the sort information for use later */
			peraggstate->sortColIdx =
				(AttrNumber *) palloc(numSortCols * sizeof(AttrNumber));
			peraggstate->sortOperators =
				(Oid *) palloc(numSortCols * sizeof(Oid));
			peraggstate->sortCollations =
				(Oid *) palloc(numSortCols * sizeof(Oid));
			peraggstate->sortNullsFirst =
				(bool *) palloc(numSortCols * sizeof(bool));

			i = 0;
			foreach(lc, sortlist)
			{
				SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
				TargetEntry *tle = get_sortgroupclause_tle(sortcl,
														   aggref->args);

				/* the parser should have made sure of this */
				Assert(OidIsValid(sortcl->sortop));

				peraggstate->sortColIdx[i] = tle->resno;
				peraggstate->sortOperators[i] = sortcl->sortop;
				peraggstate->sortCollations[i] = exprCollation((Node *) tle->expr);
				peraggstate->sortNullsFirst[i] = sortcl->nulls_first;
				i++;
			}
			Assert(i == numSortCols);
		}

		if (aggref->aggdistinct)
		{
			Assert(numArguments > 0);

			/*
			 * We need the equal function for each DISTINCT comparison we will
			 * make.
			 */
			peraggstate->equalfns =
				(FmgrInfo *) palloc(numDistinctCols * sizeof(FmgrInfo));

			i = 0;
			foreach(lc, aggref->aggdistinct)
			{
				SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);

				fmgr_info(get_opcode(sortcl->eqop), &peraggstate->equalfns[i]);
				i++;
			}
			Assert(i == numDistinctCols);
		}

		ReleaseSysCache(aggTuple);
	}

	/* Update numaggs to match number of unique aggregates found */
	aggstate->numaggs = aggno + 1;

	return aggstate;
}

static Datum
GetAggInitVal(Datum textInitVal, Oid transtype)
{
	Oid			typinput,
				typioparam;
	char	   *strInitVal;
	Datum		initVal;

	getTypeInputInfo(transtype, &typinput, &typioparam);
	strInitVal = TextDatumGetCString(textInitVal);
	initVal = OidInputFunctionCall(typinput, strInitVal,
								   typioparam, -1);
	pfree(strInitVal);
	return initVal;
}

void
ExecEndAgg(AggState *node)
{
	PlanState  *outerPlan;
	int			aggno;
	int			numGroupingSets = Max(node->numsets, 1);
	int			i = 0;

	/* Make sure we have closed any open tuplesorts */
	for (aggno = 0; aggno < node->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &node->peragg[aggno];

		for (i = 0; i < numGroupingSets; i++)
		{
			if (peraggstate->sortstate[i])
				tuplesort_end(peraggstate->sortstate[i]);
		}
	}

	/* And ensure any agg shutdown callbacks have been called */
	for (i = 0; i < numGroupingSets; ++i)
		ReScanExprContext(node->aggcontext[i]);

	if (node->chain_tuplestore && node->chain_depth > 0)
		tuplestore_end(node->chain_tuplestore);

	/*
	 * We don't actually free any ExprContexts here (see comment in
	 * ExecFreeExprContext), just unlinking the output one from the plan node
	 * suffices.
	 */
	ExecFreeExprContext(&node->ss.ps);

	/* clean up tuple table */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	outerPlan = outerPlanState(node);
	ExecEndNode(outerPlan);
}

void
ExecReScanAgg(AggState *node)
{
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	Agg		   *aggnode = (Agg *) node->ss.ps.plan;
	int			aggno;
	int         numGroupingSets = Max(node->numsets, 1);
	int         setno;
	int         i;

	node->agg_done = false;

	node->ss.ps.ps_TupFromTlist = false;

	if (aggnode->aggstrategy == AGG_HASHED)
	{
		/*
		 * In the hashed case, if we haven't yet built the hash table then we
		 * can just return; nothing done yet, so nothing to undo. If subnode's
		 * chgParam is not NULL then it will be re-scanned by ExecProcNode,
		 * else no reason to re-scan it at all.
		 */
		if (!node->table_filled)
			return;

		/*
		 * If we do have the hash table and the subplan does not have any
		 * parameter changes, then we can just rescan the existing hash table;
		 * no need to build it again.
		 */
		if (node->ss.ps.lefttree->chgParam == NULL)
		{
			ResetTupleHashIterator(node->hashtable, &node->hashiter);
			return;
		}
	}

	/* Make sure we have closed any open tuplesorts */
	for (aggno = 0; aggno < node->numaggs; aggno++)
	{
		for (setno = 0; setno < numGroupingSets; setno++)
		{
			AggStatePerAgg peraggstate = &node->peragg[aggno];

			if (peraggstate->sortstate[setno])
			{
				tuplesort_end(peraggstate->sortstate[setno]);
				peraggstate->sortstate[setno] = NULL;
			}
		}
	}

	/*
	 * We don't need to ReScanExprContext the output tuple context here;
	 * ExecReScan already did it. But we do need to reset our per-grouping-set
	 * contexts, which may have transvalues stored in them.
	 *
	 * Note that with AGG_HASHED, the hash table is allocated in a sub-context
	 * of the aggcontext. We're going to rebuild the hash table from scratch,
	 * so we need to use MemoryContextDeleteChildren() to avoid leaking the old
	 * hash table's memory context header. (ReScanExprContext does the actual
	 * reset, but it doesn't delete child contexts.)
	 */

	for (i = 0; i < numGroupingSets; ++i)
	{
		ReScanExprContext(node->aggcontext[i]);
		MemoryContextDeleteChildren(node->aggcontext[i]->ecxt_per_tuple_memory);
	}

	/* Release first tuple of group, if we have made a copy */
	if (node->grp_firstTuple != NULL)
	{
		heap_freetuple(node->grp_firstTuple);
		node->grp_firstTuple = NULL;
	}
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/* Forget current agg values */
	MemSet(econtext->ecxt_aggvalues, 0, sizeof(Datum) * node->numaggs);
	MemSet(econtext->ecxt_aggnulls, 0, sizeof(bool) * node->numaggs);

	if (aggnode->aggstrategy == AGG_HASHED)
	{
		/* Rebuild an empty hash table */
		build_hash_table(node);
		node->table_filled = false;
	}
	else
	{
		/*
		 * Reset the per-group state (in particular, mark transvalues null)
		 */
		MemSet(node->pergroup, 0,
			   sizeof(AggStatePerGroupData) * node->numaggs * numGroupingSets);

		node->input_done = false;
	}

	/*
	 * If we're in a chain, let the chain head know whether we
	 * rescanned. (This is nonsense if it happens as a result of chgParam,
	 * but the chain head only cares about this when rescanning explicitly
	 * when chgParam is empty.)
	 */

	if (aggnode->aggstrategy == AGG_CHAINED)
		node->chain_head->chain_rescan++;

	/*
	 * If we're a chain head, we reset the tuplestore if parameters changed,
	 * and let subplans repopulate it.
	 *
	 * If we're a chain head and the subplan parameters did NOT change, then
	 * whether we need to reset the tuplestore depends on whether anything
	 * (specifically the Sort nodes) protects the child ChainAggs from rescan.
	 * Since this is hard to know in advance, we have the ChainAggs signal us
	 * as to whether the reset is needed.  Since we're preempting the rescan
	 * in some cases, we only check whether any ChainAgg node was reached in
	 * the rescan; the others may have already been reset.
	 */
	if (aggnode->chain_depth > 0)
	{
		if (node->ss.ps.lefttree->chgParam)
			tuplestore_clear(node->chain_tuplestore);
		else
		{
			node->chain_rescan = 0;

			ExecReScan(node->ss.ps.lefttree);

			if (node->chain_rescan > 0)
				tuplestore_clear(node->chain_tuplestore);
			else
				tuplestore_rescan(node->chain_tuplestore);
		}
		node->chain_done = false;
	}
	else if (node->ss.ps.lefttree->chgParam == NULL)
	{



		ExecReScan(node->ss.ps.lefttree);
	}
}


/***********************************************************************
 * API exposed to aggregate functions
 ***********************************************************************/


/*
 * AggCheckCallContext - test if a SQL function is being called as an aggregate
 *
 * The transition and/or final functions of an aggregate may want to verify
 * that they are being called as aggregates, rather than as plain SQL
 * functions.  They should use this function to do so.  The return value
 * is nonzero if being called as an aggregate, or zero if not.  (Specific
 * nonzero values are AGG_CONTEXT_AGGREGATE or AGG_CONTEXT_WINDOW, but more
 * values could conceivably appear in future.)
 *
 * If aggcontext isn't NULL, the function also stores at *aggcontext the
 * identity of the memory context that aggregate transition values are being
 * stored in.  Note that the same aggregate call site (flinfo) may be called
 * interleaved on different transition values in different contexts, so it's
 * not kosher to cache aggcontext under fn_extra.  It is, however, kosher to
 * cache it in the transvalue itself (for internal-type transvalues).
 */
int
AggCheckCallContext(FunctionCallInfo fcinfo, MemoryContext *aggcontext)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		if (aggcontext)
		{
			AggState    *aggstate = ((AggState *) fcinfo->context);
			ExprContext *cxt  = aggstate->aggcontext[aggstate->current_set];
			*aggcontext = cxt->ecxt_per_tuple_memory;
		}
		return AGG_CONTEXT_AGGREGATE;
	}
	if (fcinfo->context && IsA(fcinfo->context, WindowAggState))
	{
		if (aggcontext)
			*aggcontext = ((WindowAggState *) fcinfo->context)->curaggcontext;
		return AGG_CONTEXT_WINDOW;
	}

	/* this is just to prevent "uninitialized variable" warnings */
	if (aggcontext)
		*aggcontext = NULL;
	return 0;
}

/*
 * AggGetAggref - allow an aggregate support function to get its Aggref
 *
 * If the function is being called as an aggregate support function,
 * return the Aggref node for the aggregate call.  Otherwise, return NULL.
 *
 * Note that if an aggregate is being used as a window function, this will
 * return NULL.  We could provide a similar function to return the relevant
 * WindowFunc node in such cases, but it's not needed yet.
 */
Aggref *
AggGetAggref(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggStatePerAgg curperagg = ((AggState *) fcinfo->context)->curperagg;

		if (curperagg)
			return curperagg->aggref;
	}
	return NULL;
}

/*
 * AggGetTempMemoryContext - fetch short-term memory context for aggregates
 *
 * This is useful in agg final functions; the context returned is one that
 * the final function can safely reset as desired.  This isn't useful for
 * transition functions, since the context returned MAY (we don't promise)
 * be the same as the context those are called in.
 *
 * As above, this is currently not useful for aggs called as window functions.
 */
MemoryContext
AggGetTempMemoryContext(FunctionCallInfo fcinfo)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;

		return aggstate->tmpcontext->ecxt_per_tuple_memory;
	}
	return NULL;
}

/*
 * AggRegisterCallback - register a cleanup callback for an aggregate
 *
 * This is useful for aggs to register shutdown callbacks, which will ensure
 * that non-memory resources are freed.  The callback will occur just before
 * the associated aggcontext (as returned by AggCheckCallContext) is reset,
 * either between groups or as a result of rescanning the query.  The callback
 * will NOT be called on error paths.  The typical use-case is for freeing of
 * tuplestores or tuplesorts maintained in aggcontext, or pins held by slots
 * created by the agg functions.  (The callback will not be called until after
 * the result of the finalfn is no longer needed, so it's safe for the finalfn
 * to return data that will be freed by the callback.)
 *
 * As above, this is currently not useful for aggs called as window functions.
 */
void
AggRegisterCallback(FunctionCallInfo fcinfo,
					ExprContextCallbackFunction func,
					Datum arg)
{
	if (fcinfo->context && IsA(fcinfo->context, AggState))
	{
		AggState   *aggstate = (AggState *) fcinfo->context;
		ExprContext *cxt  = aggstate->aggcontext[aggstate->current_set];

		RegisterExprContextCallback(cxt, func, arg);

		return;
	}
	elog(ERROR, "aggregate function cannot register a callback in this context");
}


/*
 * aggregate_dummy - dummy execution routine for aggregate functions
 *
 * This function is listed as the implementation (prosrc field) of pg_proc
 * entries for aggregate functions.  Its only purpose is to throw an error
 * if someone mistakenly executes such a function in the normal way.
 *
 * Perhaps someday we could assign real meaning to the prosrc field of
 * an aggregate?
 */
Datum
aggregate_dummy(PG_FUNCTION_ARGS)
{
	elog(ERROR, "aggregate function %u called as normal function",
		 fcinfo->flinfo->fn_oid);
	return (Datum) 0;			/* keep compiler quiet */
}
