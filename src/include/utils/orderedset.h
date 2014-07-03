/*-------------------------------------------------------------------------
 *
 * orderedset.h
 *	  Generic support for ordered-set aggregates
 *
 * Ordered-set aggregates are free to supply their own transition functions,
 * but it is expected that the same transition function will serve for almost
 * all imaginable cases including the built-in ones.  We therefore expose here
 * the structures used by the builtin ordered_set_transition and
 * ordered_set_transition_multi functions.
 *
 * User-defined ordered-set aggs can therefore use the stock transition
 * function and supply their own final function to interpret the sorted data.
 *
 * The state for an ordered-set aggregate is divided into a per-group struct
 * (which is the internal-type transition state datum returned to nodeAgg.c)
 * and a per-query struct, which contains data and sub-objects that we can
 * create just once per query because they will not change across groups.  The
 * per-query struct and subsidiary data live in the executor's per-query memory
 * context, and go away implicitly at ExecutorEnd().
 */
#ifndef ORDEREDSET_H
#define ORDEREDSET_H

#include "nodes/primnodes.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "utils/tuplesort.h"
#include "fmgr.h"

typedef struct OSAPerQueryState
{
	/* Aggref for this aggregate: */
	Aggref	   *aggref;
	/* Memory context containing this struct and other per-query data: */
	MemoryContext qcontext;

	/* These fields are used only when accumulating tuples: */

	/* Tuple descriptor for tuples inserted into sortstate: */
	TupleDesc	tupdesc;
	/* Tuple slot we can use for inserting/extracting tuples: */
	TupleTableSlot *tupslot;
	/* Per-sort-column sorting information */
	int			numSortCols;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *eqOperators;
	Oid		   *sortCollations;
	bool	   *sortNullsFirsts;
	/* Equality operator call info, created only if needed: */
	FmgrInfo   *equalfns;

	/* These fields are used only when accumulating datums: */

	/* Info about datatype of datums being sorted: */
	Oid			sortColType;
	int16		typLen;
	bool		typByVal;
	char		typAlign;
	/* Info about sort ordering: */
	Oid			sortOperator;
	Oid			eqOperator;
	Oid			sortCollation;
	bool		sortNullsFirst;
	/* Equality operator call info, created only if needed: */
	FmgrInfo	equalfn;
} OSAPerQueryState;

typedef struct OSAPerGroupState
{
	/* Link to the per-query state for this aggregate: */
	OSAPerQueryState *qstate;
	/* MemoryContext for per-group data */
	MemoryContext gcontext;
	/* Sort object we're accumulating data in: */
	Tuplesortstate *sortstate;
	/* Number of normal rows inserted into sortstate: */
	int64		number_of_rows;
} OSAPerGroupState;

#endif   /* ORDEREDSET_H */
