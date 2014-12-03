/*-------------------------------------------------------------------------
 *
 * nodeOrderCheck.c
 *	  Routines to handle order checking of results of queries where appropriate
 *
 * OrderCheck essentially returns unmodified tuples as it gets from it's
 * child node. It tracks the previous tuple seen and matches a given set
 * of attributes between previous tuple seen and current tuple, given an
 * order and a set of sort info. OrderCheck essentially checks if the order
 * of returned tuples matches a given order.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeOrderCheck.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecOrderCheck		- generate a unique'd temporary relation
 *		ExecInitOrderCheck	- initialize node and subnodes
 *		ExecEndOrderCheck	- shutdown node and subnodes
 *
 * NOTES
 *		Assumes tuples returned from subplan arrive in
 *		sorted order.
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodeOrderCheck.h"
#include "utils/memutils.h"


/* ----------------------------------------------------------------
 *		ExecOrderCheck
 * ----------------------------------------------------------------
 */
TupleTableSlot *				/* return: a tuple or NULL */
ExecOrderCheck(OrderCheckState *node)
{
	OrderCheck	   *plannode = (OrderCheck *) node->ps.plan;
	TupleTableSlot *resultTupleSlot;
	TupleTableSlot *slot;
	PlanState  *outerPlan;
	int numcols = plannode->numCols;
	int i = 0;
	bool previousAttValid = false;

	/*
	 * get information from the node
	 */
	outerPlan = outerPlanState(node);
	resultTupleSlot = node->ps.ps_ResultTupleSlot;

	/*
	 * fetch a tuple from the outer subplan
	 */
	slot = ExecProcNode(outerPlan);
	if (TupIsNull(slot))
	{
		/* end of subplan, so we're done */
		ExecClearTuple(resultTupleSlot);
		return NULL;
	}

	/*
	 * Else test if the new tuple and the previously returned tuple follow
	 * the given order.
	 * If so then we loop back and fetch another new tuple from the
	 * subplan.
	 */

	if (!(TupIsNull(resultTupleSlot)))
	{
		for (i = 0;i < numcols;i++)
		{
			SortSupport sortKey = node->oc_sortkeys + i;
			AttrNumber current_attrnum = plannode->sortColIdx[i];
			Datum datum1;
			Datum datum2;
			int compare;
			bool isNull1;
			bool isNull2;

			datum1 = slot_getattr(resultTupleSlot, current_attrnum, &isNull1);
			datum2 = slot_getattr(slot, current_attrnum, &isNull2);

			compare = ApplySortComparator(datum1, isNull1,
										  datum2, isNull2,
										  sortKey);

			if (compare == 1)
			{
				if (previousAttValid == true)
					continue;

				elog(ERROR, "Order not same as specified");
			}

			/* For comparing two tuples with multiple sort keys, consider
			 * following cases:
			 * If two tuples are in order with respect to first sort key,
			 * we are assured that we are in order for the current tuples.
			 * If two tuples have equal first sort keys, we need to ensure
			 * that further sort keys are in order.
			 */
			if (compare == -1)
				previousAttValid = true;
			else
				previousAttValid = false;
		}
	}

	/*
	 * We have a new tuple different from the previous saved tuple (if any).
	 * Save it and return it.  We must copy it because the source subplan
	 * won't guarantee that this source tuple is still accessible after
	 * fetching the next source tuple.
	 */
	return ExecCopySlot(resultTupleSlot, slot);
}

/* ----------------------------------------------------------------
 *		ExecInitOrderCheck
 *
 *		This initializes the unique node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
OrderCheckState *
ExecInitOrderCheck(OrderCheck *node, EState *estate, int eflags)
{
	OrderCheckState *ordercheckstate;
	int i = 0;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	ordercheckstate = makeNode(OrderCheckState);
	ordercheckstate->ps.plan = (Plan *) node;
	ordercheckstate->ps.state = estate;

	/*
	 * Tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &ordercheckstate->ps);

	/*
	 * then initialize outer plan
	 */
	outerPlanState(ordercheckstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * unique nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	ExecAssignResultTypeFromTL(&ordercheckstate->ps);
	ordercheckstate->ps.ps_ProjInfo = NULL;

	ordercheckstate->oc_sortkeys = palloc0(sizeof(SortSupportData) * (node->numCols));

	for (i = 0;i < node->numCols;i++)
	{
		SortSupport current_sortkey = ordercheckstate->oc_sortkeys + i;
		current_sortkey->ssup_cxt = CurrentMemoryContext;
		current_sortkey->ssup_collation = node->collations[i];
		current_sortkey->ssup_nulls_first = node->nullsFirst[i];
		current_sortkey->ssup_attno = node->sortColIdx[i];

		PrepareSortSupportFromOrderingOp(node->sortOperators[i], current_sortkey);
	}



	return ordercheckstate;
}

/* ----------------------------------------------------------------
 *		ExecEndOrderCheck
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndOrderCheck(OrderCheckState *node)
{
	/* clean up tuple table */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	ExecEndNode(outerPlanState(node));
}
