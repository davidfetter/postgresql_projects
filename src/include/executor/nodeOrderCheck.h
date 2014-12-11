/*-------------------------------------------------------------------------
 *
 * nodeOrderCheck.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeOrderCheck.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEORDERCHECK_H
#define NODEORDERCHECK_H

#include "nodes/execnodes.h"

extern OrderCheckState *ExecInitOrderCheck(OrderCheck *node, EState *estate, int eflags);
extern TupleTableSlot *ExecOrderCheck(OrderCheckState *node);
extern void ExecEndOrderCheck(OrderCheckState *node);
extern void ExecReScanOrderCheck(OrderCheckState *node);


#endif   /* NODEORDERCHECK_H */
