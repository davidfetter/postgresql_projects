/*-------------------------------------------------------------------------
 *
 * describe.h
 *	  prototypes for commands/describe.c
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/describe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DESCRIBE_H
#define DESCRIBE_H

#include "parser/parse_node.h"
#include "tcop/dest.h"

extern void GetPGObject(ObjectType objtype, List *object_name, DestReceiver *dest);
extern TupleDesc GetPGObjectResultDesc(ObjectType objtype);

#endif								 /* DESCRIBE_H */
