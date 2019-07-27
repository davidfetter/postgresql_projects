/*--------------------------------------------------------------------
 * describe.h
 *
 * External declarations pertaining to backend/utils/misc/describe.c
 *
 * Copyright(c) 2019, PostgreSQL Global Development Group
 * Written by David Fetter <david@fetter.org>
 *
 * src/include/utils/describe.h
 * -------------------------------------------------------------------
 */
#ifndef PG_DESCRIBE_H
#define PG_DESCRIBE_H

#include "nodes/parsenodes.h"
#include "tcop/desc.h"
#include "utils/array.h"

/* upper limit for DESCRIBE */
#if SIZEOF_SIZE_T > 4 && SIZEOF_LONG > 4
#define MAX_KILOBYTES	INT_MAX
#else
#define MAX_KILOBYTES	(INT_MAX / 1024)
#endif

extern void GetDescribe(const char *type, const char *name, DestReceiver *dest);
extern TupleDesc GetDescribeResultDesc(const char *type, const char *name);

#endif
