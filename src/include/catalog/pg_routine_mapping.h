/*-------------------------------------------------------------------------
 *
 * pg_routine_mapping.h
 *	  definition of the "routine mapping" system catalog (pg_routine_mapping)
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_routine_mapping.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ROUTINE_MAPPING_H
#define PG_ROUTINE_MAPPING_H

#include "catalog/genbki.h"
#include "catalog/pg_routine_mapping_d.h"

/* ----------------
 *		pg_routine_mapping definition.  cpp turns this into
 *		typedef struct FormData_pg_routine_mapping
 * ----------------
 */
CATALOG(pg_routine_mapping,6020,RoutineMappingRelationId)
{
	NameData	rmname;
	Oid			rmproc;
	Oid			rmserver;
#ifdef CATALOG_VARLEN
	text		rmoptions[1];
#endif
} FormData_pg_routine_mapping;

/* ----------------
 *		Form_pg_routine_mapping corresponds to a pointer to a tuple with
 *		the format of pg_routine_mapping relation.
 * ----------------
 */
typedef FormData_pg_routine_mapping *Form_pg_routine_mapping;

#endif			/* PG_ROUTINE_MAPPING_H */
