/*-------------------------------------------------------------------------
 *
 * superuser.c
 *	  The superuser() function.  Determines if user has superuser privilege.
 *
 * All code should use either of these two functions to find out whether a given
 * user is a superuser so that the escape hatch built in for the single-user
 * case works.
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION src/backend/utils/misc/superuser.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_authid.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/inval.h"
#include "utils/syscache.h"


/*
 * The Postgres user running this command has Postgres superuser privileges
 */
bool
superuser(void)
{
	return superuser_arg(GetUserId());
}


/*
 * The specified role has Postgres superuser privileges
 */
bool
superuser_arg(Oid roleid)
{
	bool		result = is_member_of_role_nosuper(roleid, DEFAULT_ROLE_SUPERUSER);

	return result;
}
