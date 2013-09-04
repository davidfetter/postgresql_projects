/*-------------------------------------------------------------------------
 *
 * parse_agg.h
 *	  handle aggregates and window functions in parser
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_agg.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_AGG_H
#define PARSE_AGG_H

#include "parser/parse_node.h"

extern void transformAggregateCall(ParseState *pstate, Aggref *agg,
					   List *args, List *aggorder,
					   bool agg_distinct, bool agg_within_group);
extern void transformWindowFuncCall(ParseState *pstate, WindowFunc *wfunc,
						WindowDef *windef);

extern void parseCheckAggregates(ParseState *pstate, Query *qry);

extern void build_aggregate_fnexprs(Oid *agg_input_types,
						int agg_num_inputs,
						bool agg_variadic,
						Oid agg_state_type,
						Oid agg_result_type,
						Oid agg_input_collation,
						Oid transfn_oid,
						Oid finalfn_oid,
						Expr **transfnexpr,
						Expr **finalfnexpr);

void
build_orderedset_fnexprs(Oid *agg_input_types,
					int agg_num_inputs,
					Oid agg_result_type,
					Oid agg_input_collation,
					Oid *agg_input_collation_array,
					Oid finalfn_oid,
					Expr **finalfnexpr);

int get_aggregate_argtypes(Aggref *aggref, 
				Oid *inputTypes, 
				Oid *inputCollations);

#endif   /* PARSE_AGG_H */
