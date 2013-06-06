#include "postgres.h"
#include "fmgr.h"
#include <string.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum percentile_disc(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(percentile_disc);

Datum
percentile_disc_final(PG_FUNCTION_ARGS)
{
	text *test_text;
	elog(WARNING,"test percentile_disc");

	test_text = PG_GETARG_TEXT_P(0);
	PG_RETURN_TEXT_P(test_text);
}
