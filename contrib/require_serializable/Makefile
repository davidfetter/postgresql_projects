# contrib/require_serializable/Makefile

MODULE_big = require_serializable
OBJS = src/require_serializable.o

PGFILEDESC = 'require_serializable - require SERIALIZABLE isolation'

REGRESS = require_serializable

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
