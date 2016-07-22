# contrib/require_where/Makefile

MODULE_big = require_where
OBJS = require_where.o

PGFILEDESC = 'require_where - require simple DELETEs and UPDATEs to have a WHERE clause'

REGRESS = require_where

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS = $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/require_where
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_builddir)/contrib/contrib-global.mk
endif
