EXTENSION   = plexor
EXT_VERSION = 2.2

MODULE_big  = $(EXTENSION)

# sql
PLEXOR_SQL = sql/plexor_lang.sql
EXT_SQL     = sql/$(EXTENSION)--$(EXT_VERSION).sql

INCLUDES    = src/plexor.h

SRCS        = src/plexor.c \
              src/fdw_validator.c \
              src/cluster.c \
              src/connection.c \
              src/type.c \
              src/function.c \
              src/result.c \
              src/transaction.c \
              src/parser.c \
              src/execute.c \
              src/query.c
OBJS        = $(SRCS:.c=.o)
EXTRA_CLEAN =

PQINCSERVER = $(shell $(PG_CONFIG) --includedir-server)
PQINC = $(shell $(PG_CONFIG) --includedir)
PQLIB = $(shell $(PG_CONFIG) --libdir)

DATA_built  = $(EXT_SQL)

SHLIB_LINK = -L$(PQLIB) -lpq

#PG_CPPFLAGS = -std=c89
PG_CPPFLAGS += -I$(PQINC)

PG_CONFIG   = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

include $(PGXS)

sql/plexor.sql: $(PLEXOR_SQL)
	cat $^ > $@

sql/$(EXTENSION)--$(EXT_VERSION).sql: $(PLEXOR_SQL)
	cat $^ > $@

$(OBJS): $(INCLUDES)


