EXTENSION   = plexor
EXT_VERSION = 2.1

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
              src/execute.c \
              src/query.c
OBJS        = src/scanner.o src/parser.tab.o $(SRCS:.c=.o)
EXTRA_CLEAN = src/scanner.[ch] src/parser.tab.[ch]

PQINCSERVER = $(shell $(PG_CONFIG) --includedir-server)
PQINC = $(shell $(PG_CONFIG) --includedir)
PQLIB = $(shell $(PG_CONFIG) --libdir)

DATA_built  = $(EXT_SQL)

SHLIB_LINK = -L$(PQLIB) -lpq

#PG_CPPFLAGS = -std=c89

PG_CONFIG   = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)


include $(PGXS)

# PGXS may define them as empty
FLEX := $(if $(FLEX),$(FLEX),flex)
BISON := $(if $(BISON),$(BISON),bison)

# parser rules
src/scanner.o: src/parser.tab.h
src/parser.tab.h: src/parser.tab.c
src/parser.tab.c: src/parser.y
	@mkdir -p src
	$(BISON) -t -b src/parser -d $<

src/parser.o: src/scanner.h
src/scanner.h: src/scanner.c
src/scanner.c: src/scanner.l
	@mkdir -p src
	$(FLEX) -o$@ --header-file=$(@:.c=.h) $<

sql/plexor.sql: $(PLEXOR_SQL)
	cat $^ > $@

sql/$(EXTENSION)--$(EXT_VERSION).sql: $(PLEXOR_SQL)
	cat $^ > $@

$(OBJS): $(INCLUDES)


