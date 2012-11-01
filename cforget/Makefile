TARGET ?= /bitly/local
BLDDIR := build

BINARIES := forgettable

SRCS_forgettable        := forgettable.c        set.c helper.c
SRCS_forgettable_worker := forgettable_worker.c set.c helper.c

CFLAGS  := -I/usr/local/Cellar/hiredis/0.11.0/include -I/usr/local/include -Wall -O2 -g -DDEBUG
LDFLAGS := -L/usr/local/Cellar/hiredis/0.11.0/lib -L/usr/local/lib -levent -lsimplehttp -ljson -lbitly 

all: $(BINARIES)

OBJS_forgettable        := $(patsubst %.c, $(BLDDIR)/%.o, $(SRCS_forgettable))

DEPS := $(patsubst %.o, %.o.deps, $(OBJS_forgettable))
# sort removes duplicates
-include $(sort $(DEPS))

forgettable       : $(OBJS_forgettable)

$(BLDDIR)/%.o: %.c
	@echo " CC " $@
	@mkdir -p $(dir $@)
	@$(CC) -o $@ -c $< $(CFLAGS) -MMD -MF $@.deps

$(BINARIES): %:
	@echo " LD " $@
	@mkdir -p $(dir $@)
	@$(CC) -o $@ $(OBJS_$@) $(LDFLAGS)

install: $(patsubst %, $(TARGET)/bin/%, $(BINARIES))

$(TARGET)/bin/%: %
	@echo " INSTALL " $@
	@/usr/bin/install -d $(dir $@)
	@/usr/bin/install -D $< $@

clean:
	@echo " RM " $(BINARIES) $(OBJS_forgettable) $(DEPS)
	@rm -rf      $(BINARIES) $(OBJS_forgettable) $(DEPS)

.PHONY: clean install all
