CC=g++
CFLAGS=-std=c++17 -g -Wall -pthread -I./  -D SPLINTERDB_PLATFORM_DIR=platform_linux
LDFLAGS= -lpthread -ltbb -lhiredis -lsplinterdb -lrocksdb -lnuma
SUBDIRS=core db retwis
SUBCPPSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc) $(wildcard retwis/*.cc)
SUBCSRCS=$(wildcard core/*.c) $(wildcard db/*.c) $(wildcard retwis/*.c)
OBJECTS=$(SUBCPPSRCS:.cc=.o) $(SUBCSRCS:.c=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

