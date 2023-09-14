CC=g++
CFLAGS=-std=c++11 -c -g -O3 -Wall -D SPLINTERDB_PLATFORM_DIR=platform_linux
CPPSOURCES=$(wildcard *.cc)
CSOURCES=$(wildcard *.c)
OBJECTS=$(CPPSOURCES:.cc=.o) $(CSOURCES:.c=.o)

all: $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)
