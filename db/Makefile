ROCKSDB_INCLUDE=/home/ubuntu/zyh/rocksdb/rocksdb-6.4.6/include
ROCKSDB_LIBRARY=/home/ubuntu/zyh/rocksdb/rocksdb-6.4.6/librocksdb

CC=g++
CFLAGS=-std=c++11 -c -g -Wall -fgnu-tm -lrocksdb
INCLUDES=-I../ -I$(ROCKSDB_INCLUDE) -L$(ROCKSDB_LIBRARY)
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $(INCLUDES) $< -o $@

clean:
	$(RM) $(OBJECTS)

