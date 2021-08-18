
# Rocksdb的头文件
#ROCKSDB_INCLUDE=/home/ubuntu/zyh/Cloud/rocksCloud/include
# RocksDB 的静态链接库
#ROCKSDB_LIBRARY=/home/ubuntu/zyh/Cloud/rocksCloud/build/librocksdb.a 
#ROCKSDB_LIB=/home/ubuntu/zyh/Cloud/rocksCloud/build/

ROCKSDB_INCLUDE=/home/mxh/rocksdb/include
ROCKSDB_LIBRARY=/home/mxh/rocksdb/librocksdb.a
ROCKSDB_LIB=/home/mxh/rocksdb

CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread -I./ -I$(ROCKSDB_INCLUDE) -L$(ROCKSDB_LIB)
LDFLAGS= -lpthread -lrocksdb -lz -lbz2 -llz4 -ldl -lsnappy -lpmem -lnuma -lzstd -lhdr_histogram -lboost_regex -lboost_iostreams
SUBDIRS= core db 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	#$(MAKE) -C $@
	$(MAKE) -C $@ ROCKSDB_INCLUDE=${ROCKSDB_INCLUDE} ROCKSDB_LIBRARY=${ROCKSDB_LIBRARY}

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

