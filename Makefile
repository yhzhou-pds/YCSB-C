
# Rocksdb的头文件
ROCKSDB_INCLUDE=/home/ubuntu/zyh/rocksdb/rocksdb-6.4.6/include
ROCKSDB_LIBRARY=/home/ubuntu/zyh/rocksdb/rocksdb-6.4.6/librocksdb.a  
#Rocksdb的静态链接库
ROCKSDB_LIB=/home/ubuntu/zyh/rocksdb/rocksdb-6.4.6

CC=g++
CFLAGS=-std=c++11 -g -Wall -pthread -I./ -I$(ROCKSDB_INCLUDE) -L$(ROCKSDB_LIB) -L$(ROCKSDB_LIBRARY)
LDFLAGS= -lpthread -lrocksdb -lz -lbz2 -llz4 -ldl -lsnappy -lpmem -lnuma -lzstd -lhdr_histogram
SUBDIRS= core db 
SUBSRCS=$(wildcard core/*.cc) $(wildcard db/*.cc)
OBJECTS=$(SUBSRCS:.cc=.o)
EXEC=ycsbc

all: $(SUBDIRS) $(EXEC)

$(SUBDIRS):
	$(MAKE) -C $@
	#$(MAKE) -C $@ ROCKSDB_INCLUDE=${ROCKSDB_INCLUDE}

$(EXEC): $(wildcard *.cc) $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@

clean:
	for dir in $(SUBDIRS); do \
		$(MAKE) -C $$dir $@; \
	done
	$(RM) $(EXEC)

.PHONY: $(SUBDIRS) $(EXEC)

