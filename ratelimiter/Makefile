
# TARGET =libratelimiter.so
# CC =g++ -fPIC -g -O3 -Wall -std=c++11
# INCLUDE =$(shell find ./ -name "*.h")
# SOURCE =$(shell find ./ -name "*.cc")
# OBJS =$(SOURCE:%.cpp=%.o)

# $(TARGET):$(OBJS)
# 	$(CC) -shared -o $(TARGET) $(OBJS) $(LIBS)
	
# %.o: %.cpp $(INCLUDE)
# 	$(CC) -c $< -o $@ $(LIBS)
	
# all:$(TARGET)

# clean:
# 	rm -rf $(OBJS) $(TARGET)

CC=g++
CFLAGS=-std=c++11 -c -g -Wall
SOURCES=$(wildcard *.cc)
OBJECTS=$(SOURCES:.cc=.o)

all: $(SOURCES) $(OBJECTS)

.cc.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	$(RM) $(OBJECTS)
