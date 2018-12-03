
#############################################################################
# Makefile for building: rdp-tool
# Template: app
#############################################################################


####### Compiler, tools and options
#TRUNKDIR=$(shell pwd -P|sed -e 's/\(\/rdp-proj\).*/\1/')
TRUNKDIR=../../
RDP_DEPS:= $(TRUNKDIR)/deps

CC            = gcc
CXX           = g++
CFLAGS        = -O2 -Wall -W -fPIC $(DEFINES)
#CXXFLAGS      = -pipe -O2 -std=gnu++0x -Wall -W -fPIC $(DEFINES) 
CXXFLAGS      = -pipe -g -std=gnu++0x -Wall -W -fPIC $(DEFINES) 
INCPATH       = -I. -I $(RDP_DEPS)/jansson/include -I $(RDP_DEPS)/zklib/include
LINK          = g++
LFLAGS        = -Wl,-O1
LIBS          = -L $(RDP_DEPS)/zklib/lib/ -lzookeeper_mt -L $(RDP_DEPS)/jansson/lib/ -ljansson

SOURCES       = main.cc 
OBJECTS       = main.o 

.cc.o:
	$(CXX) $(CXXFLAGS) -c $^ $(INCPATH)
.o:
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)
 
all:zk

zk:$(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

.PHONY:clean

clean:
	rm -f $(OBJECTS) zk
