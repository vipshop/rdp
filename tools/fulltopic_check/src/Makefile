#############################################################################
# Makefile for building: rdp-tool
# Template: app
#############################################################################


####### Compiler, tools and options
TRUNKDIR=$(shell pwd -P|sed -e 's/\(\/rdp-proj\).*/\1/')
PRO_DIR:= $(TRUNKDIR)/../../../
RDP_DEPS:= $(PRO_DIR)/deps

CC            = gcc
CXX           = g++
CFLAGS        = -g -O2 -Wall -W -fPIC $(DEFINES)
CXXFLAGS      = -g -pipe -O2 -std=gnu++0x -Wall -W -fPIC $(DEFINES)
INCPATH       = -I. \
								-I$(RDP_DEPS)/protobuf/include \
								-I$(RDP_DEPS)/librdkafka/include \
								-I$(RDP_DEPS)/glog/include \
								-I$(RDP_DEPS)/rdp-comm/include \
								-I$(RDP_DEPS)/zklib/include \
								-I$(RDP_DEPS)/gflags/include \
								-I$(RDP_DEPS)/jansson/include \
								-I$(RDP_DEPS)/librdp_message/include/message \
								-I$(RDP_DEPS)/protobuf/include \
								-I$(RDP_DEPS)/boost/include \
								-I$(RDP_DEPS)/jansson/include \
								-I$(RDP_DEPS)/lz4/include

LINK          = g++
LFLAGS        = -Wl,-O1

LIBS          = $(SUBLIBS) \
								$(RDP_DEPS)/protobuf/lib/libprotobuf.a \
								$(RDP_DEPS)/protobuf/lib/libprotoc.a \
								$(RDP_DEPS)/protobuf/lib/libprotobuf-lite.a \
								$(RDP_DEPS)/glog/lib/libglog.a \
								$(RDP_DEPS)/rdp-comm/lib/librdp-comm.a \
								$(RDP_DEPS)/zklib/lib/libzookeeper_mt.a \
								$(RDP_DEPS)/jansson/lib/libjansson.a \
								$(RDP_DEPS)/librdp_message/lib/librdp_message.a \
								$(RDP_DEPS)/protobuf/lib/libprotobuf.a \
								$(RDP_DEPS)/jansson/lib/libjansson.a \
								$(RDP_DEPS)/lz4/lib/liblz4.so \
								-lpthread \
								-L $(RDP_DEPS)/gflags/lib -lgflags \
								-L $(RDP_DEPS)/librdkafka/lib -l rdkafka++ -l rdkafka \
								-L $(RDP_DEPS)/boost/lib -l boost_system

SOURCES = $(wildcard $(TRUNKDIR)/*.cc)

OBJECTS = $(SOURCES:.cc=.o)

BINARY = rdp-topic-check

.cc.o:
	$(CXX) $(CFLAGS) -c $^ $(INCPATH)
.o:
	$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)

$(BINARY): $(OBJECTS)
	$(CXX) $(CFLAGS) -o $@ $^ $(LIBS)

all:$(BINARY)

clean:
	rm -f $(BINARY)
	rm -rf $(OBJECTS)
