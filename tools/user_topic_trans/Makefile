
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
INCPATH       = -I. -I $(RDP_DEPS)/jansson/include \
                -I ${RDP_DEPS}/librdp_message/include/message/ \
                -I ${RDP_DEPS}/zklib/include/ \
                -I ${RDP_DEPS}/librdkafka/include/librdkafka/ \
                -I ${RDP_DEPS}/protobuf/include/ \
								-I$(RDP_DEPS)/lz4/include

LINK          = g++
LFLAGS        = -Wl,-O1
LIBS          = ${RDP_DEPS}/librdp_message/lib/librdp_message.a \
								${RDP_DEPS}/protobuf/lib/libprotobuf.a \
                ${RDP_DEPS}/librdkafka/lib/librdkafka++.a \
                ${RDP_DEPS}/librdkafka/lib/librdkafka.a \
                $(RDP_DEPS)/zklib/lib/libzookeeper_mt.a \
								$(RDP_DEPS)/jansson/lib/libjansson.a \
								$(RDP_DEPS)/lz4/lib/liblz4.so \
								-lpthread  -lz -lrt 

SOURCES       = kafka_topic_translate.cpp 
OBJECTS       = kafka_topic_translate.o 

.cpp.o:
	$(CXX) $(CXXFLAGS) -c $^ $(INCPATH)
.o:
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)
 
all:kafka_topic_trans

kafka_topic_trans:$(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

.PHONY:clean

clean:
	rm -f $(OBJECTS) kafka_topic_trans
