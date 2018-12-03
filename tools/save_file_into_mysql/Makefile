
#############################################################################
# Makefile for building: rdp-tool
# Template: app
#############################################################################


####### Compiler, tools and options
TRUNKDIR=../../
RDP_DEPS:= $(TRUNKDIR)/3rd_party

CC            = gcc
CXX           = g++
CFLAGS        = -O1 -Wall 
CXXFLAGS      = -O1 -Wall
LINK          = g++
LFLAGS        = -Wl,-O1
INCPATH       = -I. -I $(RDP_DEPS)/mysql-server-mysql-5.7.17/bld/include  -I $(RDP_DEPS)/mysql-server-mysql-5.7.17/include  -I $(RDP_DEPS)/mysql-server-mysql-5.7.17/libbinlogevents/export
LIBS          = -L $(RDP_DEPS)/mysql-server-mysql-5.7.17/bld/libmysql -lmysqlclient -lpthread

SOURCES       = main.cc 
OBJECTS       = main.o 

.cc.o:
	$(CXX) $(CXXFLAGS) -c $^ $(INCPATH)
.o:
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)
 
all:save_file_into_mysql

save_file_into_mysql:$(OBJECTS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

.PHONY:clean

clean:
	rm -f $(OBJECTS) save_file_into_mysql
