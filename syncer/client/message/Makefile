TRUNKDIR=$(shell pwd -P|xargs dirname| xargs dirname| xargs dirname)
RDP_DEPS:= $(TRUNKDIR)/deps

LIBNAME = librdp_message
LIBVER = 1.0.0

CXX := g++
CXXFLAGS := \
	-g3 \
	-O2 \
	-fno-strict-aliasing \
	-Wall \
	-fPIC \
	-I$(RDP_DEPS)/protobuf/include

LDFLAGS := \
	-L$(RDP_DEPS)/protobuf/lib/

LIBS := \
	-lprotobuf

#-pthread \

SRC := $(wildcard *.cc)
HDR := $(wildcard *.h)
OBJ := $(patsubst %.cc, %.o, $(SRC))
DEP := $(patsubst %.o, %.d, $(OBJ))

SHARED_EXT = so
STATIC_EXT = a
SHARED_LIB = $(LIBNAME).$(SHARED_EXT)
STATIC_LIB = $(LIBNAME).$(STATIC_EXT)

LIBVER_MAJOR = $(shell echo $(LIBVER) | cut -d. -f1)

all:
	$(MAKE) pb
	$(MAKE) libs

libs: shared static

shared: $(SHARED_LIB).$(LIBVER) $(SHARED_LIB).$(LIBVER_MAJOR) $(SHARED_LIB)

static: $(STATIC_LIB)

pb:
	$(RDP_DEPS)/protobuf/bin/protoc --cpp_out=. -Iproto proto/*.proto
	$(RDP_DEPS)/protobuf/bin/protoc --java_out=. -Iproto proto/*.proto

$(SHARED_LIB).$(LIBVER) : $(OBJ)
	$(CXX) $^ -o $@ $(LDFLAGS) $(LIBS) -shared

$(SHARED_LIB).$(LIBVER_MAJOR) : $(SHARED_LIB).$(LIBVER)
	ln -sf $^ $@

$(SHARED_LIB) : $(SHARED_LIB).$(LIBVER)
	ln -sf $^ $@

$(STATIC_LIB) : $(OBJ)
	ar -rs $@ $^

#-include $(DEP)

%.o : %.cc
	$(CXX) -c $(CXXFLAGS) $< -o $@
%.d : %.cc
	@$(CXX) -MM $< $(CXXFLAGS) | sed 's/$(notdir $*)\.o/$(subst /,\/,$*).o $(subst /,\/,$*).d/g' > $@

clean:
	-rm -rf $(HDR) $(SRC) $(OBJ) $(DEP) *.a *.so *.so.* *.java cloud_search packages

test: shared
	$(MAKE) test -C test

PACKAGE_DIRS=include/message lib
PACKAGE_NAME=$(LIBNAME)
install:
	mkdir -p packages/$(PACKAGE_NAME)
	@for d in $(PACKAGE_DIRS); do \
		if test ! -d packages/$(PACKAGE_NAME)/$$d; then mkdir -p packages/$(PACKAGE_NAME)/$$d; fi \
	done
	/usr/bin/install -m 644 -c *.h 				packages/$(PACKAGE_NAME)/include/message
	/usr/bin/install -c $(STATIC_LIB) 			packages/$(PACKAGE_NAME)/lib
	/usr/bin/install -c $(SHARED_LIB).$(LIBVER) packages/$(PACKAGE_NAME)/lib
	/sbin/ldconfig -n packages/$(PACKAGE_NAME)/lib
	cd packages/$(PACKAGE_NAME)/lib;ln -sf $(SHARED_LIB).$(LIBVER_MAJOR) $(SHARED_LIB);ln -sf $(SHARED_LIB).$(LIBVER) $(SHARED_LIB).$(LIBVER_MAJOR);cd -;
	cp -Rf packages/$(PACKAGE_NAME) $(RDP_DEPS)

#qmodule: clean install
#	(cd packages/$(PACKAGE_NAME); qmodule_add $(LIBNAME) $(LIBVER) .)
#	(qmodule_get -m=$(LIBNAME) -v=$(LIBVER))


.PHONY: all clean test shared static pb

