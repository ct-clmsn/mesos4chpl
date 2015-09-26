INSTALL_DIR=$(HOME)/opt/bin
CXXFLAGS=-g -O2 -std=c++0x -L/usr/local/lib -I$(HOME)/opt/include

# -lprotobuf is linked into -lmesos statically...don't include it here or else you'll a glibc
# double "free" error. Some global variables defined in -lprotobuf end up getting freed twice
# when the mesos runtime shuts down
#
LDFLAGS=-pthread -lmesos

LIBJAVADIR=/usr/java/latest/jre/lib/amd64/server/
CXX=g++
CXXCOMPILE=$(CXX) $(INCLUDES) $(CXXFLAGS) -L/usr/local/lib -DLIBJAVADIR=\"$(LIBJAVADIR)\" -c -i $@
CXXLINK=$(CXX) $(INCLUDES) $(CXXFLAGS) -L/usr/local/lib -DLIBJAVADIR=\"$(LIBJAVADIR)\" -o $@

default: all
all: chpl-scheduler

chpl-scheduler: ChapelScheduler.cpp
	$(CXXLINK) $< $(LDFLAGS)

%: %.cpp
	$(CXXLINK) $< $(LDFLAGS)

clean:
	rm chpl-scheduler

install: all
	cp chpl-scheduler $(INSTALL_DIR)

uninstall: clean
	rm $(INSTALL_DIR)/chpl-scheduler

