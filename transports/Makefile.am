JANUS_PATH=/usr/local
CFLAGS = $(shell pkg-config --cflags glib-2.0) -std=c++17 -Wno-permissive -I$(JANUS_PATH)/include/janus
AM_CXXFLAGS = $(CFLAGS) -std=c++17 -I/usr/include -I$(JANUS_PATH)/include -Wno-permissive -I$(JANUS_PATH)/include/janus
LIBS = $(shell pkg-config --libs glib-2.0)

lib_LTLIBRARIES = libjanus_skeleton_transport.la
libjanus_skeleton_transport_la_SOURCES = janus_skeleton_transport.cpp \
                                         janus_skeleton_transport_lifecycle.cpp \
                                         janus_skeleton_transport_sessions.cpp \
                                         janus_skeleton_transport_requests.cpp \
                                         janus_skeleton_transport_comm.cpp
libjanus_skeleton_transport_la_CXXFLAGS = $(AM_CXXFLAGS) -std=c++17 -Wno-permissive

libjanus_skeleton_transport_la_LDFLAGS = -version-info 0:0:0 $(shell pkg-config --libs glib-2.0) -L$(JANUS_PATH)/lib
libdir = $(exec_prefix)/lib/janus/transports

