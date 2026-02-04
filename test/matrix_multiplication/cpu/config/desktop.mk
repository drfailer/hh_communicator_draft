TRACER_PATH=$(HOME)/Programming/projects/tracer-c/tracer/

CC=g++
CXXFLAGS=\
		 -std=c++20 \
		 -O3 \
		 -ggdb \
		 -I../../../lib/hedgehog/ -I../../../src/ \
		 -I$(HOME)/Programming/usr/include/ \
		 -I$(TRACER_PATH) \
		 -I/usr/lib/x86_64-linux-gnu/openmpi/include/ \
  		 $(shell pkg-config /usr/lib/x86_64-linux-gnu/openmpi/lib/pkgconfig/ompi-c.pc --cflags) -DOMPI_SKIP_MPICXX \
		 -Wall -Wextra -Wuninitialized -Wconversion \
		 -MMD \
		 -fdiagnostics-color=auto
LDFLAGS=\
		-L$(HOME)/Programming/usr/lib/ \
		$(shell pkg-config /usr/lib/x86_64-linux-gnu/openmpi/lib/pkgconfig/ompi-c.pc --libs) -DOMPI_SKIP_MPICXX \
		-ltbb \
		-lopenblas -lpmix
