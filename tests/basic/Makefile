CC=g++
CXXFLAGS=\
		 -std=c++20 \
		 -Wall -Wextra -Wuninitialized \
		 -MMD -g -fdiagnostics-color=auto \
		 -I/usr/lib/x86_64-linux-gnu/openmpi/include/ \
  		 $(shell pkg-config /usr/lib/x86_64-linux-gnu/openmpi/lib/pkgconfig/ompi-c.pc --cflags) -DOMPI_SKIP_MPICXX \
		 -I../../src/ -I../../build/_deps/hedgehog-src/hedgehog/ -I../common
LDFLAGS=\
		-L$(HOME)/Programming/usr/lib/ \
		$(shell pkg-config /usr/lib/x86_64-linux-gnu/openmpi/lib/pkgconfig/ompi-c.pc --libs) -DOMPI_SKIP_MPICXX \
		-ltbb \
		-lopenblas -lpmix
SRC=$(wildcard *.cpp)
OBJ=$(addprefix build/,$(SRC:.cpp=.o))
DEP=$(addprefix build/,$(SRC:.cpp=.d))

prog: $(OBJ)
	$(CC) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

build/%.o: %.cpp
	@mkdir -p build
	$(CC) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -rf build

-include $(DEP)
