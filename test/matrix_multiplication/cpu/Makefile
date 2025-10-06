CC=g++
CXXFLAGS=\
		 -std=c++20 \
		 -O3 \
		 -ggdb \
		 -I../../../lib/hedgehog/ -I../../../lib/serializer-cpp/ \
		 -I/usr/lib/x86_64-linux-gnu/openmpi/include/ \
		 -I$(HOME)/Programming/usr/include \
		 $(shell pkg-config ompi-c --cflags) -DOMPI_SKIP_MPICXX \
		 -Wall -Wextra -Wuninitialized \
		 -MMD \
		 -fdiagnostics-color=auto
LDFLAGS=\
		$(shell pkg-config ompi-c --libs) -DOMPI_SKIP_MPICXX \
		-L$(HOME)/Programming/usr/lib/ -lopenblas
SRC=$(wildcard src/*.cpp)
OBJ=$(addprefix build/,$(SRC:src/%.cpp=%.o))
DEP=$(addprefix build/,$(SRC:src/%.cpp=%.d))

prog: $(OBJ)
	$(CC) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

build/%.o: src/%.cpp
	@mkdir -p build
	$(CC) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -rf build

-include $(DEP)
