CLH_PATH=$(HOME)/Programming/projects/clh.git/v2/
TRACER_PATH=$(HOME)/Programming/projects/tracer-c/tracer/

CC=g++
CXXFLAGS=\
		 -std=c++20 \
		 -O3 \
		 -ggdb \
		 -I../../../lib/hedgehog/ -I../../../lib/serializer-cpp/ \
		 -I$(HOME)/Programming/usr/include/ \
		 -I$(CLH_PATH) -I$(TRACER_PATH) \
		 -Wall -Wextra -Wuninitialized \
		 -MMD \
		 -fdiagnostics-color=auto
LDFLAGS=\
		-L$(HOME)/Programming/usr/lib/ \
		-L$(CLH_PATH)/build/lib/ \
		-lclh -lopenblas -lpmix \
		-Wl,-rpath,$(CLH_PATH)/build/lib/
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
