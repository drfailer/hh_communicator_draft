TRACER_PATH=$(HOME)/Programming/projects/tracer-c/tracer/
CC=g++
CXXFLAGS=\
		 -std=c++20 \
		 -O3 \
		 -ggdb \
		 -Wall -Wextra -Wuninitialized -Wconversion \
		 -MMD \
		 -fdiagnostics-color=auto
LDFLAGS=
SRC=$(wildcard *.cpp)
OBJ=$(addprefix build/,$(SRC:%.cpp=%.o))
DEP=$(addprefix build/,$(SRC:%.cpp=%.d))

prog: $(OBJ)
	$(CC) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

build/%.o: %.cpp
	@mkdir -p build
	$(CC) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -rf build

-include $(DEP)
