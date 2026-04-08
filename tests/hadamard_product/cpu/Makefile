# include config/blackbird.mk
include config/desktop.mk
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
