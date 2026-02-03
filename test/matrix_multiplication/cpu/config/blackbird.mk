CC=g++
CXXFLAGS=\
		 -std=c++20 \
		 -O3 \
		 -ggdb \
		 -I../../../lib/hedgehog/ -I../../../lib/serializer-cpp/ -I../../../src/ \
		 -I../../../../tracer-c/tracer/ \
  		 -DOMPI_SKIP_MPICXX \
		 -Wall -Wextra -Wuninitialized \
		 -MMD \
		 -fdiagnostics-color=auto
LDFLAGS=\
		-l:libclh.a -lucp -lucs -lpmix \
		-lmpi -DOMPI_SKIP_MPICXX \
		-ltbb \
		-lopenblas -llapacke
