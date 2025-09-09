g++ -std=c++20 -o main main.cpp \
    -ggdb \
    -I../../../lib/hedgehog/ -I../../../lib/serializer-cpp/ \
    -I/usr/lib/x86_64-linux-gnu/openmpi/include/ \
    $(pkg-config /usr/lib/x86_64-linux-gnu/openmpi/lib/pkgconfig/ompi-c.pc --libs --cflags) -DOMPI_SKIP_MPICXX \
    -ltbb \
    -L$HOME/Programming/usr/lib/ -lopenblas

