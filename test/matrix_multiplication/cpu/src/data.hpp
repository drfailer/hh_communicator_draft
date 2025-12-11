#ifndef DATA
#define DATA
#include "../../../../src/communicator/communicator_memory_manager.hpp"
#include "../../../../src/communicator/package.hpp"
#include "log.hpp"
#include <cstddef>
#include <serializer/serializer.hpp>

using MT = double;

enum class MatrixId {
    A,
    B,
    C,
    P,
};

template <typename T, MatrixId Id>
struct Matrix {
    size_t rows;
    size_t cols;
    size_t ld;
    T     *mem;

    Matrix(size_t rows, size_t cols)
        : rows(rows),
          cols(cols),
          ld(cols),
          mem(new T[rows * cols]) {}
    ~Matrix() {
        delete[] mem;
    }

    template <MatrixId OtherId>
    Matrix(Matrix<T, OtherId> const &) = delete;

    template <MatrixId OtherId>
    Matrix<T, Id> const &operator=(Matrix<T, OtherId> const &) = delete;
};

template <typename T, MatrixId Id>
struct MatrixTile {
    size_t            tileSize = 0;
    size_t            rows = 0;
    size_t            cols = 0;
    size_t            ld = 0;
    size_t            rowIdx = 0;
    size_t            colIdx = 0;
    size_t            matrixRows = 0;
    size_t            matrixCols = 0;
    T                *mem = nullptr;
    size_t            processCount = 0;
    bool              sent = true;
    serializer::Bytes buf;

    size_t bufSize() const {
        return 8 * 8 // rows, cols, matrixRows, matrixCols, row, col, ld, size
               + (1 + tileSize * tileSize * sizeof(T)); // v, mem[]
    }

    MatrixTile(size_t tileSize)
        : tileSize(tileSize),
          rows(tileSize),
          cols(tileSize),
          ld(tileSize),
          mem(new T[tileSize * tileSize]),
          buf(bufSize()) {}

    ~MatrixTile() {
        delete[] mem;
    }

    template <MatrixId OtherId>
    MatrixTile(MatrixTile<T, OtherId> const &) = delete;

    template <MatrixId OtherId>
    MatrixTile<T, Id> const &operator=(MatrixTile<T, OtherId> const &) = delete;

    void set(size_t rowIdx, size_t colIdx, size_t matrixRows, size_t matrixCols) {
        this->rowIdx = rowIdx;
        this->colIdx = colIdx;
        this->matrixRows = matrixRows;
        this->matrixCols = matrixCols;
    }

    // hh::comm::Package package() {
    //     return hh::comm::Package{.data = {hh::comm::Buffer{(char *)buf.data(), bufSize()}}};
    // }
    // hh::comm::Package pack() {
    //     namespace ser = serializer;
    //     using Ser = ser::Serializer<ser::Bytes>;
    //     ser::serialize<Ser>(buf, 0, tileSize, rows, cols, ld, rowIdx, colIdx, matrixRows, matrixCols,
    //                         ser::tools::PointerArray(mem, tileSize, tileSize));
    //     return hh::comm::Package{.data = {hh::comm::Buffer{(char *)buf.data(), bufSize()}}};
    // }
    // void unpack(hh::comm::Package &&p) {
    //     assert(p.data[0].mem == (char *)buf.data());
    //     namespace ser = serializer;
    //     using Ser = ser::Serializer<ser::Bytes>;
    //     ser::deserialize<Ser>(buf, 0, tileSize, rows, cols, ld, rowIdx, colIdx, matrixRows, matrixCols,
    //                           ser::tools::PointerArray(mem, tileSize, tileSize));
    // }

    hh::comm::Package package() {
        return hh::comm::Package{.data = {
                                     hh::comm::Buffer{(char *)this, 8 * 8},
                                     hh::comm::Buffer{(char *)mem, tileSize * tileSize * sizeof(*mem)},
                                 }};
    }
    hh::comm::Package pack() {
        return hh::comm::Package{.data = {
                                     hh::comm::Buffer{(char *)this, 8 * 8},
                                     hh::comm::Buffer{(char *)mem, tileSize * tileSize * sizeof(*mem)},
                                 }};
    }
    void unpack(hh::comm::Package &&) {}

    void preSend() {
        sent = false;
    }

    void postSend() {
        sent = true;
    }

    void cleanMemory() {
        sent = true;
        processCount = 0;
    }

    bool canBeRecycled() const {
        return processCount == 0 && sent;
    }
};

template <typename T>
struct ProductData {
    std::shared_ptr<MatrixTile<T, MatrixId::A>> a;
    std::shared_ptr<MatrixTile<T, MatrixId::B>> b;
    std::shared_ptr<MatrixTile<T, MatrixId::P>> p;
};

template <typename T>
struct SumData {
    std::shared_ptr<MatrixTile<T, MatrixId::P>> p;
    std::shared_ptr<MatrixTile<T, MatrixId::C>> c;
};

using MatrixTilePair
    = std::pair<std::shared_ptr<Matrix<MT, MatrixId::C>>, std::shared_ptr<MatrixTile<MT, MatrixId::C>>>;

using MMType = hh::tool::MemoryPool<MatrixTile<MT, MatrixId::A>, MatrixTile<MT, MatrixId::B>,
                                    MatrixTile<MT, MatrixId::C>, MatrixTile<MT, MatrixId::P>>;

#endif
