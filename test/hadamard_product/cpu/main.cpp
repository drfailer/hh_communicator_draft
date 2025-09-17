#include "../../../lib/serializer-cpp/serializer/serializer.hpp"
#include "../../../src/communicator/communicator_core_task.hpp"
#include "../../../src/communicator/communicator_task.hpp"
#include "../../common/ap.h"
#include "../../common/utest.h"
#include "../../common/timer.h"
#include "log.hpp"
#include <hedgehog/hedgehog.h>
#include <iostream>

template <typename T>
struct Matrix {
    size_t rows;
    size_t cols;
    size_t ld;
    T     *mem;

    Matrix(size_t rows, size_t cols)
        : rows(rows),
          cols(cols),
          ld(cols),
          mem(new T[cols * rows]) {}

    ~Matrix() {
        delete[] mem;
    }
};

template <typename T>
struct MatrixTile {
    size_t            rows = 0;
    size_t            cols = 0;
    size_t            matrixRows = 0;
    size_t            matrixCols = 0;
    size_t            rowIdx = 32;
    size_t            colIdx = 32;
    size_t            ld = 0;
    size_t            size = 0;
    T                *mem = nullptr;
    size_t            processCount = 0;
    serializer::Bytes buf;

    size_t bufSize() const {
        return 8 * 8 // rows, cols, matrixRows, matrixCols, row, col, ld, size
               + (1 + size * sizeof(T)); // v, mem[]
    }

    MatrixTile(size_t rows, size_t cols)
        : rows(rows),
          cols(cols),
          ld(cols),
          size(rows * cols),
          mem(new T[size]) {}

    ~MatrixTile() {
        delete[] mem;
    }

    void set(size_t rowIdx, size_t colIdx, size_t rows, size_t cols, size_t matrixRows, size_t matrixCols) {
        this->rowIdx = rowIdx;
        this->colIdx = colIdx;
        this->rows = rows;
        this->cols = cols;
        this->matrixRows = matrixRows;
        this->matrixCols = matrixCols;
    }

    MatrixTile(MatrixTile<T> const &) = delete;
    MatrixTile<T> const &operator=(MatrixTile<T> const &) = delete;

    hh::comm::Package package() {
        buf.resize(bufSize());
        return hh::comm::Package{.data = {hh::comm::Buffer{(char *)buf.data(), buf.size()}}};
    }

    hh::comm::Package pack() {
        namespace ser = serializer;
        using Ser = ser::Serializer<ser::Bytes>;
        ser::serialize<Ser>(buf, 0, rows, cols, matrixRows, matrixCols, rowIdx, colIdx, ld, size,
                            ser::tools::PointerArray(mem, size));
        assert(buf.size() == bufSize());
        return hh::comm::Package{.data = {hh::comm::Buffer{(char *)buf.data(), buf.size()}}};
    }

    void unpack(hh::comm::Package &&p) {
        assert(p.data[0].mem == (char *)buf.data());
        namespace ser = serializer;
        using Ser = ser::Serializer<ser::Bytes>;
        ser::deserialize<Ser>(buf, 0, rows, cols, matrixRows, matrixCols, rowIdx, colIdx, ld, size,
                              ser::tools::PointerArray(mem, size));
    }
};

template <typename T>
struct MatrixTriplet {
    std::shared_ptr<Matrix<T>> a;
    std::shared_ptr<Matrix<T>> b;
    std::shared_ptr<Matrix<T>> c;
};

template <typename T>
struct TileTriplet {
    std::shared_ptr<MatrixTile<T>> a;
    std::shared_ptr<MatrixTile<T>> b;
    std::shared_ptr<MatrixTile<T>> c;

    TileTriplet(size_t rows, size_t cols)
        : a(std::make_shared<MatrixTile<T>>(rows, cols)),
          b(std::make_shared<MatrixTile<T>>(rows, cols)),
          c(std::make_shared<MatrixTile<T>>(rows, cols)) {}

    hh::comm::Package package() {
        return hh::comm::Package{.data = {
                                     a->package().data[0],
                                     b->package().data[0],
                                     c->package().data[0],
                                 }};
    }

    hh::comm::Package pack() {
        return hh::comm::Package{.data = {
                                     a->pack().data[0],
                                     b->pack().data[0],
                                     c->pack().data[0],
                                 }};
    }

    void unpack(hh::comm::Package &&package) {
        a->unpack(hh::comm::Package{.data = {{package.data[0].mem, package.data[0].len}}});
        b->unpack(hh::comm::Package{.data = {{package.data[1].mem, package.data[1].len}}});
        c->unpack(hh::comm::Package{.data = {{package.data[2].mem, package.data[2].len}}});
    }
};

template <typename T>
struct MatrixTilePair {
    std::shared_ptr<Matrix<T>>      matrix;
    std::shared_ptr<TileTriplet<T>> tiles;
};

#define CopyStateIO 2, TileTriplet<T>, MatrixTriplet<T>, MatrixTilePair<T>

template <typename T>
struct CopyState : hh::AbstractState<CopyStateIO> {
    std::shared_ptr<Matrix<T>>                   dest;
    std::vector<std::shared_ptr<TileTriplet<T>>> queue;

    void execute(std::shared_ptr<MatrixTriplet<T>> data) override {
        dest = data->c;
        if (!queue.empty()) {
            for (auto tiles : queue) {
                this->addResult(std::make_shared<MatrixTilePair<T>>(dest, tiles));
            }
        }
    }

    void execute(std::shared_ptr<TileTriplet<T>> data) {
        if (!dest) {
            queue.emplace_back(data);
        } else {
            this->addResult(std::make_shared<MatrixTilePair<T>>(dest, data));
        }
    }
};

#define EndStateIO 1, MatrixTilePair<T>, Matrix<T>

template <typename T>
struct EndState : hh::AbstractState<EndStateIO> {
    size_t                                                               count = 0;
    std::shared_ptr<Matrix<T>>                                           c = nullptr;
    size_t                                                               tileSize = 0;
    std::shared_ptr<hh::tool::CommunicatorMemoryManager<TileTriplet<T>>> tileMM = nullptr;

    EndState(size_t tileSize, std::shared_ptr<hh::tool::CommunicatorMemoryManager<TileTriplet<T>>> tileMM)
        : tileSize(tileSize),
          tileMM(tileMM) {}

    void execute(std::shared_ptr<MatrixTilePair<T>> data) {
        logh::infog(logh::IG::EndState, "end state", "receive data");

        if (!c) {
            c = data->matrix;
            size_t nbTileRow = c->rows / tileSize + (c->rows % tileSize > 0 ? 1 : 0);
            size_t nbTileCol = c->cols / tileSize + (c->cols % tileSize > 0 ? 1 : 0);
            count = nbTileRow * nbTileCol - 1;
        } else {
            --count;
            if (count == 0) {
                this->addResult(c);
                logh::infog(logh::IG::EndState, "end state", "matrix C completed");
            }
        }
        tileMM->returnMemory(std::move(data->tiles));
    }
};

#define HadamardProductGraphIO 1, MatrixTriplet<T>, Matrix<T>

template <typename T>
struct HadamardProductGraph : hh::Graph<HadamardProductGraphIO> {
    HadamardProductGraph(hh::comm::CommHandle *commHandle, size_t tileSize)
        : hh::Graph<HadamardProductGraphIO>("HadamardProductGraph") {
        auto tileMM = std::make_shared<hh::tool::CommunicatorMemoryManager<TileTriplet<T>>>();
        tileMM->template preallocate<TileTriplet<T>>(200, tileSize, tileSize);

        std::vector<int> scatterTaskReceivers;
        for (int i = 1; i < commHandle->nbProcesses; ++i) {
            scatterTaskReceivers.emplace_back(i);
        }
        auto scatterTask = std::make_shared<hh::CommunicatorTask<TileTriplet<T>>>(
            commHandle, scatterTaskReceivers, hh::CommunicatorTaskOpt{.sendersAreReceivers = true, .scatter = true},
            "ScatterTask");
        auto gatherTask = std::make_shared<hh::CommunicatorTask<TileTriplet<T>>>(
            commHandle, std::vector<int>({0}), hh::CommunicatorTaskOpt{.sendersAreReceivers = false, .scatter = false},
            "GatherTask");

        scatterTask->setMemoryManager(tileMM);
        gatherTask->setMemoryManager(tileMM);

        auto splitTask = std::make_shared<hh::LambdaTask<1, MatrixTriplet<T>, TileTriplet<T>>>("splitTask", 20 / commHandle->nbProcesses);
        auto productTask = std::make_shared<hh::LambdaTask<1, TileTriplet<T>, TileTriplet<T>>>("ProductTask", 40 / commHandle->nbProcesses);
        auto copyTask = std::make_shared<hh::LambdaTask<1, MatrixTilePair<T>, MatrixTilePair<T>>>("CopyTask", 20 / commHandle->nbProcesses);
        auto copyState = std::make_shared<hh::StateManager<CopyStateIO>>(std::make_shared<CopyState<T>>(), "CopyState");
        auto endState = std::make_shared<hh::StateManager<EndStateIO>>(std::make_shared<EndState<T>>(tileSize, tileMM),
                                                                       "EndState");

        splitTask->template setLambda<MatrixTriplet<T>>([=](std::shared_ptr<MatrixTriplet<T>> data, auto self) {
            size_t nbTileRow = data->a->rows / tileSize + (data->a->rows % tileSize > 0 ? 1 : 0);
            size_t nbTileCol = data->a->cols / tileSize + (data->a->cols % tileSize > 0 ? 1 : 0);

            for (size_t rowIdx = 0; rowIdx < nbTileRow; ++rowIdx) {
                for (size_t colIdx = 0; colIdx < nbTileCol; ++colIdx) {
                    auto   tiles = tileMM->template getMemory<TileTriplet<T>>(true);
                    size_t tileRows = std::min(tileSize, data->a->rows - rowIdx * tileSize);
                    size_t tileCols = std::min(tileSize, data->a->cols - colIdx * tileSize);

                    tiles->a->set(rowIdx, colIdx, tileRows, tileCols, data->a->rows, data->a->cols);
                    tiles->b->set(rowIdx, colIdx, tileRows, tileCols, data->b->rows, data->b->cols);
                    tiles->c->set(rowIdx, colIdx, tileRows, tileCols, data->c->rows, data->c->cols);
                    for (size_t trow = 0; trow < tileRows; ++trow) {
                        for (size_t tcol = 0; tcol < tileCols; ++tcol) {
                            size_t mrow = rowIdx * tileSize + trow;
                            size_t mcol = colIdx * tileSize + tcol;
                            tiles->a->mem[tcol + trow * tiles->a->ld] = data->a->mem[mcol + mrow * data->a->ld];
                            tiles->b->mem[tcol + trow * tiles->b->ld] = data->b->mem[mcol + mrow * data->b->ld];
                            tiles->c->mem[tcol + trow * tiles->c->ld] = data->c->mem[mcol + mrow * data->c->ld];
                        }
                    }
                    logh::infog(logh::IG::SplitTask, "split task", "add tile: rank = ", commHandle->rank);
                    self.addResult(tiles);
                }
            }
        });
        productTask->template setLambda<TileTriplet<T>>([=](std::shared_ptr<TileTriplet<T>> data, auto self) {
            logh::infog(logh::IG::ProductTask, "product task", "process tile: rank = ", commHandle->rank, " tile[",
                        data->c->rowIdx, ",", data->c->colIdx, "]");
            for (size_t row = 0; row < data->a->rows; ++row) {
                for (size_t col = 0; col < data->a->cols; ++col) {
                    // logh::info("A[", row, ",", col, "] = ", data->a->mem[col + row * data->a->ld]);
                    // logh::info("B[", row, ",", col, "] = ", data->b->mem[col + row * data->b->ld]);
                    data->c->mem[col + row * data->c->ld]
                        = data->a->mem[col + row * data->a->ld] * data->b->mem[col + row * data->b->ld];
                    // logh::info("C[", row, ",", col, "] = ", data->c->mem[col + row * data->c->ld]);
                }
            }
            self.addResult(data);
        });
        copyTask->template setLambda<MatrixTilePair<T>>([=](std::shared_ptr<MatrixTilePair<T>> data, auto self) {
            auto tile = data->tiles->c;
            logh::infog(logh::IG::CopyTask, "copy task", "process tile: rank = ", commHandle->rank, " tile[",
                        tile->rowIdx, ",", tile->colIdx, "]");
            for (size_t trow = 0; trow < tile->rows; ++trow) {
                for (size_t tcol = 0; tcol < tile->cols; ++tcol) {
                    size_t mrow = tile->rowIdx * tileSize + trow;
                    size_t mcol = tile->colIdx * tileSize + tcol;
                    data->matrix->mem[mcol + mrow * data->matrix->ld] = tile->mem[tcol + trow * tile->ld];
                }
            }
            self.addResult(data);
        });

        this->inputs(splitTask);
        this->inputs(copyState);
        this->edges(splitTask, scatterTask);
        this->edges(scatterTask, productTask);
        this->edges(productTask, gatherTask);
        this->edges(gatherTask, copyState);
        this->edges(copyState, copyTask);
        this->edges(copyTask, endState);
        this->outputs(endState);
    }
};

template <typename T>
std::shared_ptr<Matrix<T>> createMatrix(size_t cols, size_t rows) {
    auto matrix = std::make_shared<Matrix<T>>(cols, rows);

    for (size_t i = 0; i < rows; ++i) {
        for (size_t j = 0; j < cols; ++j) {
            if constexpr (std::is_floating_point_v<T>) {
                matrix->mem[i * cols + j] = 1. / T(i * cols + j + 1);
            } else {
                matrix->mem[i * cols + j] = i * cols + j + 1;
            }
        }
    }
    return matrix;
}

template <typename T>
UTest(hadamardProduct, std::shared_ptr<Matrix<T>> A, std::shared_ptr<Matrix<T>> B, std::shared_ptr<Matrix<T>> C) {
    for (size_t row = 0; row < C->rows; ++row) {
        for (size_t col = 0; col < C->cols; ++col) {
            T eval = A->mem[col + row * A->ld] * B->mem[col + row * B->ld];
            T fval = C->mem[col + row * C->ld];

            if (!((eval - 1e-3) <= fval && fval <= (eval + 1e-3))) {
                logh::error("C[", row, ",", col, "] = ", fval, ", expected = ", eval);
                return;
            }
        }
    }
    logh::info("test success");
}

template <typename T>
void printMatrix(std::string const &name, std::shared_ptr<Matrix<T>> matrix) {
    std::cout << name << " =" << std::endl;

    for (size_t i = 0; i < matrix->rows; ++i) {
        std::cout << "   ";
        for (size_t j = 0; j < matrix->cols; ++j) {
            std::cout << matrix->mem[i * matrix->ld + j] << " ";
        }
        std::cout << std::endl;
    }
}

struct Config {
    size_t M;
    size_t N;
    size_t tileSize;
};

Config parseArgs(int argc, char **argv) {
    Config             config;
    ap::ArgumentParser ap = ap::argument_parser_create(argc, argv);

    ap::add_size_t_arg(&ap, "-M", &config.M, 1024);
    ap::add_size_t_arg(&ap, "-N", &config.N, 1024);
    ap::add_size_t_arg(&ap, "-tileSize", &config.tileSize, 32);
    auto status = argument_parser_run(&ap);

    if (status != ap::ArgumentParserStatus::Ok) {
        exit(status == ap::ArgumentParserStatus::Help ? 0 : 1);
    }

    logh::info("config = { M = ", config.M, ", N = ", config.N, ", tileSize = ", config.tileSize, " }");
    return config;
}

int main(int argc, char **argv) {
    Config config = parseArgs(argc, argv);

    using MatrixType = size_t;
    hh::comm::commInit(argc, argv);
    hh::comm::CommHandle *commHandle = hh::comm::commCreate(true);
    auto                  A = createMatrix<MatrixType>(config.M, config.N);
    auto                  B = createMatrix<MatrixType>(config.M, config.N);
    auto                  C = createMatrix<MatrixType>(config.M, config.N);

    // TODO
    HadamardProductGraph<MatrixType> graph(commHandle, config.tileSize);

    hh::GraphSignalHandler<1, MatrixTriplet<MatrixType>, Matrix<MatrixType>>::registerGraph(&graph);
    hh::GraphSignalHandler<1, MatrixTriplet<MatrixType>, Matrix<MatrixType>>::handleSignal(SIGTERM);
    hh::GraphSignalHandler<1, MatrixTriplet<MatrixType>, Matrix<MatrixType>>::handleSignal(SIGKILL);

    graph.executeGraph(true);
    timer_start(graph);
    if (commHandle->rank == 0) {
        graph.pushData(std::make_shared<MatrixTriplet<MatrixType>>(A, B, C));
    }
    hh::comm::commBarrier();
    std::cout << "finish pushing data" << std::endl;
    graph.finishPushingData();
    graph.waitForTermination();
    timer_end(graph);
    if (commHandle->rank == 0) {
        timer_report_prec(graph, milliseconds);
    }
    std::cout << "graph terminated" << std::endl;
    graph.createDotFile(std::to_string(commHandle->rank) + "graph.dot", hh::ColorScheme::EXECUTION,
                        hh::StructureOptions::QUEUE);
    hh::comm::commBarrier();

    if (commHandle->rank == 0 && config.M < 10 && config.N < 10) {
        printMatrix("A", A);
        printMatrix("B", B);
        printMatrix("C", C);
    }

    if (commHandle->rank == 0) {
        utest_start();
        urun_test(hadamardProduct<MatrixType>, A, B, C);
        utest_end();
    }

    hh::comm::commDestroy(commHandle);
    hh::comm::commFinalize();
    return 0;
}
