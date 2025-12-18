#include "../../../common/ap.h"
#include "../../../common/timer.h"
#include "../../../common/utest.h"
#include "graph.hpp"
#include <cblas.h>
#include <communicator/service/clh_service.hpp>
#include <communicator/service/mpi_service.hpp>

hh::comm::rank_t GLOBAL_RANK = 0;

template <MatrixId Id>
std::shared_ptr<Matrix<MT, Id>> createMatrix(size_t cols, size_t rows) {
    auto matrix = std::make_shared<Matrix<MT, Id>>(cols, rows);

    for (size_t i = 0; i < rows; ++i) {
        for (size_t j = 0; j < cols; ++j) {
            if constexpr (std::is_floating_point_v<MT>) {
                matrix->mem[i * cols + j] = 1. / MT(i * cols + j + 1);
            } else {
                matrix->mem[i * cols + j] = i * cols + j + 1;
            }
        }
    }
    return matrix;
}

template <MatrixId Id>
void printMatrix(std::string const &name, std::shared_ptr<Matrix<MT, Id>> matrix) {
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
    size_t K;
    size_t tileSize;
    size_t poolSize;
    size_t threads;
};

Config parseArgs(int argc, char **argv) {
    Config             config;
    ap::ArgumentParser ap = ap::argument_parser_create(argc, argv, "C[m,n] = A[m,k]*B[k,n] + C[m,n]");

    ap::add_size_t_arg(&ap, "-M", &config.M, 1024);
    ap::add_size_t_arg(&ap, "-N", &config.N, 1024);
    ap::add_size_t_arg(&ap, "-K", &config.K, 1024);
    ap::add_size_t_arg(&ap, "-tileSize", &config.tileSize, 256);
    ap::add_size_t_arg(&ap, "-poolSize", &config.poolSize, 256,
                       "size of the memory pool for the block (there are 4 pools for A, B, C and P blocks)");
    ap::add_size_t_arg(&ap, "-threads", &config.threads, 40);
    auto status = ap::argument_parser_run(&ap);

    if (status != ap::ArgumentParserStatus::Ok) {
        exit(status == ap::ArgumentParserStatus::Help ? 0 : 1);
    }

    logh::info("config = { M = ", config.M, ", N = ", config.N, ", K = ", config.K, ", tileSize = ", config.tileSize,
               " }");

    return config;
}

void matmul(std::shared_ptr<Matrix<MT, MatrixId::A>> A, std::shared_ptr<Matrix<MT, MatrixId::B>> B,
            std::shared_ptr<Matrix<MT, MatrixId::C>> C) {
    blasint M = (blasint)C->rows;
    blasint N = (blasint)C->cols;
    blasint K = (blasint)A->cols;

    if constexpr (std::is_same_v<MT, float>) {
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, M, N, K, 1.f, (const float *)A->mem, (blasint)A->ld,
                    (const float *)B->mem, (blasint)B->ld, 0, (float *)C->mem, (blasint)C->ld);
    } else if constexpr (std::is_same_v<MT, double>) {
        timer_start(cblas);
        cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, M, N, K, 1.f, (const double *)A->mem, (blasint)A->ld,
                    (const double *)B->mem, (blasint)B->ld, 0, (double *)C->mem, (blasint)C->ld);
        timer_end(cblas);
        timer_report_prec(cblas, milliseconds);
    } else {
        TODO("unsuported type for test");
    }
}

UTest(mm_result, std::shared_ptr<Matrix<MT, MatrixId::C>> C, std::shared_ptr<Matrix<MT, MatrixId::C>> E) {
    for (size_t row = 0; row < C->rows; ++row) {
        for (size_t col = 0; col < C->cols; ++col) {
            MT eval = E->mem[col + row * C->ld];
            MT fval = C->mem[col + row * C->ld];
            if constexpr (std::is_floating_point_v<MT>) {
                uassert_float_equal(fval, eval, (MT)1e-6);
            } else {
                uassert_equal(fval, eval);
            }
        }
    }
    logh::info("test success");
}

int main(int argc, char **argv) {
    // hh::comm::CommService *service = new hh::comm::CLHService(true);
    hh::comm::CommService *service = new hh::comm::MPIService(&argc, &argv, true);
    Config config = parseArgs(argc, argv);

    // TODO: we need an function in comm tool to interface this
    GLOBAL_RANK = service->rank();

    std::shared_ptr<Matrix<MT, MatrixId::A>> A = nullptr;
    std::shared_ptr<Matrix<MT, MatrixId::B>> B = nullptr;
    std::shared_ptr<Matrix<MT, MatrixId::C>> C = nullptr;
    std::shared_ptr<Matrix<MT, MatrixId::C>> E = nullptr;

    if (service->rank() == 0) {
        A = createMatrix<MatrixId::A>(config.M, config.K);
        B = createMatrix<MatrixId::B>(config.K, config.N);
        C = createMatrix<MatrixId::C>(config.M, config.N);
        E = createMatrix<MatrixId::C>(config.M, config.N);
        std::memset(C->mem, 0, sizeof(MT) * C->rows * C->cols);
        matmul(A, B, E);
    }

    MMGraph graph(service, config.M, config.N, config.K, config.tileSize, config.poolSize, config.threads);

    // hh::GraphSignalHandler<MMGraphIO>::registerGraph(&graph);
    // hh::GraphSignalHandler<MMGraphIO>::setDebugOptions(hh::DebugOptions::ALL);
    // hh::GraphSignalHandler<MMGraphIO>::handleSignal(SIGTERM);
    // hh::GraphSignalHandler<MMGraphIO>::handleSignal(SIGKILL);

    graph.executeGraph(true);

    timer_start(graph_execution);
    if (service->rank() == 0) {
        graph.pushData(A);
        graph.pushData(B);
        graph.pushData(C);
    }
    service->barrier();
    graph.finishPushingData();
    graph.waitForTermination();
    logh::info("graph terminated");
    timer_end(graph_execution);

    timer_start(create_dot_files);
    graph.createDotFile("build/graph" + std::to_string(service->rank()) + ".dot", hh::ColorScheme::EXECUTION,
                        hh::StructureOptions::QUEUE);
    service->barrier();
    timer_end(create_dot_files);

    service->barrier();
    delete service;

    if (GLOBAL_RANK != 0) {
        return 0;
    }

    if (config.M < 10 && config.N < 10 && config.K < 10) {
        printMatrix("A", A);
        printMatrix("B", B);
        printMatrix("C", C);
    }

    timer_report_prec(graph_execution, milliseconds);
    timer_report_prec(create_dot_files, milliseconds);
    std::cout << "test" << graph.mm->extraPrintingInformation("\n") << std::endl;

    utest_start();
    urun_test(mm_result, C, E);
    utest_end();

    return 0;
}
