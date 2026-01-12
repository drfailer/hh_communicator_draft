#ifndef GRAPH_HPP
#define GRAPH_HPP
#include "data.hpp"
#include "log.hpp"
#include "state.hpp"
#include "task.hpp"
#include <cblas.h>
#include <communicator/communicator_task.hpp>
#include <communicator/send_strategies.hpp>
#include <hedgehog/hedgehog.h>
#include <vector>

#define MMGraphIO 3, Matrix<MT, MatrixId::A>, Matrix<MT, MatrixId::B>, Matrix<MT, MatrixId::C>, Matrix<MT, MatrixId::C>

struct MMGraph : hh::Graph<MMGraphIO> {
    std::shared_ptr<MMType> mm = nullptr;
    /*
     * C[m,n] = A[m,k]*B[k,n] + C[m,n]
     */
    MMGraph(hh::comm::CommService *service, size_t M, size_t N, size_t K, size_t tileSize, size_t poolSize,
            size_t threads)
        : hh::Graph<MMGraphIO>("MMGraph") {
        size_t NB_PROCESSES = service->nbProcesses();
        size_t RANK = service->rank();
        size_t SPLIT_TASK_THREADS = std::max<size_t>(1, threads / 2);
        size_t PRODUCT_TASK_THREADS = std::max<size_t>(1, threads / NB_PROCESSES);
        size_t SUM_TASK_THREADS = std::max<size_t>(1, threads / (4 * NB_PROCESSES));
        size_t COPY_TILE_TASK_THREADS = std::max<size_t>(1, threads / 2);
        size_t TM = M / tileSize + (M % tileSize > 0 ? 1 : 0);
        size_t TN = N / tileSize + (N % tileSize > 0 ? 1 : 0);
        size_t TK = K / tileSize + (K % tileSize > 0 ? 1 : 0);

        openblas_set_num_threads(1);

        mm = std::make_shared<MMType>();
        mm->template fill<MatrixTile<MT, MatrixId::A>>(TM * TK, tileSize);
        mm->template fill<MatrixTile<MT, MatrixId::B>>(TK * TN, tileSize);
        mm->template fill<MatrixTile<MT, MatrixId::C>>(TM * TN, tileSize);
        mm->template fill<MatrixTile<MT, MatrixId::P>>(poolSize, tileSize);

        auto splitTask = std::make_shared<SplitTask>(tileSize, mm, SPLIT_TASK_THREADS);
        auto distributeTask
            = std::make_shared<hh::CommunicatorTask<MatrixTile<MT, MatrixId::A>, MatrixTile<MT, MatrixId::B>,
                                                    MatrixTile<MT, MatrixId::C>>>(service);

        auto productState = std::make_shared<ProductState>(mm, TM, TN, TK, RANK, NB_PROCESSES);
        auto productStateManager = std::make_shared<hh::StateManager<ProductStateIO>>(productState);
        auto productTask = std::make_shared<ProductTask>(PRODUCT_TASK_THREADS);

        auto sumState = std::make_shared<SumState>(mm, TM, TN, TK, RANK, NB_PROCESSES);
        auto sumStateManager = std::make_shared<SumStateManager>(sumState);
        auto sumTask = std::make_shared<SumTask>(SUM_TASK_THREADS);

        auto gatherTask = std::make_shared<hh::CommunicatorTask<MatrixTile<MT, MatrixId::C>>>(service);

        auto copyTileState = std::make_shared<CopyTileStateManager>(std::make_shared<CopyTileState>(mm, TM, TN, RANK));
        auto copyTileTask = std::make_shared<CopyTileTask>(COPY_TILE_TASK_THREADS);

        distributeTask->template strategy<MatrixTile<MT, MatrixId::A>>([NB_PROCESSES, TN](auto tile) {
            std::vector<hh::comm::rank_t> dests = {(hh::comm::rank_t)(tile->rowIdx * TN % NB_PROCESSES)};
            for (size_t colIdx = 1; colIdx < TN; ++colIdx) {
                hh::comm::rank_t rank = (colIdx + tile->rowIdx * TN) % NB_PROCESSES;
                if (rank == dests[0]) {
                    break;
                }
                dests.push_back(rank);
            }
            logh::infog(logh::IG::DestDB, "DestCB", "A[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->template strategy<MatrixTile<MT, MatrixId::B>>([NB_PROCESSES, TM, TN](auto tile) {
            std::vector<hh::comm::rank_t> dests = {(hh::comm::rank_t)(tile->colIdx % NB_PROCESSES)};
            for (size_t rowIdx = 1; rowIdx < TM; ++rowIdx) {
                hh::comm::rank_t rank = (tile->colIdx + rowIdx * TN) % NB_PROCESSES;
                if (rank == dests[0]) {
                    break;
                }
                dests.push_back(rank);
            }
            logh::infog(logh::IG::DestDB, "DestCB", "B[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->template strategy<MatrixTile<MT, MatrixId::C>>([NB_PROCESSES, TN](auto tile) {
            size_t                        idx = tile->colIdx + tile->rowIdx * TN;
            std::vector<hh::comm::rank_t> dests = {(hh::comm::rank_t)(idx % NB_PROCESSES)};
            logh::infog(logh::IG::DestDB, "DestCB", "C[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->setMemoryManager(mm);

        gatherTask->template strategy<MatrixTile<MT, MatrixId::C>>(
            hh::comm::strategy::Gather<MatrixTile<MT, MatrixId::C>>(0));
        gatherTask->setMemoryManager(mm);

        this->inputs(splitTask);
        this->inputs(copyTileState);
        this->edges(splitTask, distributeTask);
        this->edges(distributeTask, productStateManager);
        this->edges(distributeTask, sumStateManager);
        this->edges(productStateManager, productTask);
        this->edges(productTask, sumStateManager);
        this->edges(sumStateManager, sumTask);
        this->edges(sumTask, sumStateManager);
        this->edges(sumStateManager, gatherTask);
        this->edges(gatherTask, copyTileState);
        this->edges(copyTileState, copyTileTask);
        this->edges(copyTileTask, copyTileState);
        this->outputs(copyTileState);
    }
};

#endif
