#ifndef GRAPH
#define GRAPH
#include "../.././src/communicator/communicator_task.hpp"
#include "/home/rvc1/Programming/usr/include/cblas.h"
#include "data.hpp"
#include "log.hpp"
#include "state.hpp"
#include "task.hpp"
#include <hedgehog/hedgehog.h>
#include <vector>

#define MMGraphIO 3, Matrix<MT, MatrixId::A>, Matrix<MT, MatrixId::B>, Matrix<MT, MatrixId::C>, Matrix<MT, MatrixId::C>

struct MMGraph : hh::Graph<MMGraphIO> {
    std::shared_ptr<MMType> mm = nullptr;
    /*
     * C[m,n] = A[m,k]*B[k,n] + C[m,n]
     */
    MMGraph(hh::comm::CommHandle *commHandle, size_t M, size_t N, size_t K, size_t tileSize, size_t poolSize,
            size_t threads)
        : hh::Graph<MMGraphIO>("MMGraph") {
        size_t NB_PROCESSES = commHandle->nbProcesses;
        size_t RANK = commHandle->rank;
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

        std::vector<int> distributeTaskReceivers;
        for (size_t i = 1; i < NB_PROCESSES; ++i) {
            distributeTaskReceivers.emplace_back(i);
        }

        auto splitTask = std::make_shared<SplitTask>(tileSize, mm, SPLIT_TASK_THREADS);
        auto distributeTask
            = std::make_shared<hh::CommunicatorTask<MatrixTile<MT, MatrixId::A>, MatrixTile<MT, MatrixId::B>,
                                                    MatrixTile<MT, MatrixId::C>>>(
                commHandle, distributeTaskReceivers,
                hh::CommunicatorTaskOpt{.sendersAreReceivers = true, .scatter = true});

        auto productState = std::make_shared<ProductState>(mm, TM, TN, TK, RANK, NB_PROCESSES);
        auto productStateManager = std::make_shared<hh::StateManager<ProductStateIO>>(productState);
        auto productTask = std::make_shared<ProductTask>(PRODUCT_TASK_THREADS);

        auto sumState = std::make_shared<SumState>(mm, TM, TN, TK, RANK, NB_PROCESSES);
        auto sumStateManager = std::make_shared<SumStateManager>(sumState);
        auto sumTask = std::make_shared<SumTask>(SUM_TASK_THREADS);

        auto gatherTask = std::make_shared<hh::CommunicatorTask<MatrixTile<MT, MatrixId::C>>>(
            commHandle, std::vector<int>({0}), hh::CommunicatorTaskOpt{.sendersAreReceivers = false, .scatter = false});

        auto copyTileState = std::make_shared<CopyTileStateManager>(std::make_shared<CopyTileState>(mm, TM, TN, RANK));
        auto copyTileTask = std::make_shared<CopyTileTask>(COPY_TILE_TASK_THREADS);

        distributeTask->template destCB<MatrixTile<MT, MatrixId::A>>([NB_PROCESSES, TN](auto tile) {
            std::vector<int> dests = {(int)(tile->rowIdx * TN % NB_PROCESSES)};
            for (size_t colIdx = 1; colIdx < TN; ++colIdx) {
                int rank = (colIdx + tile->rowIdx * TN) % NB_PROCESSES;
                if (rank == dests[0]) {
                    break;
                }
                dests.push_back(rank);
            }
            logh::infog(logh::IG::DestDB, "DestCB", "A[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->template destCB<MatrixTile<MT, MatrixId::B>>([NB_PROCESSES, TM, TN](auto tile) {
            std::vector<int> dests = {(int)(tile->colIdx % NB_PROCESSES)};
            for (size_t rowIdx = 1; rowIdx < TM; ++rowIdx) {
                int rank = (tile->colIdx + rowIdx * TN) % NB_PROCESSES;
                if (rank == dests[0]) {
                    break;
                }
                dests.push_back(rank);
            }
            logh::infog(logh::IG::DestDB, "DestCB", "B[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->template destCB<MatrixTile<MT, MatrixId::C>>([NB_PROCESSES, TN](auto tile) {
            size_t           idx = tile->colIdx + tile->rowIdx * TN;
            std::vector<int> dests = {(int)(idx % NB_PROCESSES)};
            logh::infog(logh::IG::DestDB, "DestCB", "C[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });

        distributeTask->setMemoryManager(mm);
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
