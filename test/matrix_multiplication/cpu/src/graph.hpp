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
    MMGraph(hh::comm::CommHandle *commHandle, size_t M, size_t N, size_t K, size_t tileSize, size_t poolSize)
        : hh::Graph<MMGraphIO>("MMGraph") {
        size_t nbProcesses = commHandle->nbProcesses;
        size_t TM = M / tileSize + (M % tileSize > 0 ? 1 : 0);
        size_t TN = N / tileSize + (N % tileSize > 0 ? 1 : 0);
        size_t TK = K / tileSize + (K % tileSize > 0 ? 1 : 0);

        openblas_set_num_threads(1);

        mm = std::make_shared<MMType>();
        mm->template fill<MatrixTile<MT, MatrixId::A>>(TM * TK, tileSize);
        mm->template fill<MatrixTile<MT, MatrixId::B>>(TK * TN, tileSize);
        mm->template fill<MatrixTile<MT, MatrixId::C>>(TM * TM, tileSize);
        mm->template fill<MatrixTile<MT, MatrixId::P>>(poolSize, tileSize);

        std::vector<int> distributeTaskReceivers;
        for (size_t i = 1; i < nbProcesses; ++i) {
            distributeTaskReceivers.emplace_back(i);
        }

        auto splitTask = std::make_shared<SplitTask>(tileSize, mm, 20);
        auto distributeTask
            = std::make_shared<hh::CommunicatorTask<MatrixTile<MT, MatrixId::A>, MatrixTile<MT, MatrixId::B>,
                                                    MatrixTile<MT, MatrixId::C>>>(
                commHandle, distributeTaskReceivers,
                hh::CommunicatorTaskOpt{.sendersAreReceivers = true, .scatter = true});

        auto productState = std::make_shared<hh::StateManager<ProductStateIO>>(
            std::make_shared<ProductState>(mm, TM, TN, TK, commHandle->rank, commHandle->nbProcesses));
        auto productTask = std::make_shared<ProductTask>(40 / commHandle->nbProcesses);

        auto sumState = std::make_shared<SumStateManager>(
            std::make_shared<SumState>(mm, TM, TN, TK, commHandle->rank, nbProcesses));
        auto sumTask = std::make_shared<SumTask>(8 / commHandle->nbProcesses);

        auto gatherTask = std::make_shared<hh::CommunicatorTask<MatrixTile<MT, MatrixId::C>>>(
            commHandle, std::vector<int>({0}), hh::CommunicatorTaskOpt{.sendersAreReceivers = false, .scatter = false});

        auto copyTileState
            = std::make_shared<CopyTileStateManager>(std::make_shared<CopyTileState>(mm, TM, TN, commHandle->rank));
        auto copyTileTask = std::make_shared<CopyTileTask>(20);

        distributeTask->template destCB<MatrixTile<MT, MatrixId::A>>([nbProcesses, TN](auto tile) {
            std::vector<int> dests = {(int)(tile->rowIdx * TN % nbProcesses)};
            for (size_t colIdx = 1; colIdx < TN; ++colIdx) {
                int rank = (colIdx + tile->rowIdx * TN) % nbProcesses;
                if (rank == dests[0]) {
                    break;
                }
                dests.push_back(rank);
            }
            tile->sendCount = dests.size();
            logh::infog(logh::IG::DestDB, "DestCB", "A[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->template destCB<MatrixTile<MT, MatrixId::B>>([nbProcesses, TM, TN](auto tile) {
            std::vector<int> dests = {(int)(tile->colIdx % nbProcesses)};
            for (size_t rowIdx = 1; rowIdx < TM; ++rowIdx) {
                int rank = (tile->colIdx + rowIdx * TN) % nbProcesses;
                if (rank == dests[0]) {
                    break;
                }
                dests.push_back(rank);
            }
            tile->sendCount = dests.size();
            logh::infog(logh::IG::DestDB, "DestCB", "B[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            return dests;
        });
        distributeTask->template destCB<MatrixTile<MT, MatrixId::C>>([nbProcesses, TN](auto tile) {
            size_t           idx = tile->colIdx + tile->rowIdx * TN;
            std::vector<int> dests = {(int)(idx % nbProcesses)};
            logh::infog(logh::IG::DestDB, "DestCB", "C[", tile->rowIdx, ",", tile->colIdx, "] => ", dests);
            tile->sendCount = 1;
            return dests;
        });

        distributeTask->setMemoryManager(mm);
        gatherTask->setMemoryManager(mm);

        this->inputs(splitTask);
        this->inputs(copyTileState);
        this->edges(splitTask, distributeTask);
        this->edges(distributeTask, productState);
        this->edges(distributeTask, sumState);
        this->edges(productState, productTask);
        this->edges(productTask, sumState);
        this->edges(sumState, sumTask);
        this->edges(sumTask, sumState);
        this->edges(sumState, gatherTask);
        this->edges(gatherTask, copyTileState);
        this->edges(copyTileState, copyTileTask);
        this->edges(copyTileTask, copyTileState);
        this->outputs(copyTileState);
    }
};

#endif
