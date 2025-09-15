#ifndef STATE
#define STATE
#include "data.hpp"
#include "log.hpp"
#include <hedgehog/hedgehog.h>
#include <map>

/******************************************************************************/
/*                               product state                                */
/******************************************************************************/

#define ProductStateIn MatrixTile<MT, MatrixId::A>, MatrixTile<MT, MatrixId::B>
#define ProductStateOut ProductData<MT>
#define ProductStateIO 2, ProductStateIn, ProductStateOut

template <MatrixId Id>
struct TileMap {
    size_t                                           rows = 0;
    size_t                                           cols = 0;
    std::vector<std::shared_ptr<MatrixTile<MT, Id>>> tiles = {};
    std::vector<size_t>                              counts = {};

    TileMap(size_t rows, size_t cols)
        : rows(rows),
          cols(cols),
          tiles(rows * cols, nullptr),
          counts(rows * cols, 0) {}

    std::shared_ptr<MatrixTile<MT, Id>> &tile(size_t row, size_t col) {
        return tiles[col + row * cols];
    }

    size_t &count(size_t row, size_t col) {
        return counts[col + row * cols];
    }

    bool empty() {
        return tiles.empty();
    }
};

struct ProductState : hh::AbstractState<ProductStateIO> {
    std::shared_ptr<MMType> mm;
    TileMap<MatrixId::A>    as;
    TileMap<MatrixId::B>    bs;

    ProductState(std::shared_ptr<MMType> mm, size_t M, size_t N, size_t K, int rank, int nbProcesses);

    std::shared_ptr<MatrixTile<MT, MatrixId::P>> getPAndUpdateCount(auto a, auto b);
    void                                         execute(std::shared_ptr<MatrixTile<MT, MatrixId::A>> a) override;
    void                                         execute(std::shared_ptr<MatrixTile<MT, MatrixId::B>> b) override;
};

/******************************************************************************/
/*                                 sum state                                  */
/******************************************************************************/

#define SumStateIn MatrixTile<MT, MatrixId::C>, ProductData<MT>, SumData<MT>
#define SumStateOut SumData<MT>, MatrixTile<MT, MatrixId::C>
#define SumStateIO 3, SumStateIn, SumStateOut

struct SumQueue {
    std::shared_ptr<MatrixTile<MT, MatrixId::C>>              c = nullptr;
    std::vector<std::shared_ptr<MatrixTile<MT, MatrixId::P>>> queue = {};

    std::shared_ptr<MatrixTile<MT, MatrixId::P>> pop() {
        if (queue.empty()) {
            return nullptr;
        }
        auto result = queue.back();
        queue.pop_back();
        return result;
    }

    void push(std::shared_ptr<MatrixTile<MT, MatrixId::P>> p) {
        queue.push_back(p);
    }

    bool empty() {
        return queue.empty();
    }
};

struct SumState : hh::AbstractState<SumStateIO> {
    std::map<std::pair<size_t, size_t>, SumQueue> queues;
    std::shared_ptr<MMType>                       mm;
    size_t                                        processCount = 0;

    SumState(std::shared_ptr<MMType> mm, size_t M, size_t N, size_t K, int rank, size_t nbProcesses);

    void execute(std::shared_ptr<MatrixTile<MT, MatrixId::C>> c) override;
    void execute(std::shared_ptr<ProductData<MT>> data) override;
    void execute(std::shared_ptr<SumData<MT>> data) override;

    bool done() const {
        return queues.empty();
    }
};

class SumStateManager : public hh::StateManager<SumStateIO> {
  public:
    SumStateManager(std::shared_ptr<SumState> state)
        : hh::StateManager<SumStateIO>(state, "SumStateManager") {}

    [[nodiscard]] bool canTerminate() const override {
        this->state()->lock();
        auto result = std::dynamic_pointer_cast<SumState>(this->state())->done();
        this->state()->unlock();
        return result;
    }
};

/******************************************************************************/
/*                              copy tile state                               */
/******************************************************************************/

#define CopyTileStateIn Matrix<MT, MatrixId::C>, MatrixTile<MT, MatrixId::C>, MatrixTilePair
#define CopyTileStateOut MatrixTilePair, Matrix<MT, MatrixId::C>
#define CopyTileStateIO 3, CopyTileStateIn, CopyTileStateOut

struct CopyTileState : hh::AbstractState<CopyTileStateIO> {
    std::shared_ptr<Matrix<MT, MatrixId::C>>                  c;
    std::vector<std::shared_ptr<MatrixTile<MT, MatrixId::C>>> tiles;
    std::shared_ptr<MMType>                                   mm;
    size_t                                                    nbCopies = 0;
    int                                                       rank = -1;

    CopyTileState(std::shared_ptr<MMType> mm, size_t M, size_t N, int rank);

    void execute(std::shared_ptr<Matrix<MT, MatrixId::C>> c) override;
    void execute(std::shared_ptr<MatrixTile<MT, MatrixId::C>> tile) override;
    void execute(std::shared_ptr<MatrixTilePair> data) override;

    bool done() const {
        if (rank == 0) {
            return nbCopies == 0;
        }
        return true;
    }
};

class CopyTileStateManager : public hh::StateManager<CopyTileStateIO> {
  public:
    CopyTileStateManager(std::shared_ptr<CopyTileState> state)
        : hh::StateManager<CopyTileStateIO>(state, "CopyTileStateManager") {}

    [[nodiscard]] bool canTerminate() const override {
        this->state()->lock();
        auto result = std::dynamic_pointer_cast<CopyTileState>(this->state())->done();
        this->state()->unlock();
        return result;
    }
};

#endif
