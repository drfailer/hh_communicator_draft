#include "state.hpp"

std::string MPIRank() {
    int rank = -1;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    return "(" + std::to_string(rank) + ")";
}

/******************************************************************************/
/*                               product state                                */
/******************************************************************************/

ProductState::ProductState(std::shared_ptr<MMType> mm, size_t M, size_t N, size_t K, int rank, int nbProcesses)
    : hh::AbstractState<ProductStateIO>(),
      mm(mm),
      as(M, K),
      bs(K, N),
      rank(rank),
      nbProcesses(nbProcesses) {
    for (size_t row = 0; row < M; ++row) {
        for (size_t col = 0; col < N; ++col) {
            int tileRank = (int)((col + row * N) % nbProcesses);
            if (tileRank == rank) {
                for (size_t acol = 0; acol < as.cols; ++acol) {
                    ++as.count(row, acol);
                }
                for (size_t brow = 0; brow < bs.rows; ++brow) {
                    ++bs.count(brow, col);
                }
            }
        }
    }
}

std::shared_ptr<MatrixTile<MT, MatrixId::P>> ProductState::getPAndUpdateCount(auto a, auto b) {
    auto p = mm->template getMemory<MatrixTile<MT, MatrixId::P>>();
    p->rows = a->rows;
    p->cols = b->cols;
    p->processCount = 1;
    p->set(a->rowIdx, b->colIdx, a->matrixRows, b->matrixCols);

    logh::infog(logh::IG::ProductState, "product state", MPIRank(), " P[", p->rowIdx, ",", p->colIdx, "] = A[",
                a->rowIdx, ",", a->colIdx, "] x B[", b->rowIdx, ",", b->colIdx, "]");

    if (--as.count(a->rowIdx, a->colIdx) == 0) {
        as.tile(a->rowIdx, a->colIdx) = nullptr;
    }
    if (--bs.count(b->rowIdx, b->colIdx) == 0) {
        bs.tile(b->rowIdx, b->colIdx) = nullptr;
    }
    return p;
}

bool ProductState::rankShouldMakeProduct(auto a, auto b) {
    return (int)((b->colIdx + a->rowIdx * bs.cols) % nbProcesses) == rank;
}

void ProductState::execute(std::shared_ptr<MatrixTile<MT, MatrixId::A>> a) {
    logh::infog(logh::IG::ProductState, "product state", MPIRank(), " A[", a->rowIdx, ",", a->colIdx,
                "] tile received");

    assert(as.tile(a->rowIdx, a->colIdx) == nullptr);
    a->processCount = as.count(a->rowIdx, a->colIdx);
    as.tile(a->rowIdx, a->colIdx) = a;

    for (size_t col = 0; col < bs.cols; ++col) {
        auto b = bs.tile(a->colIdx, col);

        if (b != nullptr && rankShouldMakeProduct(a, b)) {
            assert(a->canBeRecycle() == false);
            assert(b->canBeRecycle() == false);
            assert(b->rowIdx == a->colIdx);
            assert(b->colIdx == col);
            auto p = getPAndUpdateCount(a, b);
            assert(a->colIdx == b->rowIdx);
            this->addResult(std::make_shared<ProductData<MT>>(a, b, p));
        }
    }
}

void ProductState::execute(std::shared_ptr<MatrixTile<MT, MatrixId::B>> b) {
    logh::infog(logh::IG::ProductState, "product state", MPIRank(), " B[", b->rowIdx, ",", b->colIdx,
                "] tile received");

    assert(bs.tile(b->rowIdx, b->colIdx) == nullptr);
    b->processCount = bs.count(b->rowIdx, b->colIdx);
    bs.tile(b->rowIdx, b->colIdx) = b;

    for (size_t row = 0; row < as.rows; ++row) {
        auto a = as.tile(row, b->rowIdx);

        if (a != nullptr && rankShouldMakeProduct(a, b)) {
            assert(a->canBeRecycle() == false);
            assert(b->canBeRecycle() == false);
            assert(a->rowIdx == row);
            assert(a->colIdx == b->rowIdx);
            auto p = getPAndUpdateCount(a, b);
            assert(a->colIdx == b->rowIdx);
            this->addResult(std::make_shared<ProductData<MT>>(a, b, p));
        }
    }
}

/******************************************************************************/
/*                                 sum state                                  */
/******************************************************************************/

SumState::SumState(std::shared_ptr<MMType> mm, size_t M, size_t N, size_t K, int rank, size_t nbProcesses)
    : hh::AbstractState<SumStateIO>(),
      mm(mm),
      processCount(K) {
    for (size_t row = 0; row < M; ++row) {
        for (size_t col = 0; col < N; ++col) {
            int tileRank = (int)((col + row * N) % nbProcesses);
            if (tileRank == rank) {
                auto key = std::make_pair(row, col);
                queues.insert({key, SumQueue{nullptr, {}}});
            }
        }
    }
}

void SumState::execute(std::shared_ptr<MatrixTile<MT, MatrixId::C>> c) {
    logh::infog(logh::IG::SumState, "sum state", MPIRank(), " C[", c->rowIdx, ",", c->colIdx, "] tile received");

    c->processCount = processCount;
    auto key = std::make_pair(c->rowIdx, c->colIdx);
    if (!queues.contains(key)) {
        logh::error("(c tile) queue does not contain key = ", key);
        return;
    }

    auto &queue = queues.at(key);

    if (queue.empty()) {
        queue.c = c;
        return;
    }
    auto p = queue.pop();
    this->addResult(std::make_shared<SumData<MT>>(p, c));
}

void SumState::execute(std::shared_ptr<ProductData<MT>> data) {
    auto [a, b, p] = *data;
    auto key = std::make_pair(p->rowIdx, p->colIdx);

    logh::infog(logh::IG::SumState, "sum state", MPIRank(), " product data received, P[", p->rowIdx, ",", p->colIdx,
                "]");

    if (!queues.contains(key)) {
        logh::error("(product data) queue does not contain key = ", key);
        return;
    }

    auto &queue = queues.at(key);

    if (queue.c) {
        this->addResult(std::make_shared<SumData<MT>>(p, queue.c));
        queue.c = nullptr;
    } else {
        queue.push(data->p);
    }

    // FIXME: returning memory here causes problems
    // --a->processCount;
    // mm->returnMemory(std::move(a));
    // --b->processCount;
    // mm->returnMemory(std::move(b));
}

void SumState::execute(std::shared_ptr<SumData<MT>> data) {
    logh::infog(logh::IG::SumState, "sum state", MPIRank(), " sum data received, C[", data->c->rowIdx, ",",
                data->c->colIdx, "]");

    auto key = std::make_pair(data->c->rowIdx, data->c->colIdx);
    if (!queues.contains(key)) {
        logh::error("(sum data) queue does not contain key = ", key);
        return;
    }

    auto &queue = queues.at(key);

    --data->p->processCount;
    mm->returnMemory(std::move(data->p));

    if (--data->c->processCount == 0) {
        assert(queue.empty());
        queues.erase(key);
        this->addResult(data->c);
        if (queues.empty()) {
            logh::warn(MPIRank(), " terminate sum state");
        }
    }

    if (queue.empty()) {
        queue.c = data->c;
        return;
    }

    auto p = queue.pop();
    this->addResult(std::make_shared<SumData<MT>>(p, data->c));
}

/******************************************************************************/
/*                              copy tile state                               */
/******************************************************************************/

CopyTileState::CopyTileState(std::shared_ptr<MMType> mm, size_t M, size_t N, int rank)
    : hh::AbstractState<CopyTileStateIO>(),
      mm(mm),
      nbCopies(M * N),
      rank(rank) {}

void CopyTileState::execute(std::shared_ptr<Matrix<MT, MatrixId::C>> c) {
    logh::infog(logh::IG::CopyTileState, "copy tile state", "C received");

    this->c = c;
    if (!tiles.empty()) [[unlikely]] {
        for (auto tile : tiles) {
            this->addResult(std::make_shared<MatrixTilePair>(this->c, tile));
        }
        tiles.clear();
    }
}

void CopyTileState::execute(std::shared_ptr<MatrixTile<MT, MatrixId::C>> tile) {
    logh::infog(logh::IG::CopyTileState, "copy tile state", "tile received");

    tile->processCount = 1;
    if (!this->c) {
        this->tiles.push_back(tile);
    } else {
        this->addResult(std::make_shared<MatrixTilePair>(this->c, tile));
    }
}

void CopyTileState::execute(std::shared_ptr<MatrixTilePair> data) {
    --data->second->processCount;
    mm->returnMemory(std::move(data->second));
    if (--nbCopies == 0) {
        logh::warn("copy tile state result");
        this->addResult(c);
    }
}
