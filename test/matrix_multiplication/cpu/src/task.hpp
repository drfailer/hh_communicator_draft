#ifndef TASK
#define TASK
#include "data.hpp"
#include "log.hpp"
#include <hedgehog/hedgehog.h>
#include <memory>

/******************************************************************************/
/*                                 split task                                 */
/******************************************************************************/

#define SplitTaskIn Matrix<MT, MatrixId::A>, Matrix<MT, MatrixId::B>, Matrix<MT, MatrixId::C>
#define SplitTaskOut MatrixTile<MT, MatrixId::A>, MatrixTile<MT, MatrixId::B>, MatrixTile<MT, MatrixId::C>
#define SplitTaskIO 3, SplitTaskIn, SplitTaskOut

struct SplitTask : hh::AbstractTask<SplitTaskIO> {
    std::shared_ptr<MMType> mm;
    size_t                  tileSize;

    SplitTask(size_t tileSize, std::shared_ptr<MMType> mm, size_t nbThreads)
        : hh::AbstractTask<SplitTaskIO>("SplitTask", nbThreads),
          mm(mm),
          tileSize(tileSize) {}

    template <MatrixId Id>
    void split(std::shared_ptr<Matrix<MT, Id>> matrix);

#define EXECUTE(Id) void execute(std::shared_ptr<Matrix<MT, MatrixId::Id>> data) override;
    EXECUTE(A)
    EXECUTE(B)
    EXECUTE(C)
#undef EXECUTE

    std::shared_ptr<hh::AbstractTask<SplitTaskIO>> copy() override {
        return std::make_shared<SplitTask>(tileSize, mm, this->numberThreads());
    }
};

/******************************************************************************/
/*                                product task                                */
/******************************************************************************/

#define ProductTaskIO 1, ProductData<MT>, ProductData<MT>

struct ProductTask : hh::AbstractTask<ProductTaskIO> {
    ProductTask(size_t nbThreads)
        : hh::AbstractTask<ProductTaskIO>("ProductTask", nbThreads) {}

    void execute(std::shared_ptr<ProductData<MT>> data) override;

    std::shared_ptr<hh::AbstractTask<ProductTaskIO>> copy() override {
        return std::make_shared<ProductTask>(this->numberThreads());
    }
};

/******************************************************************************/
/*                                  sum task                                  */
/******************************************************************************/

#define SumTaskIO 1, SumData<MT>, SumData<MT>

struct SumTask : hh::AbstractTask<SumTaskIO> {
    SumTask(size_t nbThreads)
        : hh::AbstractTask<SumTaskIO>("SumTask", nbThreads) {}

    void execute(std::shared_ptr<SumData<MT>> data) override;

    std::shared_ptr<hh::AbstractTask<SumTaskIO>> copy() override {
        return std::make_shared<SumTask>(this->numberThreads());
    }
};

/******************************************************************************/
/*                               copy tile task                               */
/******************************************************************************/

#define CopyTileTaskIn MatrixTilePair
#define CopyTileTaskOut MatrixTilePair
#define CopyTileTaskIO 1, CopyTileTaskIn, CopyTileTaskOut

struct CopyTileTask : hh::AbstractTask<CopyTileTaskIO> {
    std::shared_ptr<MMType> tileMM = nullptr;

    CopyTileTask(size_t numberThreads)
        : hh::AbstractTask<CopyTileTaskIO>("CopyTileTask", numberThreads) {}

    void execute(std::shared_ptr<MatrixTilePair> pair) override;

    std::shared_ptr<hh::AbstractTask<CopyTileTaskIO>> copy() override {
        return std::make_shared<CopyTileTask>(this->numberThreads());
    }
};

#endif
