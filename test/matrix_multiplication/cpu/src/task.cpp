#include "task.hpp"
#include <cblas.h>

/******************************************************************************/
/*                                 split task                                 */
/******************************************************************************/

template <MatrixId Id>
void SplitTask::split(std::shared_ptr<Matrix<MT, Id>> matrix) {
    size_t nbTileRow = matrix->rows / tileSize + (matrix->rows % tileSize > 0 ? 1 : 0);
    size_t nbTileCol = matrix->cols / tileSize + (matrix->cols % tileSize > 0 ? 1 : 0);

    for (size_t row = 0; row < nbTileRow; ++row) {
        for (size_t col = 0; col < nbTileCol; ++col) {
            auto tile = mm->template getMemory<MatrixTile<MT, Id>>(hh::tool::MemoryPoolAllocMode::Wait);
            // auto tile = std::make_shared<MatrixTile<MT, Id>>(tileSize);

            tile->rows = std::min(tileSize, matrix->rows - row * tileSize);
            tile->cols = std::min(tileSize, matrix->cols - col * tileSize);
            tile->set(row, col, matrix->rows, matrix->cols);
            for (size_t trow = 0; trow < tile->rows; ++trow) {
                size_t mrow = tile->rowIdx * tileSize + trow;
                size_t mcol = tile->colIdx * tileSize;
                std::memcpy(&tile->mem[trow * tile->ld], &matrix->mem[mcol + mrow * matrix->ld],
                            sizeof(MT) * tile->cols);
            }
            this->addResult(tile);
        }
    }
}

#define EXECUTE(Id)                                                                                                    \
    void SplitTask::execute(std::shared_ptr<Matrix<MT, MatrixId::Id>> data) {                                          \
        logh::info("split matrix " #Id);                                                                               \
        split(data);                                                                                                   \
    }
EXECUTE(A)
EXECUTE(B)
EXECUTE(C)
#undef EXECUTE

/******************************************************************************/
/*                                product task                                */
/******************************************************************************/

void ProductTask::execute(std::shared_ptr<ProductData<MT>> data) {
    auto [a, b, p] = *data;

    // logh::infog(logh::IG::ProductTask, "product task", "P[", p->rowIdx, ",", p->colIdx, "] = A[", a->rowIdx, ",",
    //             a->colIdx, "] x B[", b->rowIdx, ",", b->colIdx, "]");

    if constexpr (std::is_same_v<MT, float>) {
        cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (blasint)a->rows, (blasint)b->cols, (blasint)a->cols,
                    1.f, (const float *)a->mem, (blasint)a->ld, (const float *)b->mem, (blasint)b->ld, 0,
                    (float *)p->mem, (blasint)p->ld);
    } else if constexpr (std::is_same_v<MT, double>) {
        cblas_dgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans, (blasint)a->rows, (blasint)b->cols, (blasint)a->cols,
                    1.f, (const double *)a->mem, (blasint)a->ld, (const double *)b->mem, (blasint)b->ld, 0,
                    (double *)p->mem, (blasint)p->ld);
    } else {
        logh::error("should not use the default implementation");
        for (size_t row = 0; row < p->rows; ++row) {
            for (size_t col = 0; col < p->cols; ++col) {
                p->mem[col + row * p->ld] = 0;

                for (size_t k = 0; k < a->cols; ++k) {
                    p->mem[col + row * p->ld] += a->mem[k + row * a->ld] * b->mem[col + k * b->ld];
                }
            }
        }
    }

    // for (size_t row = 0; row < p->rows; ++row) {
    //     for (size_t col = 0; col < p->cols; ++col) {
    //         p->mem[col + row * p->ld] = 0;
    //
    //         for (size_t k = 0; k < a->cols; ++k) {
    //             p->mem[col + row * p->ld] += a->mem[k + row * a->ld] * b->mem[col + k * b->ld];
    //         }
    //     }
    // }

    this->addResult(data);
}

/******************************************************************************/
/*                                  sum task                                  */
/******************************************************************************/

void SumTask::execute(std::shared_ptr<SumData<MT>> data) {
    auto c = data->c;
    auto p = data->p;

    logh::infog(logh::IG::SumTask, "sum task", "C[", c->rowIdx, ",", c->colIdx, "] + P[", p->rowIdx, ",", p->colIdx,
                "]");

    for (size_t row = 0; row < c->rows; ++row) {
        for (size_t col = 0; col < c->cols; ++col) {
            c->mem[col + row * c->ld] += p->mem[col + row * p->ld];
        }
    }
    this->addResult(data);
}

/******************************************************************************/
/*                               copy tile task                               */
/******************************************************************************/

void CopyTileTask::execute(std::shared_ptr<MatrixTilePair> pair) {
    auto   tile = pair->second;
    auto   matrix = pair->first;
    size_t tileSize = tile->tileSize;

    for (size_t trow = 0; trow < tile->rows; ++trow) {
        size_t mrow = tile->rowIdx * tileSize + trow;
        size_t mcol = tile->colIdx * tileSize;
        std::memcpy(&matrix->mem[mcol + mrow * matrix->ld], &tile->mem[trow * tile->ld], sizeof(MT) * tile->cols);
    }
    this->addResult(pair);
}
