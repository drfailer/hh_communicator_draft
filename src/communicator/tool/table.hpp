#ifndef COMMUNICATOR_TOOL_TABLE
#define COMMUNICATOR_TOOL_TABLE
#include <vector>
#include <cassert>
#include <unistd.h>

template <typename T>
class Table {
 public:
  explicit Table() = default;

  Table(size_t rows, size_t cols)
      : rows_(rows), cols_(cols), data_(rows * cols) {}

  T const &operator()(size_t row, size_t col) const {
      assert(row < rows_ && col < cols_);
      return data_[row * cols_ + col];
  }
  T &operator()(size_t row, size_t col) {
      assert(row < rows_ && col < cols_);
      return data_[row * cols_ + col];
  }

  T *operator[](size_t row) { return &data_[row * cols_]; }

  size_t rows() const { return rows_; }
  size_t cols() const { return cols_; }

 private:
  size_t rows_, cols_;
  std::vector<T> data_;
};

#endif
