#ifndef COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#define COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#include <memory>
#include <tuple>
#include <vector>

namespace hh {

namespace tool {

template <typename... Types> class CommunicatorMemoryManager {
public:
  template <typename T> using pool_type_t = std::vector<std::shared_ptr<T>>;

public:
  template <typename T> void preallocate(size_t size, auto &&...args) {
    auto &pool = std::get<pool_type_t<T>>(pools_);
    pool.resize(size);

    for (auto &elt : pool) {
      elt = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }

  template <typename T> std::shared_ptr<T> getMemory() {
    auto &pool = std::get<pool_type_t<T>>(pools_);
    std::shared_ptr<T> elt = nullptr;

    if (!pool.empty()) {
        elt = pool.back();
        pool.pop_back();
    }
    return elt;
  }

  template <typename T>
  void returnMemory(std::shared_ptr<T> &&elt) {
    auto &pool = std::get<pool_type_t<T>>(pools_);
    pool.emplace_back(std::move(elt));
  }

private:
  std::tuple<pool_type_t<Types>...> pools_;
};

} // end namespace tool

} // end namespace hh

#endif
