#ifndef COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#define COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#include <memory>
#include <tuple>
#include <vector>
#include "../log.hpp"

namespace hh {

namespace tool {

template <typename... Types> struct CommunicatorMemoryManager {
  std::tuple<std::shared_ptr<std::vector<std::shared_ptr<Types>>>...> pools = {};

  template <typename T> std::shared_ptr<std::vector<std::shared_ptr<T>>> &pool() {
    return std::get<std::shared_ptr<std::vector<std::shared_ptr<T>>>>(pools);
  }

  template <typename T> void preallocate(size_t size, auto &&...args) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      pool = std::make_shared<std::vector<std::shared_ptr<T>>>();
    }

    pool->resize(size, nullptr);
    for (size_t i = 0; i < size; ++i) {
      pool->operator[](i) = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }

  template <typename T> std::shared_ptr<T> getMemory() {
    auto &pool = this->template pool<T>();

    if (pool == nullptr || pool->empty()) {
      logh::error("memory pool emtpy.");
      return nullptr;
    }
    auto ptr = pool->back();
    pool->pop_back();
    return ptr;
  }

  template <typename T> void returnMemory(std::shared_ptr<T> elt) {
    auto &pool = this->template pool<T>();

    // FIXME: there is now way to know if the input data belongs to the mm, therefore we add it anyway for now
    if (pool == nullptr) {
      pool = std::make_shared<std::vector<std::shared_ptr<T>>>();
    }

    pool->push_back(elt);
  }

  template <typename... SubsetTypes> std::shared_ptr<CommunicatorMemoryManager<SubsetTypes...>> convert() {
    auto mm = std::make_shared<CommunicatorMemoryManager<SubsetTypes...>>();
    ((mm->template pool<SubsetTypes>() = this->template pool<SubsetTypes>()), ...);
    return mm;
  }
};

} // end namespace tool

} // end namespace hh

#endif
