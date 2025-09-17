#ifndef COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#define COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#include "../log.hpp"
#include "hedgehog/src/tools/meta_functions.h"
#include <condition_variable>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

namespace hh {

namespace tool {

/*
 * Memory pool for one data type.
 *
 * The data type can implement the following methods:
 * - postProcess: called when returnMemory is called (even when the memory is not recycled)
 * - canBeRecycled: return true if the memory can be return to the pool.
 * - cleanMemory: called before the memory returns to the pool.
 */
template <typename T>
struct SingleTypeMemoryPool {
  std::vector<std::shared_ptr<T>> memory;
  std::mutex                      mutex;
  std::condition_variable         cv;
  size_t                          preallocatedSize;
  size_t                          nbGetMemory;
  size_t                          nbReturnMemory;

  std::shared_ptr<T> getMemory(bool wait = true) {
    std::unique_lock<std::mutex> poolLock(mutex);

    if (memory.empty()) {
      if (wait) {
        logh::warn("waiting for memory pool: ", std::string(typeToStr<T>()));
        cv.wait(poolLock, [&]() { return !memory.empty(); });
        logh::warn("end waiting for memory pool: ", std::string(typeToStr<T>()));
      } else {
        return nullptr;
      }
    }
    ++nbGetMemory;
    auto data = memory.back();
    memory.pop_back();
    return data;
  }

  bool returnMemory(std::shared_ptr<T> &&data) {
    // TODO: in debug mode, we should be able to detect when memory is returned multiple times
    if constexpr (requires { data->postProcess(); }) {
      data->postProcess();
    }
    if constexpr (requires { data->canBeRecycled(); }) {
      if (!data->canBeRecycled()) {
        return false;
      }
    }
    if constexpr (requires { data->cleanMemory(); }) {
      data->cleanMemory();
    }
    std::lock_guard<std::mutex> poolLock(mutex);
    ++nbReturnMemory;
    memory.push_back(data);
    cv.notify_all();
    return true;
  }

  void fill(size_t size, auto &&...args) {
    std::lock_guard<std::mutex> poolLock(mutex);
    preallocatedSize = size;
    memory.resize(size, nullptr);
    for (auto &data : memory) {
      data = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }

  std::string extraPrintingInformation() const {
    std::string typeStr = hh::tool::typeToStr<T>();
    return "MemoryPool[" + typeStr + "]: { " + "preallocatedSize = " + std::to_string(preallocatedSize)
           + ", nbGetMemory = " + std::to_string(nbGetMemory) + ", nbReturnMemory = " + std::to_string(nbReturnMemory)
           + ", poolSize = " + std::to_string(memory.size()) + " }";
  }
};

template <typename... Types>
struct MemoryPool {
  std::tuple<std::shared_ptr<SingleTypeMemoryPool<Types>>...> pools = {};

  template <typename T>
  std::shared_ptr<SingleTypeMemoryPool<T>> &pool() {
    return std::get<std::shared_ptr<SingleTypeMemoryPool<T>>>(pools);
  }

  template <typename T>
  std::shared_ptr<SingleTypeMemoryPool<T>> pool() const {
    return std::get<std::shared_ptr<SingleTypeMemoryPool<T>>>(pools);
  }

  template <typename T>
  void fill(size_t size, auto &&...args) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      pool = std::make_shared<SingleTypeMemoryPool<T>>();
    }
    pool->fill(size, std::forward<decltype(args)>(args)...);
  }

  template <typename T>
  std::shared_ptr<T> getMemory(bool wait = true) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      logh::error("memory pool emtpy.");
      return nullptr;
    }
    return pool->getMemory(wait);
  }

  template <typename T>
  bool returnMemory(std::shared_ptr<T> &&data) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      pool = std::make_shared<SingleTypeMemoryPool<T>>();
    }

    return pool->returnMemory(std::move(data));
  }

  template <typename... SubsetTypes>
  std::shared_ptr<MemoryPool<SubsetTypes...>> convert() {
    auto mm = std::make_shared<MemoryPool<SubsetTypes...>>();
    ((mm->template pool<SubsetTypes>() = this->template pool<SubsetTypes>()), ...);
    return mm;
  }

  std::string extraPrintingInformation() const {
    std::string infos = "MemoryManager: {\\l";
    (
        [&] {
          auto pool = this->template pool<Types>();
          if (pool) {
            infos.append("    " + pool->extraPrintingInformation() + "\\l");
          }
        }(),
        ...);
    return infos + "}\\l";
  }
};

} // end namespace tool

} // end namespace hh

#endif
