#ifndef COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#define COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#include "../log.hpp"
#include "hedgehog/src/tools/meta_functions.h"
#include <cassert>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <source_location>
#include <tuple>
#include <vector>

namespace hh {

namespace tool {

enum class MemoryPoolAllocMode {
  Wait, // wait for memory if the pool is empty
  Dynamic, // increase the size of the pool if it's empty
  Fail, // return nullptr directly if the pool is empty
};

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
  struct UsedMemory {
    std::source_location loc;
  };
  std::vector<std::shared_ptr<T>>          memory;
  std::map<std::shared_ptr<T>, UsedMemory> usedMemory;
  std::mutex                               mutex;
  std::condition_variable                  cv;
  size_t                                   preallocatedSize;
  size_t                                   nbGetMemory;
  size_t                                   nbReturnMemory;

  ~SingleTypeMemoryPool() {
    if (usedMemory.size() > 0) {
      logh::error(usedMemory.size(), " elements of type `", std::string(typeToStr<T>()),
                  "' were not returned to the pool.");
      for (auto um : usedMemory) {
        auto loc = um.second.loc;
        logh::error("Memory allocated at (", loc.file_name(), ":", loc.line(),
                    ") was not returned to the pool (type = `", std::string(typeToStr<T>()), "').");
      }
    }
  }

  std::shared_ptr<T> getMemory(MemoryPoolAllocMode allocMode, std::source_location loc) {
    std::unique_lock<std::mutex> poolLock(mutex);

    if (memory.empty()) {
      switch (allocMode) {
      case MemoryPoolAllocMode::Wait:
        logh::warn("waiting for memory pool: ", std::string(typeToStr<T>()));
        cv.wait(poolLock, [&]() { return !memory.empty(); });
        logh::warn("end waiting for memory pool: ", std::string(typeToStr<T>()));
        break;
      case MemoryPoolAllocMode::Dynamic:
        if constexpr (std::is_default_constructible_v<T>) {
          memory.push_back(std::make_shared<T>());
        } else {
          logh::error("dynamically sized pool only support default constructible types (getMemory<",
                      std::string(typeToStr<T>()), ">(Dynamic) failed), defaulting to wait mode.");
          return getMemory(MemoryPoolAllocMode::Wait, loc);
        }
        break;
      case MemoryPoolAllocMode::Fail:
        return nullptr;
        break;
      }
    }
    ++nbGetMemory;
    auto data = memory.back();
    memory.pop_back();
    assert(!usedMemory.contains(data) && "allocating the same memory multiple times.");
    usedMemory.insert({data, UsedMemory{loc}});
    return data;
  }

  bool returnMemory(std::shared_ptr<T> &&data, std::source_location loc) {
    std::lock_guard<std::mutex> poolLock(mutex);
    if (!usedMemory.contains(data)) {
      if (std::find(memory.begin(), memory.end(), data) != memory.end()) {
        logh::error("data of type '", std::string(typeToStr<T>()),
                    "` was returned to memory pool multiple time. The memory was returned at (", loc.file_name(), ":",
                    loc.line(), ").");
        return false;
      }
    }
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
    ++nbReturnMemory;
    memory.push_back(data);
    cv.notify_all();
    usedMemory.erase(data);
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
  std::shared_ptr<T> getMemory(MemoryPoolAllocMode  allocMode = MemoryPoolAllocMode::Fail,
                               std::source_location loc = std::source_location::current()) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      logh::error("memory pool emtpy.");
      return nullptr;
    }
    return pool->getMemory(allocMode, loc);
  }

  template <typename T>
  bool returnMemory(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      pool = std::make_shared<SingleTypeMemoryPool<T>>();
    }

    return pool->returnMemory(std::move(data), loc);
  }

  template <typename... SubsetTypes>
  std::shared_ptr<MemoryPool<SubsetTypes...>> convert() {
    auto mm = std::make_shared<MemoryPool<SubsetTypes...>>();
    ((mm->template pool<SubsetTypes>() = this->template pool<SubsetTypes>()), ...);
    return mm;
  }

  std::string extraPrintingInformation(std::string const &eol = "\\l") const {
    std::string infos = "MemoryManager: {" + eol;
    (
        [&] {
          auto pool = this->template pool<Types>();
          if (pool) {
            infos.append("    " + pool->extraPrintingInformation() + eol);
          }
        }(),
        ...);
    return infos + "}" + eol;
  }
};

} // end namespace tool

} // end namespace hh

#endif
