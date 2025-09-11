#ifndef COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#define COMMUNICATOR_COMMUNICATOR_MEMORY_MANAGER
#include "../log.hpp"
#include <condition_variable>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

namespace hh {

namespace tool {

template <typename T> struct MemoryPool {
  std::vector<std::shared_ptr<T>> memory;
  std::mutex mutex;
  std::condition_variable cv;

  std::shared_ptr<T> getMemory() {
    std::lock_guard<std::mutex> poolLock(mutex);

    if (memory.empty()) {
      return nullptr;
    }
    auto data = memory.back();
    memory.pop_back();
    return data;
  }

  std::shared_ptr<T> getMemoryOrWait() {
    std::unique_lock<std::mutex> poolLock(mutex);

    if (memory.empty()) {
      logh::warn("waiting for memory pool");
      cv.wait(poolLock, [&]() { return !memory.empty(); });
      logh::warn("end waiting");
    }
    auto data = memory.back();
    memory.pop_back();
    return data;
  }

  bool returnMemory(std::shared_ptr<T> &&data) {
    if (data.use_count() > 1) {
      // security so that we don't return any memory that is still referenced.
      // The `returnMemory` function requires to move the pointer, therefore
      // the count sould be 1 here.
      return false;
    }
    std::lock_guard<std::mutex> poolLock(mutex);
    memory.push_back(data);
    cv.notify_all();
    return true;
  }

  void preallocate(size_t size, auto &&...args) {
    std::lock_guard<std::mutex> poolLock(mutex);
    memory.resize(size, nullptr);
    for (auto &data : memory) {
      data = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }
};

template <typename... Types> struct CommunicatorMemoryManager {
  std::tuple<std::shared_ptr<MemoryPool<Types>>...> pools = {};

  template <typename T> std::shared_ptr<MemoryPool<T>> &pool() {
    return std::get<std::shared_ptr<MemoryPool<T>>>(pools);
  }

  template <typename T> void preallocate(size_t size, auto &&...args) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      pool = std::make_shared<MemoryPool<T>>();
    }
    pool->preallocate(size, std::forward<decltype(args)>(args)...);
  }

  template <typename T> std::shared_ptr<T> getMemory() {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      logh::error("memory pool emtpy.");
      return nullptr;
    }
    return pool->getMemory();
  }

  template <typename T> std::shared_ptr<T> getMemoryOrWait() {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      logh::error("memory pool emtpy.");
      return nullptr;
    }
    return pool->getMemoryOrWait();
  }

  template <typename T> bool returnMemory(std::shared_ptr<T> &&data) {
    auto &pool = this->template pool<T>();

    // FIXME: there is now way to know if the input data belongs to the mm, therefore we add it anyway for now
    if (pool == nullptr) {
      pool = std::make_shared<MemoryPool<T>>();
    }

    return pool->returnMemory(std::move(data));
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
