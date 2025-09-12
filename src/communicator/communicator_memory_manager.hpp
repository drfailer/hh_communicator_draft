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

template <typename T>
struct MemoryPool {
  std::vector<std::shared_ptr<T>> memory;
  std::mutex                      mutex;
  std::condition_variable         cv;
  size_t                          preallocatedSize;
  size_t                          nbGetMemory;
  size_t                          nbReturnMemory;

  std::shared_ptr<T> getMemory(bool wait = true) {
    std::unique_lock<std::mutex> poolLock(mutex);

    ++nbGetMemory;
    if (memory.empty()) {
      if (wait) {
        logh::warn("waiting for memory pool: ", std::string(typeid(memory.data()).name()));
        cv.wait(poolLock, [&]() { return !memory.empty(); });
        logh::warn("end waiting");
      } else {
        return nullptr;
      }
    }
    auto data = memory.back();
    memory.pop_back();
    return data;
  }

  bool returnMemory(std::shared_ptr<T> &&data) {
    if constexpr (requires { data->postProcess(); }) {
      data->postProcess();
    }
    if constexpr (requires { data->canBeRecycle(); }) {
      if (!data->canBeRecycle()) {
        return false;
      }
    }
    if constexpr (requires { data->cleanMemory(); }) {
      data->cleanMemory();
    }
    ++nbReturnMemory;
    std::lock_guard<std::mutex> poolLock(mutex);
    memory.push_back(data);
    cv.notify_all();
    return true;
  }

  void preallocate(size_t size, auto &&...args) {
    std::lock_guard<std::mutex> poolLock(mutex);
    preallocatedSize = size;
    memory.resize(size, nullptr);
    for (auto &data : memory) {
      data = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }

  std::string extraPrintingInformation() const {
    // FIXME: GCC bug -> cannot use declval to create the object and print the type
    return "MemoryPool[" + std::string(typeid(memory.data()).name()) + "]: { " + "preallocatedSize = "
           + std::to_string(preallocatedSize) + ", nbGetMemory = " + std::to_string(nbGetMemory) + ", nbReturnMemory = "
           + std::to_string(nbReturnMemory) + ", poolSize = " + std::to_string(memory.size()) + " }";
  }
};

template <typename... Types>
struct CommunicatorMemoryManager {
  std::tuple<std::shared_ptr<MemoryPool<Types>>...> pools = {};

  template <typename T>
  std::shared_ptr<MemoryPool<T>> &pool() {
    return std::get<std::shared_ptr<MemoryPool<T>>>(pools);
  }

  template <typename T>
  std::shared_ptr<MemoryPool<T>> pool() const {
    return std::get<std::shared_ptr<MemoryPool<T>>>(pools);
  }

  template <typename T>
  void preallocate(size_t size, auto &&...args) {
    auto &pool = this->template pool<T>();

    if (pool == nullptr) {
      pool = std::make_shared<MemoryPool<T>>();
    }
    pool->preallocate(size, std::forward<decltype(args)>(args)...);
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

    // FIXME: there is now way to know if the input data belongs to the mm, therefore we add it anyway for now
    if (pool == nullptr) {
      pool = std::make_shared<MemoryPool<T>>();
    }

    return pool->returnMemory(std::move(data));
  }

  template <typename... SubsetTypes>
  std::shared_ptr<CommunicatorMemoryManager<SubsetTypes...>> convert() {
    auto mm = std::make_shared<CommunicatorMemoryManager<SubsetTypes...>>();
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
