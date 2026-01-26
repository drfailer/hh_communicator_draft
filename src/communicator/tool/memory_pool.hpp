#ifndef COMMUNICATOR_TOOL_MEMORY_POOL
#define COMMUNICATOR_TOOL_MEMORY_POOL
#include "../../log.hpp"
#include "../stats.hpp"
#include "hedgehog/src/tools/meta_functions.h"
#include "memory_manager.hpp"
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <source_location>
#include <tuple>
#include <vector>

namespace hh {
namespace comm {
namespace tool {

template <typename T>
class SingleTypeMemoryPool : public SingleTypeMemoryManager<T> {
public:
  virtual ~SingleTypeMemoryPool() {
    if (usedMemory.size() > 0) {
      logh::error(usedMemory.size(), " elements of type `", std::string(hh::tool::typeToStr<T>()),
                  "' were not returned to the pool.");
      for (auto um : usedMemory) {
        auto loc = um.second.loc;
        logh::error("Memory allocated at (", loc.file_name(), ":", loc.line(),
                    ") was not returned to the pool (type = `", std::string(hh::tool::typeToStr<T>()), "').");
      }
    }
  }

  std::shared_ptr<T> allocate(MemoryManagerAllocatedMode mode = MemoryManagerAllocatedMode::Fail,
                              std::source_location       loc = std::source_location::current()) override {
    std::unique_lock<std::mutex> poolLock(mutex);

    if (memory.empty()) {
      switch (mode) {
      case MemoryManagerAllocatedMode::Wait: {
        ++this->stats.waitCount;
        auto wts = std::chrono::system_clock::now();
        cv.wait(poolLock, [&]() { return !memory.empty(); });
        auto wte = std::chrono::system_clock::now();
        this->stats.waitTime += std::chrono::duration_cast<std::chrono::nanoseconds>(wte - wts);
      } break;
      case MemoryManagerAllocatedMode::Dynamic:
        if constexpr (std::is_default_constructible_v<T>) {
          memory.push_back(std::make_shared<T>());
        } else {
          logh::error("dynamically sized pool only support default constructible types (allocated<",
                      std::string(hh::tool::typeToStr<T>()), ">(Dynamic) failed), defaulting to wait mode.");
          return nullptr;
        }
        break;
      case MemoryManagerAllocatedMode::Fail:
        return nullptr;
        break;
      }
    }
    ++this->stats.nbGetMemory;
    auto data = memory.back();
    memory.pop_back();
    assert(!usedMemory.contains(data) && "allocating the same memory multiple times.");
    usedMemory.insert({data, UsedMemory{loc}});
    return data;
  }

  void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) override {
    std::lock_guard<std::mutex> poolLock(mutex);
    if (!usedMemory.contains(data)) {
      if (std::find(memory.begin(), memory.end(), data) != memory.end()) {
        logh::error("data of type '", std::string(hh::tool::typeToStr<T>()),
                    "` was returned to memory pool multiple time. The memory was returned at (", loc.file_name(), ":",
                    loc.line(), ").");
        return;
      }
      // in that case, we consider that the data has been allocated manually
      // and can be used to extend the pool.
    }
    if constexpr (requires { data->postProcess(); }) {
      data->postProcess();
    }
    if constexpr (requires { data->canBeRecycled(); }) {
      if (!data->canBeRecycled()) {
        return;
      }
    }
    if constexpr (requires { data->cleanMemory(); }) {
      data->cleanMemory();
    }
    ++this->stats.nbReturnMemory;
    memory.push_back(data);
    cv.notify_all();
    usedMemory.erase(data);
  }

  void fill(size_t size, auto &&...args) {
    std::lock_guard<std::mutex> poolLock(mutex);
    this->stats.preallocatedSize = size;
    memory.resize(size, nullptr);
    for (auto &data : memory) {
      data = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }

  std::string extraPrintingInformation() const override {
    std::string typeStr = hh::tool::typeToStr<T>();
    return "MemoryPool[" + typeStr + "]: { " + "default size = " + std::to_string(this->stats.preallocatedSize)
           + ", number gets = " + std::to_string(this->stats.nbGetMemory) + ", number returns = "
           + std::to_string(this->stats.nbReturnMemory) + ", pool size = " + std::to_string(memory.size())
           + ", wait time = " + comm::durationToString(this->stats.waitTime)
           + ", wait count = " + std::to_string(this->stats.waitCount) + " }";
  }

private:
  struct UsedMemory {
    std::source_location loc;
  };
  std::vector<std::shared_ptr<T>>          memory;
  std::map<std::shared_ptr<T>, UsedMemory> usedMemory;
  std::mutex                               mutex;
  std::condition_variable                  cv;
  struct {
    size_t                   preallocatedSize;
    size_t                   nbGetMemory;
    size_t                   nbReturnMemory;
    size_t                   waitCount;
    std::chrono::nanoseconds waitTime;
  } stats;
};

template <typename... Types>
class MemoryPool : public SingleTypeMemoryPool<Types>... {
public:
  std::shared_ptr<MemoryManager<Types...>> memoryManager() {
    return std::make_shared<MemoryManager<Types...>>(this);
  }

  template <typename T>
  std::shared_ptr<T> allocate(MemoryManagerAllocatedMode mode = MemoryManagerAllocatedMode::Fail,
                              std::source_location       loc = std::source_location::current()) {
    return static_cast<SingleTypeMemoryPool<T> *>(this)->allocate(mode, loc);
  }

  template <typename T>
  void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) {
    static_cast<SingleTypeMemoryPool<T> *>(this)->release(std::move(data), loc);
  }

  template <typename T>
  void fill(size_t size, auto &&...args) {
    static_cast<SingleTypeMemoryPool<T> *>(this)->fill(size, std::forward<decltype(args)>(args)...);
  }

  std::string extraPrintingInformation(std::string const &eol = "\\l") const {
    std::string infos = "MemoryManager: {" + eol;
    ([&] { infos.append("    " + static_cast<SingleTypeMemoryPool<Types> const*>(this)->extraPrintingInformation() + eol); }(),
     ...);
    return infos + "}" + eol;
  }
};

} // end namespace tool
} // end namespace comm
} // end namespace hh

#endif
