#ifndef COMMUNICATOR_TOOL_MEMORY_POOL
#define COMMUNICATOR_TOOL_MEMORY_POOL
#include <hedgehog.h>
#include "../tool/log.hpp"
#include "../profiling/profiling_tools.hpp"
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

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {
/// @brief Tools namespace
namespace tool {

/// @brief Implementation of the `SingleTypeMemoryManager` for the type T.
/// @tparam T Type of the element managed by the pool.
template <typename T>
class SingleTypeMemoryPool : public SingleTypeMemoryManager<T> {
public:
  /// @brief Destructor: verify if there are elements in the usedMemory. If it
  ///        is the case, these elements where not returned properly to the
  ///        pool, therefore we print an error message.
  ~SingleTypeMemoryPool() override {
    if (usedMemory.size() > 0) {
      log::error(usedMemory.size(), " elements of type `", hh::tool::typeToStr<T>(),
                 "' were not returned to the pool.");
      for (auto um : usedMemory) {
        auto loc = um.second.loc;
        log::error("Memory allocated at (", loc.file_name(), ":", loc.line(),
                   ") was not returned to the pool (type = `", hh::tool::typeToStr<T>(), "').");
      }
    }
  }

  /// @brief Allocate a new element from the pool. Depending on the mode, the
  ///        function will either return nullptr if the pool is empty,
  ///        dynamically allocate a new element (requires the type to be
  ///        default constructible) or wait until some elements return to the
  ///        pool. Note that this pool profiles the number of
  ///        allocated/released elements as well as the waiting time.
  /// @param mode Allocation mode
  /// @param loc  Source location used to track the memory.
  /// @return Newly allocated element.
  std::shared_ptr<T> allocate(MemoryManagerAllocateMode mode = MemoryManagerAllocateMode::Fail,
                              std::source_location       loc = std::source_location::current()) override {
    std::unique_lock<std::mutex> poolLock(mutex);

    if (memory.empty()) {
      switch (mode) {
      case MemoryManagerAllocateMode::Wait: {
        ++this->stats.waitCount;
        auto wts = std::chrono::system_clock::now();
        cv.wait(poolLock, [&]() { return !memory.empty(); });
        auto wte = std::chrono::system_clock::now();
        this->stats.waitTime += std::chrono::duration_cast<std::chrono::nanoseconds>(wte - wts);
      } break;
      case MemoryManagerAllocateMode::Dynamic:
        if constexpr (std::is_default_constructible_v<T>) {
          memory.push_back(std::make_shared<T>());
        } else {
          log::error("dynamically sized pool only support default constructible types (allocated<",
                     hh::tool::typeToStr<T>(), ">(Dynamic) failed), defaulting to wait mode.");
          return nullptr;
        }
        break;
      case MemoryManagerAllocateMode::Fail:
        return nullptr;
        break;
      }
    }
    ++this->stats.allocateCount;
    auto data = memory.back();
    memory.pop_back();
    assert(!usedMemory.contains(data) && "allocating the same memory multiple times.");
    usedMemory.insert({data, UsedMemory{loc}});
    return data;
  }

  /// @brief Return the given element to the pool. This function verifies that
  ///        the released element is not already present in the free list and
  ///        prints an error messages if it is the case (to avoid double free).
  /// @param data Element to return to the pool.
  /// @param loc  Source location.
  void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) override {
    std::lock_guard<std::mutex> poolLock(mutex);
    if (!usedMemory.contains(data)) {
      if (std::find(memory.begin(), memory.end(), data) != memory.end()) {
        log::error("data of type '", hh::tool::typeToStr<T>(),
                   "` was returned to memory pool multiple time. The memory was returned at (",
                   loc.file_name(), ":", loc.line(), ").");
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
    ++this->stats.releaseCount;
    memory.push_back(data);
    cv.notify_all();
    usedMemory.erase(data);
  }

  /// @brief Used to pre-allocated a certain number of elements at the start.
  /// @param count Number of elements to allocate.
  /// @param args  Arguments to give to the data type constructor.
  void fill(size_t count, auto &&...args) {
    std::lock_guard<std::mutex> poolLock(mutex);
    this->stats.preallocatedSize = count;
    memory.resize(count, nullptr);
    for (auto &data : memory) {
      data = std::make_shared<T>(std::forward<decltype(args)>(args)...);
    }
  }

  /// @brief Returns the extra information to print in the dot file. Here are
  ///        the information contained in the result:
  ///        - default size: number of preallocated element (fill)
  ///        - allocate count: number of allocations
  ///        - release count: number of releases
  ///        - pool size: final queue size (usefull if using dynamic pool)
  ///        - wait time: time spend waiting for memory in the pool when using
  ///          the Wait allocation mode.
  ///        - wait count: number of times the pool had to wait for memory when
  ///          using the Wait mode.
  /// @return Strings containing the extra information.
  std::string extraPrintingInformation() const override {
    std::string typeStr = hh::tool::typeToStr<T>();
    return "MemoryPool[" + typeStr + "]: { " + "default size = " + std::to_string(this->stats.preallocatedSize)
           + ", allocate count  = " + std::to_string(this->stats.allocateCount) + ", release count = "
           + std::to_string(this->stats.releaseCount) + ", pool size = " + std::to_string(memory.size())
           + ", wait time = " + comm::durationToString(this->stats.waitTime)
           + ", wait count = " + std::to_string(this->stats.waitCount) + " }";
  }

private:
  /// @brief Structure that store information about the used memory
  struct UsedMemory {
    std::source_location loc; ///< source location where the data was allocated
  };
  std::vector<std::shared_ptr<T>>          memory;     ///< list of free elements
  std::map<std::shared_ptr<T>, UsedMemory> usedMemory; ///< list of allocated elements
  std::mutex                               mutex;      ///< mutex to make the pool thread-safe
  std::condition_variable                  cv;         ///< conditional variable used for waiting

  /// @brief Structure that contains all the profiling information
  struct {
    size_t                   preallocatedSize; ///< number of preallocated elements (fill)
    size_t                   allocateCount;    ///< number of allocation
    size_t                   releaseCount;     ///< number of return
    size_t                   waitCount;        ///< number of time the pool waited for memory
    std::chrono::nanoseconds waitTime;         ///< total wait time
  } stats;
};

/// @brief Memory pool that manages multiple types.
///
/// Example:
///
/// @code
/// hh::comm::tool::MemoryPool<char, int, double> pool;
/// pool.fill<char>(10);
/// pool.fill<int>(10, 42); // with constructor argument
/// pool.fill<double>(10);
///
/// auto comm = hh::comm::CommunicatorTask<char, int, double>(commService);
/// comm->setMemoryManager(pool);
/// @endcode
///
/// /!\ If we don't fill the pool with a type, the communicator will not be able
/// to receive data from this type unless another part of the code augments
/// the size of the pool (dynamic allocate, or release a non managed element
/// (transfer ownership to the pool)).
///
/// @tparam Types Types managed by the pool.
template <typename... Types>
class MemoryPool : public SingleTypeMemoryPool<Types>... {
public:
  /// @brief Call the allocate method for the type `T`.
  /// @tparam T Type of the element to allocate.
  /// @param mode Allocate mode.
  /// @param loc  Source location.
  /// @return Newly allocated element.
  template <typename T>
  std::shared_ptr<T> allocate(MemoryManagerAllocateMode mode = MemoryManagerAllocateMode::Fail,
                              std::source_location       loc = std::source_location::current()) {
    return SingleTypeMemoryPool<T>::allocate(mode, loc);
  }

  /// @brief Call the release method for the type `T`.
  /// @tparam T Type of the element to release.
  /// @param data Pointer to the element to release.
  /// @param loc  Source location.
  template <typename T>
  void release(std::shared_ptr<T> &&data, std::source_location loc = std::source_location::current()) {
    SingleTypeMemoryPool<T>::release(std::move(data), loc);
  }

  /// @brief Call the fill method for the type `T`.
  /// @tparam T Type for which we want to fill the pool.
  /// @param count Number of elements to pre-allocate.
  /// @param args  Arguments to give to the `T` constructor.
  template <typename T>
  void fill(size_t count, auto &&...args) {
    SingleTypeMemoryPool<T>::fill(count, args...);
  }

  /// @brief Returns the extra printing information for all the pools.
  /// @return String that contains the profiling information to print in the
  ///         dot file.
  std::string extraPrintingInformation(std::string const &eol = "\\l") const {
    std::string infos = "MemoryManager: {" + eol;
    ([&] { infos.append("    " + SingleTypeMemoryPool<Types>::extraPrintingInformation() + eol); }(),...);
    return infos + "}" + eol;
  }
};

} // end namespace tool
} // end namespace comm
} // end namespace hh

#endif
