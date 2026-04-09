#ifndef COMMUNICATOR_MEMORY_MANAGER_AUTOMATIC_MEMORY_POOL
#define COMMUNICATOR_MEMORY_MANAGER_AUTOMATIC_MEMORY_POOL
#include "memory_manager.hpp"
#include <condition_variable>
#include <iostream>
#include <memory>
#include <source_location>
#include <vector>
#ifdef HH_COMM_DEBUG
#include <map>
#include <unistd.h>
#endif

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {
/// @brief Tools namespace
namespace tool {

/******************************************************************************/
/*                       SingleTypeAutomaticMemoryPool                        */
/******************************************************************************/

/// @brief Automatic memory pool that uses the shared pointer destructor to
///        return memory.
/// @tparam T Type managed by the pool.
template <typename T>
class SingleTypeAutomaticMemoryPool
    : public std::enable_shared_from_this<SingleTypeAutomaticMemoryPool<T>>,
      public SingleTypeMemoryManager<T> {
public:
  /// @brief Since it inherits from enable_shared_from_this, the constructor is
  ///        private and we use a static create method that returns a shared pointer.
  /// @return Shared pointer to an instance of the pool.
  static std::shared_ptr<SingleTypeAutomaticMemoryPool<T>> create() {
    return std::shared_ptr<SingleTypeAutomaticMemoryPool<T>>(new SingleTypeAutomaticMemoryPool<T>());
  }

  /// @brief Deleted copy constructor.
  SingleTypeAutomaticMemoryPool(const SingleTypeAutomaticMemoryPool &) = delete;

  /// @brief Deleted copy operator.
  SingleTypeAutomaticMemoryPool &operator=(const SingleTypeAutomaticMemoryPool &) = delete;

  /// @brief Deleted move constructor.
  SingleTypeAutomaticMemoryPool(SingleTypeAutomaticMemoryPool &&) = delete;

  /// @brief Deleted move operator.
  SingleTypeAutomaticMemoryPool &operator=(SingleTypeAutomaticMemoryPool &&) = delete;

public:
  /// @brief Resource deleter used the shared_ptr to return the memory when the
  ///        use count is 0.
  struct ResourceDeleter {
    std::weak_ptr<SingleTypeAutomaticMemoryPool> weakPool_; ///< Pointer to the pool.

    /// @brief Return the given pointer to the automatic pool.
    /// @param ptr Deleted pointer
    void operator()(T *ptr) const {
      if (auto pool = weakPool_.lock()) {
        pool->release_ptr(ptr);
      } else {
        delete ptr;
      }
    }
  };

  /// @brief Release method use by shared_ptr.
  /// @param data Pointer to release.
  void release_ptr(T *data) {
    if (data == nullptr)
      return;

    const auto lg = std::lock_guard(mutex_);
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

#ifdef HH_COMM_DEBUG
    alloc_locations_.erase((uintptr_t)data);
#endif
    memory_.emplace_back(std::unique_ptr<T>(data));
    cv_.notify_all();
  }

  /// @brief Implementation of the `allocate` method for the SingleTypeMemoryManager
  /// @param mode Allocation mode.
  /// @param loc  Caller location (used for debuggin, see report_uses method).
  std::shared_ptr<T> allocate(const MemoryManagerAllocateMode mode = MemoryManagerAllocateMode::Fail,
                              [[maybe_unused]]std::source_location loc = std::source_location::current()) override {
    auto lock = std::unique_lock(mutex_);

    if (memory_.empty()) {
      switch (mode) {
      case MemoryManagerAllocateMode::Wait: {
        cv_.wait(lock, [&]() { return !memory_.empty(); });
        break;
      }
      case MemoryManagerAllocateMode::Dynamic: {
        if constexpr (std::is_default_constructible_v<T>) {
          memory_.push_back(std::make_shared<T>());
        } else {
          return std::shared_ptr<T>(nullptr);
        }
        break;
      }
      case MemoryManagerAllocateMode::Fail: {
        return std::shared_ptr<T>(nullptr);
      }
      }
    }
    auto res = std::move(memory_.back());
    auto raw = res.release();
    memory_.pop_back();
#ifdef HH_COMM_DEBUG
    alloc_locations_.insert({(uintptr_t)raw, loc});
#endif
    return std::shared_ptr<T>(raw, ResourceDeleter{this->shared_from_this()});
  }

  /// @brief Implementation of the `release` method for the
  ///        SingleTypeMemoryManager (does nothing because we use the shared_ptr
  ///        destructor to release)
  void release(std::shared_ptr<T> &&, std::source_location = std::source_location::current()) override { /* do nothing */ }

  std::string extraPrintingInformation() const override {
    static std::string typeStr = hh::tool::typeToStr<T>();
    std::ostringstream oss;
    oss << "AutomaticMemoryPool[" << typeStr << "]: { poolSize = " << memory_.size() << " }";
    return oss.str();
  }

  /// @brief Preallocates elements.
  /// @param capacity Number of elements to preallocate.
  /// @param args     Arguments to pass to T's constructor.
  template <typename... Args>
  void fill(const size_t capacity, Args &&...args) {
    const auto lg = std::lock_guard(mutex_);
    for (size_t i = 0; i < capacity; ++i) {
      memory_.emplace_back(std::make_unique<T>(args...));
    }
  }

  /// @brief Return the number of elements in the pool.
  /// @return Number of elements in the pool.
  [[nodiscard]] auto size() {
    const auto lg = std::lock_guard(mutex_);
    return memory_.size();
  }

  /// @brief Report all the pointers that were not returned to the pool, and
  ///        show the allocation location.
  void report_uses() {
#ifdef HH_COMM_DEBUG
    const auto lg = std::lock_guard(mutex_);
    static std::string typeStr = hh::tool::typeToStr<T>();
    for (auto const &[ptr, loc] : this->alloc_locations_) {
      log::error((T*)ptr, " of type `", typeStr, "' allocated at (", loc.file_name(), ":", loc.line(),  ") is still used.");
    }
#endif
  }

private:
  /// @brief Private default constructor (Use create() for shared_ptr management)
  explicit SingleTypeAutomaticMemoryPool() = default;

private:
  std::vector<std::unique_ptr<T>> memory_ = {}; ///< memory of the pool.
  std::mutex                      mutex_ = {};  ///< mutex for thread safety.
  std::condition_variable         cv_ = {};     ///< condition variable used for the wait allocation mode.
#ifdef HH_COMM_DEBUG
  std::map<uintptr_t, std::source_location> alloc_locations_; ///< debug map that tracks the used pointers.
#endif
};

/******************************************************************************/
/*                            AutomaticMemoryPool                             */
/******************************************************************************/

/// @brief Automatic memory pool for multiple types.
/// @tparam Types Types managed by the pool.
template <typename... Types>
class AutomaticMemoryPool {
private:
  std::tuple<std::shared_ptr<SingleTypeAutomaticMemoryPool<Types>>...> pools_; ///< single type memory pools.

public:
  /// @brief Default constructor of the pool.
  explicit AutomaticMemoryPool()
      : pools_(SingleTypeAutomaticMemoryPool<Types>::create()...) {};

  /// @brief Call the allocate method for the type `T`.
  /// @tparam T Type of the element to allocate.
  /// @param mode Allocate mode.
  /// @param loc  Source location.
  /// @return Newly allocated element.
  template <typename T>
  std::shared_ptr<T> allocate(MemoryManagerAllocateMode mode = MemoryManagerAllocateMode::Fail,
                              std::source_location       loc = std::source_location::current()) {
    return std::get<std::shared_ptr<SingleTypeAutomaticMemoryPool<T>>>(this->pools_)->allocate(mode, loc);
  }

  /// @brief Call the release method for the type `T`.
  /// @tparam T Type of the element to release.
  /// @param data Pointer to the element to release.
  /// @param loc  Source location.
  template <typename T>
  void release(std::shared_ptr<T> &&, std::source_location = std::source_location::current()) { /* do nothing */ }

  /// @brief Call the fill method for the type `T`.
  /// @tparam T Type for which we want to fill the pool.
  /// @param count Number of elements to pre-allocate.
  /// @param args  Arguments to give to the `T` constructor.
  template <typename T>
  void fill(const size_t capacity, auto &&...args) {
    std::get<std::shared_ptr<SingleTypeAutomaticMemoryPool<T>>>(this->pools_)->fill(capacity, args...);
  }

  /// @brief Returns a pointer to a SingleTypeMemoryManager for the type T.
  /// @tparam T Managed type.
  /// @return Pointer to a SingleTypeMemoryManager.
  template <typename T>
  SingleTypeMemoryManager<T> *getMemoryManager() {
      return std::get<std::shared_ptr<SingleTypeAutomaticMemoryPool<T>>>(this->pools_).get();
  }

  /// @brief Report the used pointers for all the managed types.
  void report_uses() {
#ifdef HH_COMM_DEBUG
    (std::get<std::shared_ptr<SingleTypeAutomaticMemoryPool<Types>>>(this->pools_)->report_uses(), ...);
#endif
  }

  /// @brief Returns the extra printing information for all the pools.
  /// @return String that contains the profiling information to print in the
  ///         dot file.
  std::string extraPrintingInformation(std::string const &eol = "\\l") const {
    std::string infos = "MemoryManager: {" + eol;
    ([&] {
      infos.append("    "
              + std::get<std::shared_ptr<SingleTypeAutomaticMemoryPool<Types>>>(this->pools_)->extraPrintingInformation()
              + eol);
    }(),...);
    return infos + "}" + eol;
  }
};

} // end namespace tool

} // end namespace comm

} // end namespace hh

#endif
