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

template <typename T>
class SingleTypeAutomaticMemoryPool
    : public std::enable_shared_from_this<SingleTypeAutomaticMemoryPool<T>>,
      public SingleTypeMemoryManager<T> {
public:
  static std::shared_ptr<SingleTypeAutomaticMemoryPool<T>> create() {
    return std::shared_ptr<SingleTypeAutomaticMemoryPool<T>>(new SingleTypeAutomaticMemoryPool<T>());
  }

  SingleTypeAutomaticMemoryPool(const SingleTypeAutomaticMemoryPool &) = delete;
  SingleTypeAutomaticMemoryPool &operator=(const SingleTypeAutomaticMemoryPool &) = delete;
  SingleTypeAutomaticMemoryPool(SingleTypeAutomaticMemoryPool &&) = delete;
  SingleTypeAutomaticMemoryPool &operator=(SingleTypeAutomaticMemoryPool &&) = delete;

  struct ResourceDeleter {
    std::weak_ptr<SingleTypeAutomaticMemoryPool> weakPool_;

    void operator()(T *ptr) const {
      if (auto pool = weakPool_.lock()) {
        pool->release_ptr(ptr);
      } else {
        delete ptr;
      }
    }
  };

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

  void release(std::shared_ptr<T> &&, std::source_location = std::source_location::current()) override { /* do nothing */ }

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

  std::string extraPrintingInformation() const override {
    static std::string typeStr = hh::tool::typeToStr<T>();
    std::ostringstream oss;
    oss << "AutomaticMemoryPool[" << typeStr << "]: { poolSize = " << memory_.size() << " }";
    return oss.str();
  }

  template <typename... Args>
  void fill(const size_t capacity, Args &&...args) {
    const auto lg = std::lock_guard(mutex_);
    for (size_t i = 0; i < capacity; ++i) {
      memory_.emplace_back(std::make_unique<T>(args...));
    }
  }

  [[nodiscard]] auto size() {
    const auto lg = std::lock_guard(mutex_);
    return memory_.size();
  }

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
  explicit SingleTypeAutomaticMemoryPool() = default; // Use create() for shared_ptr management

private:
  std::vector<std::unique_ptr<T>> memory_ = {};
  std::mutex                      mutex_ = {};
  std::condition_variable         cv_ = {};
#ifdef HH_COMM_DEBUG
  std::map<uintptr_t, std::source_location> alloc_locations_;
#endif
};

template <typename... Types>
class AutomaticMemoryPool {
private:
  std::tuple<std::shared_ptr<SingleTypeAutomaticMemoryPool<Types>>...> pools_;

public:
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

  template <typename T>
  SingleTypeMemoryManager<T> *getMemoryManager() {
      return std::get<std::shared_ptr<SingleTypeAutomaticMemoryPool<T>>>(this->pools_).get();
  }

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

// int main() {
//     const auto pool = SingleTypeAutomaticMemoryPool<Resource>::create();
//     pool->fill(4, 8.16, 32);
//
//     {
//         auto res = pool->allocate();
//         printf("[Acquired %f %d]\n", res->val1_, res->val2_);
//     }
//
//     return 0;
// }

#endif
