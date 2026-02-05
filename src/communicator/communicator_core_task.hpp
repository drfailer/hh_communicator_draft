#ifndef COMMUNICATOR_COMMUNICATOR_CORE_TASK
#define COMMUNICATOR_COMMUNICATOR_CORE_TASK
#include "../log.hpp"
#include "communicator.hpp"
#include "tool/memory_manager.hpp"
#include "generic_core_task.hpp"
#include "stats.hpp"
#include <algorithm>
#include <condition_variable>
#include <numeric>
#include <thread>
#include <type_traits>

/// @brief Hedgehog namespace
namespace hh {

/// @brief Forward declaration of the Communicator task.
/// @tparam Types handled by the communicator task.
template <typename... Types>
class CommunicatorTask;

/// @brief Core namespace
namespace core {

/// @brief Base Type of the communicator core task (uses the generic core task).
/// @tparam Types Types handled by the communicator.
template <typename... Types>
using CommunicatorCoreTaskBase = GenericCoreTask<CommunicatorTask<Types...>, sizeof...(Types), Types..., Types...>;

/// @brief Communicator core task.
template <typename... Types>
class CommunicatorCoreTask : public CommunicatorCoreTaskBase<Types...> {
private:
  /// @brief Type map type.
  using TM = typename CommunicatorTask<Types...>::TM;

public:
  /// @brief Constructor that takes the task the service and name.
  /// @param task    Pointer to the communicator task.
  /// @param service Pointer to the comm service.
  /// @param name    Name of the communicator task.
  CommunicatorCoreTask(CommunicatorTask<Types...> *task, comm::CommService *service, std::string const &name)
      : CommunicatorCoreTaskBase<Types...>(task, name, 1, false),
        communicator_(service) {}

  /// @brief Empty destructor.
  ~CommunicatorCoreTask() {}

public:
  /// @brief Run the core task.
  ///
  /// 1. prerun + communicator initialization
  /// 2. start communicator thread
  /// 3. enter the run loop (classic hedgehog core task operation)
  /// 4. when `canTerminate()`, leave the task loop and wait for the
  ///    communicator termination.
  void run() override {
    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    this->communicator_.init();
    this->deamon_ = std::thread([this]() {
      this->communicator_.run([&](auto data) { this->task()->addResult(std::move(data)); });
    });
    taskLoop();
    this->communicator_.fini(); // fini is called after finishPushingData

    if (this->deamon_.joinable()) {
      this->deamon_.join();
    }

    this->postRun();
    this->wakeUp();
  }

private:
  /// @brief Same implementation as the `run` function of the default CoreTask.
  void taskLoop() {
    using namespace std::chrono_literals;
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    std::condition_variable                            sleepCondition;
    bool                                               canTerminate = false;

    // Actual computation loop
    while (!this->canTerminate()) {
      // Wait for a data to arrive or termination
      this->nvtxProfiler()->startRangeWaiting();
      start = std::chrono::system_clock::now();
      canTerminate = this->sleep();
      finish = std::chrono::system_clock::now();
      this->nvtxProfiler()->endRangeWaiting();
      this->incrementWaitDuration(std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start));

      // If loop can terminate break the loop early
      if (canTerminate) {
        break;
      }

      // Operate the connectedReceivers to get a data and send it to execute
      this->operateReceivers();
    }
  }

public:
  /// @brief Return the extra profiling information to print in the dot file.
  /// @return String that contains the extra information.
  [[nodiscard]] std::string extraPrintingInformation() const override {
    std::string infos;

    if (this->mm_) {
      infos += mm_->extraPrintingInformation();
    }

    if (!communicator_.service()->profilingEnabled() || communicator_.nbProcesses() == 1) {
      return infos;
    }

    communicator_.service()->barrier();
    if (communicator_.rank() == 0) {
      infos += comm::CommTaskStats::template extraPrintingInformation<TM>(communicator_.gatherStats(),
                                                                          communicator_.service()->startTime(),
                                                                          communicator_.channel(),
                                                                          communicator_.nbProcesses());
    } else {
      communicator_.sendStats();
    }
    return infos;
  }

public:
  /// @brief Set the memory manager for the task.
  void setMemoryManager(std::shared_ptr<comm::tool::MemoryManager<Types...>> mm) {
      this->mm_ = mm;
      this->communicator_.memoryManager(mm);
  }

  /// @brief Accessor for the communicator.
  /// @return Pointer to the communicator.
  comm::Communicator<Types...> *comm() {
    return &communicator_;
  }

private:
  std::thread                                          deamon_;       ///< communicator thread
  std::shared_ptr<comm::tool::MemoryManager<Types...>> mm_;           ///< memory manager
  comm::Communicator<Types...>                         communicator_; ///< communicator (network transfer logic)
};

} // end namespace core

} // end namespace hh

#endif
