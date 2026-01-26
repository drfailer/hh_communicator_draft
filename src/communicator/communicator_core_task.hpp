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

namespace hh {

template <typename... Types>
class CommunicatorTask;

namespace core {

// functors ////////////////////////////////////////////////////////////////////

template <typename TaskType>
struct ProcessData {
  TaskType *task;

  template <typename T>
  void operator()(std::shared_ptr<T> data) {
    task->addResult(data);
  }
};

// communicator core ///////////////////////////////////////////////////////////

template <typename... Types>
using CommunicatorCoreTaskBase = GenericCoreTask<CommunicatorTask<Types...>, sizeof...(Types), Types..., Types...>;

template <typename... Types>
class CommunicatorCoreTask : public CommunicatorCoreTaskBase<Types...> {
private:
  using TM = typename CommunicatorTask<Types...>::TM;

public:
  CommunicatorCoreTask(CommunicatorTask<Types...> *task, comm::CommService *service, std::string const &name)
      : CommunicatorCoreTaskBase<Types...>(task, name, 1, false),
        communicator_(service),
        senderDisconnect_(false) {}

  ~CommunicatorCoreTask() {}

public:
  void run() override {
    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    this->senderDisconnect_ = false;
    this->deamon_ = std::thread([this]() {
      this->communicator_.run(
              ProcessData(this->task()),
              [&]() { return this->senderDisconnect_; });
    });

    taskLoop();

    if (this->deamon_.joinable()) {
      this->deamon_.join();
    }

    this->postRun();
    this->wakeUp();
  }

private:
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
    this->senderDisconnect_ = true;
  }

public:
  [[nodiscard]] std::string extraPrintingInformation() const override {
    std::string infos;

    if (this->mm_) {
      infos += mm_->extraPrintingInformation();
    }

    if (!communicator_.service()->collectStats() || communicator_.nbProcesses() == 1) {
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
  void setMemoryManager(std::shared_ptr<comm::tool::MemoryManager<Types...>> mm) {
      this->mm_ = mm;
      this->communicator_.memoryManager(mm);
  }

  comm::Communicator<Types...> *comm() {
    return &communicator_;
  }

private:
  std::thread                                          deamon_;
  std::shared_ptr<comm::tool::MemoryManager<Types...>> mm_;
  comm::Communicator<Types...>                         communicator_;
  bool                                                 senderDisconnect_;
};

} // end namespace core

} // end namespace hh

#endif
