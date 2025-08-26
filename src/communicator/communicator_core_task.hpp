#ifndef COMMUNICATOR_COMMUNICATOR_CORE_TASK
#define COMMUNICATOR_COMMUNICATOR_CORE_TASK
#include "../log.hpp"
#include "comm_tools.hpp"
#include "generic_core_task.hpp"
#include <condition_variable>
#include <thread>

namespace hh {

template <typename... Types> class CommunicatorTask;

namespace core {

template <typename... Types>
using CommunicatorCoreTaskBase = GenericCoreTask<CommunicatorTask<Types...>, sizeof...(Types), Types..., Types...>;

template <typename... Types> class CommunicatorCoreTask : public CommunicatorCoreTaskBase<Types...> {
private:
  using TypesIds = typename CommunicatorTask<Types...>::TypesIds;

public:
  CommunicatorCoreTask(CommunicatorTask<Types...> *task, std::string const &name)
      : CommunicatorCoreTaskBase<Types...>(task, name, 1, false) {}

  [[nodiscard]] bool isConnected(std::vector<bool> const &connections) const {
    for (bool connection : connections) {
      if (connection) {
        return true;
      }
    }
    return false;
  }

  void run() override {
    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    auto receivers = this->task()->comm()->receivers;

    // TODO: add a way to implement a 2 threads version in case a process has to
    // operate both as sender and receiver
    if (std::find(receivers.begin(), receivers.end(), this->task()->comm()->handle->rank) != receivers.end()) {
      runAsReceiver();
    } else {
      runAsSender();
    }

    // Do the shutdown phase
    this->postRun();
    // Wake up a node that this node is linked to
    this->wakeUp();
  }

private:
  void runAsReceiver() {
    using namespace std::chrono_literals;
    std::vector<bool> connections(this->task()->comm()->handle->nbProcesses, true);
    int source;
    comm::CommSignal signal;
    comm::Header header;

    for (auto receiverRank : this->task()->comm()->receivers) {
      connections[receiverRank] = false;
    }

    while (isConnected(connections) || !this->task()->comm()->queues.recvOps.empty()) {
      comm::commRecvSignal(this->task()->comm(), source, signal, header);

      switch (signal) {
      case comm::CommSignal::None:
        break;
      case comm::CommSignal::Disconnect:
        INFO_GRP("disconnect: from " << source << ", channel = " << (int)this->task()->comm()->channel << ", rank = "
                                     << this->task()->comm()->handle->rank << ", connections = " << connections,
                 INFO_GRP_RECEIVER_DISCONNECT);
        assert(connections[source] == true);
        connections[source] = false;
        break;
      case comm::CommSignal::Data:
        comm::commRecvData(this->task()->comm(), source, header, [&]<typename T>() { return std::make_shared<T>(); });
        break;
      }
      comm::commProcessRecvDataQueue(this->task()->comm(),
                                     [&]<typename T>(std::shared_ptr<T> data) { this->task()->addResult(data); });
      std::this_thread::sleep_for(4ms);
    }
    INFO_GRP("receiver end: channel = " << (int)this->task()->comm()->channel
                                        << ", rank = " << this->task()->comm()->handle->rank,
             INFO_GRP_RECEIVER_END);
  }

  void runAsSender() {
    using namespace std::chrono_literals;
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    std::condition_variable sleepCondition;

    this->nvtxProfiler()->startRangeWaiting();
    start = std::chrono::system_clock::now();

    // Actual computation loop
    while (!this->canTerminate() || !this->task()->comm()->queues.sendOps.empty()) {
      {
        std::unique_lock<std::mutex> lock(waitMutex_);
        sleepCondition.wait_for(lock, 4ms, [&]() { return !this->receiversEmpty(); });
      }

      // Operate the connectedReceivers to get a data and send it to execute
      if (!this->receiversEmpty()) {
        finish = std::chrono::system_clock::now();
        this->nvtxProfiler()->endRangeWaiting();
        this->incrementWaitDuration(std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start));
        this->nvtxProfiler()->startRangeWaiting();
        start = std::chrono::system_clock::now();
        this->operateReceivers();
      }
      comm::commProcessSendQueue(this->task()->comm());
    }

    comm::commSendSignal(this->task()->comm(), comm::CommSignal::Disconnect);
    comm::commProcessSendQueue(this->task()->comm());
  }

private:
  std::mutex waitMutex_;
};

} // end namespace core

} // end namespace hh

#endif
