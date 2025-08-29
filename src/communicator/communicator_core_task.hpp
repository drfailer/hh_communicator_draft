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
    bool isReceiver =
        std::find(receivers.begin(), receivers.end(), this->task()->comm()->comm->rank) != receivers.end();

    if (isReceiver) {
      logh::infog(logh::IG::Core, "core", "start receiver");
      this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::recvDeamon, this);
    } else {
      logh::infog(logh::IG::Core, "core", "start sender");
      this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::sendDeamon, this);
    }

    taskLoop();

    if (this->deamon_.joinable()) {
      logh::infog(logh::IG::Core, "core", "join ", isReceiver ? "receiver" : "sender");
      this->deamon_.join();
    }

    this->postRun();
    this->wakeUp();
  }

  // [[nodiscard]] std::string extraPrintingInformation() const override {
  //     std::string infos = this->task_->extraPrintingInformation();
  //     // TODO: add network infos
  //     return infos;
  // }

private:
  void taskLoop() {
    using namespace std::chrono_literals;
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    std::condition_variable sleepCondition;
    std::vector<int> receivers;
    bool canTerminate = false;

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

  void sendDeamon() {
    using namespace std::chrono_literals;
    while (!this->canTerminate()) {
      comm::commProcessSendOpsQueue(this->task()->comm(), []<typename T>(std::shared_ptr<T>) {
        // TODO: return to memory manager
      });
      std::this_thread::sleep_for(4ms);
    }
    comm::commSendSignal(this->task()->comm(), this->task()->comm()->receivers, comm::CommSignal::Disconnect);
    comm::commProcessSendOpsQueue(
        this->task()->comm(),
        []<typename T>(std::shared_ptr<T>) {
          // TODO: return to memory manager
        },
        true);
  }

  void recvDeamon() {
    using namespace std::chrono_literals;
    std::vector<bool> connections(this->task()->comm()->comm->nbProcesses, true);
    int source = -1;
    comm::CommSignal signal = comm::CommSignal::None;
    comm::Header header;

    for (auto receiver : this->task()->comm()->receivers) {
      connections[receiver] = false;
    }

    // TODO: empty the queue or flush?
    while (isConnected(connections) || !this->task()->comm()->queues.recvOps.empty() ||
           !this->task()->comm()->queues.pendingRecvData.empty()) {
      comm::commRecvSignal(this->task()->comm(), source, signal, header);

      switch (signal) {
      case comm::CommSignal::None:
        break;
      case comm::CommSignal::Disconnect:
        logh::infog(logh::IG::ReceiverDisconnect, "receiver disconnect", "source = ", source,
                    " channel = ", (int)this->task()->comm()->channel, " rank = ", this->task()->comm()->comm->rank,
                    " connections = ", connections);
        assert(connections[source] == true);
        connections[source] = false;
        break;
      case comm::CommSignal::Data:
        break;
      }
      comm::commProcessPendingRecvData(this->task()->comm(), [&]<typename T>() {
        static int test_idx = 0;
        if (test_idx++ % 2 == 0) {
          logh::warn("create data return nullptr");
          return std::shared_ptr<T>{nullptr};
        }
        // TODO: get from memory manager
        return std::make_shared<T>();
      });
      comm::commProcessRecvOpsQueue(this->task()->comm(),
                                    [&]<typename T>(std::shared_ptr<T> data) { this->task()->addResult(data); });
      std::this_thread::sleep_for(4ms);
    }
    logh::infog(logh::IG::ReceiverEnd, "receiver end", "channel = ", (int)this->task()->comm()->channel,
                " rank = ", this->task()->comm()->comm->rank);
  }

private:
  std::thread deamon_;
};

} // end namespace core

} // end namespace hh

#endif
