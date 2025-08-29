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

  [[nodiscard]] std::string extraPrintingInformation() const override {
    std::string networkStats;
    std::vector<comm::CommTaskStats> stats;

    if (!this->task()->comm()->comm->collectStats) {
      return networkStats;
    }

    size_t nbProcesses = this->task()->comm()->comm->nbProcesses;

    if (this->task()->comm()->comm->rank == 0) {
      stats = comm::commGatherStats(this->task()->comm());
      std::map<comm::StorageId, comm::StorageInfo> storageStats;
      size_t maxSendOpsSize = 0;
      size_t maxRecvOpsSize = 0;
      size_t maxRecvDataQueueSize = 0;
      size_t maxSendStorageSize = 0;
      size_t maxRecvStorageSize = 0;

      for (auto const &stat : stats) {
        maxSendOpsSize = std::max(maxSendOpsSize, stat.maxSendOpsSize);
        maxRecvOpsSize = std::max(maxRecvOpsSize, stat.maxRecvOpsSize);
        maxRecvDataQueueSize = std::max(maxRecvDataQueueSize, stat.maxRecvDataQueueSize);
        maxSendStorageSize = std::max(maxSendStorageSize, stat.maxSendStorageSize);
        maxRecvStorageSize = std::max(maxRecvStorageSize, stat.maxRecvStorageSize);
      }
      networkStats.append("maxSendOpsSize = " + std::to_string(maxSendOpsSize) + "\n");
      networkStats.append("maxRecvOpsSize = " + std::to_string(maxRecvOpsSize) + "\n");
      networkStats.append("maxRecvDataQueueSize = " + std::to_string(maxRecvDataQueueSize) + "\n");
      networkStats.append("maxSendStorageSize = " + std::to_string(maxSendStorageSize) + "\n");
      networkStats.append("maxRecvStorageSize = " + std::to_string(maxRecvStorageSize) + "\n");
      TODO("collect timing information");
    } else {
      comm::commSendStats(this->task()->comm());
    }
    // exchange stats
    return networkStats;
  }

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

    // TODO: empty the queue or flush? I think the best here would be to start a
    //       timer when isConnected is false. After the timer, the thread leaves
    //       the loops and flushes the queue, however, this should be reported
    //       as an error in the dot file.
    while (isConnected(connections) || !this->task()->comm()->queues.recvOps.empty() ||
           !this->task()->comm()->queues.recvDataQueue.empty()) {
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
      comm::commProcessRecvDataQueue(this->task()->comm(), [&]<typename T>() {
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
