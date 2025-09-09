#ifndef COMMUNICATOR_COMMUNICATOR_CORE_TASK
#define COMMUNICATOR_COMMUNICATOR_CORE_TASK
#include "../log.hpp"
#include "comm_tools.hpp"
#include "communicator_memory_manager.hpp"
#include "generic_core_task.hpp"
#include <algorithm>
#include <condition_variable>
#include <numeric>
#include <thread>
#include <type_traits>

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
      comm::commInfog(logh::IG::Core, "core", this->task()->comm(), "start receiver");
      this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::recvDeamon, this);
    } else {
      comm::commInfog(logh::IG::Core, "core", this->task()->comm(), "start sender");
      this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::sendDeamon, this);
    }

    taskLoop();

    if (this->deamon_.joinable()) {
      comm::commInfog(logh::IG::Core, "core", this->task()->comm(), "join ", isReceiver ? "receiver" : "sender");
      this->deamon_.join();
    }

    this->postRun();
    this->wakeUp();

    comm::commInfog(logh::IG::CoreTerminate, "core terminate", this->task()->comm());
  }

private:
  void taskLoop() {
    using namespace std::chrono_literals;
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    std::condition_variable sleepCondition;
    std::vector<int> receivers;
    bool canTerminate = false;

    comm::commInfog(logh::IG::CoreTaskLoop, "core task loop", this->task()->comm(),
                    "start task loop, canTerminate = ", canTerminate);

    // Actual computation loop
    while (!this->canTerminate()) {
      // Wait for a data to arrive or termination
      this->nvtxProfiler()->startRangeWaiting();
      start = std::chrono::system_clock::now();
      canTerminate = this->sleep();
      finish = std::chrono::system_clock::now();
      this->nvtxProfiler()->endRangeWaiting();
      this->incrementWaitDuration(std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start));

      comm::commInfog(logh::IG::CoreTaskLoop, "core task loop", this->task()->comm(),
                      "run task loop, canTerminate = ", canTerminate);

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
      static size_t dbg_idx = 0;
      if (dbg_idx++ == 4000) {
        dbg_idx = 0;
        logh::warn("sender still running: channel = ", (int)this->task()->comm()->channel,
                   ", rank = ", this->task()->comm()->comm->rank,
                   ", queue size = ", this->task()->comm()->queues.sendOps.size(),
                   ", hasNotifierConnected = ", this->hasNotifierConnected());
      }
      comm::commProcessSendOpsQueue(this->task()->comm(), [&]<typename T>(std::shared_ptr<T> data) {
        if (mm_) {
          // TODO: if the data deos not come from this mm?
          mm_->returnMemory(std::move(data));
        }
      });
      std::this_thread::sleep_for(4ms);
    }
    comm::commSendSignal(this->task()->comm(), this->task()->comm()->receivers, comm::CommSignal::Disconnect);
    comm::commInfog(logh::IG::SenderDisconnect, "sender disconnect", this->task()->comm());
    comm::commProcessSendOpsQueue(
        this->task()->comm(),
        [&]<typename T>(std::shared_ptr<T> data) {
          if (mm_) {
            // TODO: if the data deos not come from this mm?
            mm_->returnMemory(std::move(data));
          }
        },
        true);
    comm::commInfog(logh::IG::SenderEnd, "sender end", this->task()->comm());
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

      if (!isConnected(connections)) {
        logh::error("non connected task: ops queue size = ", this->task()->comm()->queues.recvOps.size(),
                    ", data queue size = ", this->task()->comm()->queues.recvDataQueue.size());
      }

      switch (signal) {
      case comm::CommSignal::None:
        break;
      case comm::CommSignal::Disconnect:
        assert(connections[source] == true);
        connections[source] = false;
        comm::commInfog(logh::IG::ReceiverDisconnect, "receiver disconnect", this->task()->comm(), "source = ", source,
                        ", connections = ", connections);
        break;
      case comm::CommSignal::Data:
        break;
      }
      comm::commProcessRecvDataQueue(this->task()->comm(), [&]<typename T>() {
        if (!mm_) {
          if constexpr (std::is_default_constructible_v<T>) {
            return std::make_shared<T>();
          } else {
            throw std::runtime_error(
                "error: fail to create data in communicator task, provide a memory manager to solve this issue.");
          }
        }
        return mm_->template getMemory<T>();
      });
      comm::commProcessRecvOpsQueue(this->task()->comm(),
                                    [&]<typename T>(std::shared_ptr<T> data) { this->task()->addResult(data); });
      std::this_thread::sleep_for(4ms);
    }
    comm::commInfog(logh::IG::ReceiverEnd, "receiver end", this->task()->comm());
  }

public:
  [[nodiscard]] std::string extraPrintingInformation() const override {
    std::string infos;
    std::vector<comm::CommTaskStats> stats;

    if (!this->task()->comm()->comm->collectStats || this->task()->comm()->comm->nbProcesses == 1) {
      return infos;
    }

    comm::commBarrier();

    size_t nbProcesses = this->task()->comm()->comm->nbProcesses;
    if (this->task()->comm()->comm->rank == 0) {
      stats = comm::commGatherStats(this->task()->comm());
      std::map<comm::StorageId, comm::StorageInfo> storageStats;
      size_t maxSendOpsSize = 0;
      size_t maxRecvOpsSize = 0;
      size_t maxRecvDataQueueSize = 0;
      size_t maxSendStorageSize = 0;
      size_t maxRecvStorageSize = 0;
      auto transmissionStats = computeTransmissionStats(stats);

      infos.append("nbProcesses = " + std::to_string(nbProcesses) + "\n");
      std::string receiversStr = "[" + std::to_string(this->task()->comm()->receivers[0]);
      for (size_t i = 1; i < this->task()->comm()->receivers.size(); ++i) {
        receiversStr += ", " + std::to_string(this->task()->comm()->receivers[i]);
      }
      receiversStr += "]";
      infos.append("receivers = " + receiversStr + "\n");

      for (auto const &stat : stats) {
        maxSendOpsSize = std::max(maxSendOpsSize, stat.maxSendOpsSize);
        maxRecvOpsSize = std::max(maxRecvOpsSize, stat.maxRecvOpsSize);
        maxRecvDataQueueSize = std::max(maxRecvDataQueueSize, stat.maxRecvDataQueueSize);
        maxSendStorageSize = std::max(maxSendStorageSize, stat.maxSendStorageSize);
        maxRecvStorageSize = std::max(maxRecvStorageSize, stat.maxRecvStorageSize);
      }
      infos.append("maxSendOpsSize = " + std::to_string(maxSendOpsSize) + "\n");
      infos.append("maxRecvOpsSize = " + std::to_string(maxRecvOpsSize) + "\n");
      infos.append("maxRecvDataQueueSize = " + std::to_string(maxRecvDataQueueSize) + "\n");
      infos.append("maxSendStorageSize = " + std::to_string(maxSendStorageSize) + "\n");
      infos.append("maxRecvStorageSize = " + std::to_string(maxRecvStorageSize) + "\n");

      for (comm::u8 typeId = 0; typeId < TypesIds().size; ++typeId) {
        if (!transmissionStats.contains(typeId)) {
          continue;
        }
        strAppend(infos, "=== type :: ", (int)typeId, " ===");
        auto transmissionDelays = transmissionStats.at(typeId).transmissionDelays;
        auto packingDelay = transmissionStats.at(typeId).packingDelay;
        auto unpackingDelay = transmissionStats.at(typeId).unpackingDelay;
        auto bandWidth = transmissionStats.at(typeId).bandWdith;
        strAppend(infos, "packing: ", packingDelay, "us, (count = ", packingDelay.size(), ")");
        strAppend(infos, "unpacking: ", unpackingDelay, "us, (count = ", unpackingDelay.size(), ")");
        strAppend(infos, "bandWdith: ", bandWidth, "MB/s");
        infos.append("transmission: {\n");
        for (size_t sender = 0; sender < nbProcesses; ++sender) {
          for (size_t receiver = 0; receiver < nbProcesses; ++receiver) {
            if (transmissionDelays[sender * nbProcesses + receiver].empty()) {
              continue;
            }
            strAppend(infos, "[", sender, " -> ", receiver, "] = ", transmissionDelays[sender * nbProcesses + receiver],
                      "us");
          }
        }
        infos.append("}\n");
      }
    } else {
      comm::commSendStats(this->task()->comm());
    }
    // exchange stats
    return infos;
  }

private:
  static void strAppend(std::string &str, auto const &...args) {
    std::ostringstream oss;
    (
        [&] {
          if constexpr (std::is_same_v<decltype(args), std::vector<double> const &>) {
            auto avg = computeAvg(args);
            oss << avg.first << " +- " << avg.second;
          } else {
            oss << args;
          }
        }(),
        ...);
    str.append(oss.str() + "\n");
  }

  static std::pair<double, double> computeAvg(std::vector<double> const &values) {
    if (values.size() == 0) {
      return {0, 0};
    }
    double avg = 0;
    double stddev = 0;

    for (double value : values) {
      avg += value;
    }
    avg /= values.size();

    for (double value : values) {
      double diff = value - avg;
      stddev += diff * diff;
    }
    stddev = std::sqrt(stddev / values.size());
    return {avg, stddev};
  }

  struct TransmissionStat {
    std::vector<double> packingDelay;
    std::vector<double> unpackingDelay;
    std::vector<std::vector<double>> transmissionDelays;
    std::vector<double> bandWdith;
  };

  std::map<comm::u8, TransmissionStat> computeTransmissionStats(std::vector<comm::CommTaskStats> const &stats) const {
    std::map<comm::u8, TransmissionStat> transmissionStats;
    size_t nbProcesses = this->task()->comm()->comm->nbProcesses;

    for (auto receiverRank : this->task()->comm()->receivers) {
      for (auto recvStorageStat : stats[receiverRank].storageStats) {
        comm::StorageId storageId = recvStorageStat.first;
        comm::StorageInfo recvInfos = recvStorageStat.second;
        comm::u32 source = storageId.source;
        comm::u8 typeId = recvInfos.typeId;

        if (stats[source].storageStats.contains(storageId)) {
          auto sendInfos = stats[source].storageStats.at(storageId);
          if (!transmissionStats.contains(typeId)) {
            transmissionStats.insert({
                typeId,
                TransmissionStat{
                    .packingDelay = {},
                    .unpackingDelay = {},
                    .transmissionDelays = std::vector<std::vector<double>>(nbProcesses * nbProcesses),
                    .bandWdith = {},
                },
            });
          }
          auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(recvInfos.recvtp - sendInfos.sendtp);
          transmissionStats.at(typeId).transmissionDelays[source * nbProcesses + receiverRank].push_back(delay.count() / 1'000'000'000.);
          transmissionStats.at(typeId).packingDelay.push_back(sendInfos.packingTime.count() / 1'000'000'000.);
          transmissionStats.at(typeId).unpackingDelay.push_back(recvInfos.unpackingTime.count() / 1'000'000'000.);
          transmissionStats.at(typeId).bandWdith.push_back((sendInfos.dataSize / (1024. * 1024.)) /
                                                           (delay.count() / 1'000'000'000.));
        }
      }
    }
    return transmissionStats;
  }

public:
  void setMemoryManager(std::shared_ptr<tool::CommunicatorMemoryManager<Types...>> mm) { this->mm_ = mm; }

private:
  std::thread deamon_;
  std::shared_ptr<tool::CommunicatorMemoryManager<Types...>> mm_;
};

} // end namespace core

} // end namespace hh

#endif
