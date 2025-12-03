#ifndef COMMUNICATOR_COMMUNICATOR_CORE_TASK
#define COMMUNICATOR_COMMUNICATOR_CORE_TASK
#include "../log.hpp"
#include "communicator_memory_manager.hpp"
#include "generic_core_task.hpp"
#include "task_communicator.hpp"
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

template <typename... Types>
struct GetMemory {
  std::shared_ptr<tool::MemoryPool<Types...>> mm;

  template <typename T>
  std::shared_ptr<T> operator()() {
    if (!mm) {
      if constexpr (std::is_default_constructible_v<T>) {
        return std::make_shared<T>();
      } else {
        throw std::runtime_error(
            "error: fail to create data in communicator task, provide a memory manager to solve this issue.");
      }
    }
    return mm->template getMemory<T>(false);
  }
};

template <typename... Types>
struct ReturnMemory {
  std::shared_ptr<tool::MemoryPool<Types...>> mm;

  template <typename T>
  void operator()(std::shared_ptr<T> data) {
    if (mm) {
      mm->returnMemory(std::move(data));
    }
  }
};

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
  using TypesIds = typename CommunicatorTask<Types...>::TypesIds;

public:
  CommunicatorCoreTask(CommunicatorTask<Types...> *task, comm::CommService *service,
                       std::vector<std::uint32_t> const &receivers, std::string const &name)
      : CommunicatorCoreTaskBase<Types...>(task, name, 1, false),
        communicator_(service, receivers),
        senderDisconnect_(false) {}

  ~CommunicatorCoreTask() {}

public:
  void run() override {
    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    auto receivers = communicator_.receivers();
    bool isReceiver = std::find(receivers.begin(), receivers.end(), communicator_.rank()) != receivers.end();

    if (isReceiver) {
      communicator_.infog(logh::IG::Core, "core", "start receiver");
      this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::recvDeamon, this);
    } else {
      communicator_.infog(logh::IG::Core, "core", "start sender");
      this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::sendDeamon, this);
    }

    taskLoop();

    if (this->deamon_.joinable()) {
      communicator_.infog(logh::IG::Core, "core", "join ", isReceiver ? "receiver" : "sender");
      this->deamon_.join();
    }

    this->postRun();
    this->wakeUp();

    communicator_.infog(logh::IG::CoreTerminate, "core terminate");
  }

private:
  void taskLoop() {
    using namespace std::chrono_literals;
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    std::condition_variable                            sleepCondition;
    bool                                               canTerminate = false;

    communicator_.infog(logh::IG::CoreTaskLoop, "core task loop", "start task loop, canTerminate = ", canTerminate);

    // Actual computation loop
    senderDisconnect_ = false;
    while (!this->canTerminate()) {
      // Wait for a data to arrive or termination
      this->nvtxProfiler()->startRangeWaiting();
      start = std::chrono::system_clock::now();
      canTerminate = this->sleep();
      finish = std::chrono::system_clock::now();
      this->nvtxProfiler()->endRangeWaiting();
      this->incrementWaitDuration(std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start));

      communicator_.infog(logh::IG::CoreTaskLoop, "core task loop", "run task loop, canTerminate = ", canTerminate);

      // If loop can terminate break the loop early
      if (canTerminate) {
        break;
      }

      // Operate the connectedReceivers to get a data and send it to execute
      this->operateReceivers();
    }
    senderDisconnect_ = true;
  }

  void sendDeamon() {
    using namespace std::chrono_literals;
    while (!senderDisconnect_) {
      sendDeamonLoopDbg();
      communicator_.processSendOpsQueue(ReturnMemory<Types...>(mm_));
      std::this_thread::sleep_for(4ms);
    }
    communicator_.processSendOpsQueue(ReturnMemory<Types...>(mm_), true);
    communicator_.sendSignal(communicator_.receivers(), comm::Signal::Disconnect);
    communicator_.infog(logh::IG::SenderDisconnect, "sender disconnect");
    communicator_.processSendOpsQueue(ReturnMemory<Types...>(mm_), true);
    communicator_.infog(logh::IG::SenderEnd, "sender end");
  }

  void recvDeamon() {
    using namespace std::chrono_literals;
    std::vector<Connection> connections = createConnectionVector();
    std::uint32_t           source = 0;
    comm::Signal            signal = comm::Signal::None;
    comm::Header            header = {0, 0, 0, 0, 0, 0};
    char                    bufMem[100] = {0};
    comm::Buffer            buf{bufMem, 100};

    // TODO: empty the queue or flush? I think the best here would be to start a
    //       timer when isConnected is false. After the timer, the thread leaves
    //       the loops and flushes the queue, however, this should be reported
    //       as an error in the dot file.
    while (isConnected(connections) || !communicator_.queues().recvOps.empty()
           || !communicator_.queues().createDataQueue.empty()) {
      communicator_.recvSignal(source, signal, header, buf);

      recvDeamonLoopDbg(connections);
      switch (signal) {
      case comm::Signal::None:
        break;
      case comm::Signal::Disconnect:
        disconnect(connections, source, buf);
        break;
      case comm::Signal::Data:
        ++connections[source].recvCount;
        break;
      }
      communicator_.processRecvDataQueue(GetMemory<Types...>(mm_));
      communicator_.processRecvOpsQueue(ProcessData(this->task()));
      std::this_thread::sleep_for(4ms);
    }
    communicator_.infog(logh::IG::ReceiverEnd, "receiver end");
  }

public:
  struct Connection {
    bool   connected;
    size_t sendCount;
    size_t recvCount;
  };

  std::vector<Connection> createConnectionVector() {
    std::vector<Connection> connections(communicator_.nbProcesses(), Connection{true, 0, 0});
    for (auto receiver : communicator_.receivers()) {
      connections[receiver].connected = false;
    }
    return connections;
  }

  bool isConnected(std::vector<Connection> const &connections) const {
    for (auto connection : connections) {
      if (connection.connected || connection.recvCount < connection.sendCount) {
        return true;
      }
    }
    return false;
  }

  void disconnect(std::vector<Connection> &connections, int source, comm::Buffer &buf) {
    assert(connections[source].connected == true);
    connections[source].connected = false;
    std::memcpy(&connections[source].sendCount, &buf.mem[1], sizeof(size_t));
    std::vector<bool> dbgConnections(connections.size(), false);
    disconnectDbg(connections, source);
  }

public:
  [[nodiscard]] std::string extraPrintingInformation() const override {
    std::string                      infos;
    std::vector<comm::CommTaskStats> stats;

    if (this->mm_) {
      infos += mm_->extraPrintingInformation();
    }

    if (!communicator_.service()->collectStats() || communicator_.nbProcesses() == 1) {
      return infos;
    }

    communicator_.service()->barrier();

    size_t nbProcesses = communicator_.nbProcesses();
    if (communicator_.rank() == 0) {
      stats = communicator_.gatherStats();
      std::map<comm::StorageId, comm::StorageInfo> storageStats;
      size_t                                       maxSendOpsSize = 0;
      size_t                                       maxRecvOpsSize = 0;
      size_t                                       maxCreateDataQueueSize = 0;
      size_t                                       maxSendStorageSize = 0;
      size_t                                       maxRecvStorageSize = 0;
      auto                                         transmissionStats = computeTransmissionStats(stats);

      infos.append("nbProcesses = " + std::to_string(nbProcesses) + "\\l");
      std::string receiversStr = "[" + std::to_string(communicator_.receivers()[0]);
      for (size_t i = 1; i < communicator_.receivers().size(); ++i) {
        receiversStr += ", " + std::to_string(communicator_.receivers()[i]);
      }
      receiversStr += "]";
      strAppend(infos, "receivers = " + receiversStr);

      for (auto const &stat : stats) {
        maxSendOpsSize = std::max(maxSendOpsSize, stat.maxSendOpsSize);
        maxRecvOpsSize = std::max(maxRecvOpsSize, stat.maxRecvOpsSize);
        maxCreateDataQueueSize = std::max(maxCreateDataQueueSize, stat.maxCreateDataQueueSize);
        maxSendStorageSize = std::max(maxSendStorageSize, stat.maxSendStorageSize);
        maxRecvStorageSize = std::max(maxRecvStorageSize, stat.maxRecvStorageSize);
      }
      strAppend(infos, "maxSendOpsSize = " + std::to_string(maxSendOpsSize));
      strAppend(infos, "maxRecvOpsSize = " + std::to_string(maxRecvOpsSize));
      strAppend(infos, "maxCreateDataQueueSize = " + std::to_string(maxCreateDataQueueSize));
      strAppend(infos, "maxSendStorageSize = " + std::to_string(maxSendStorageSize));
      strAppend(infos, "maxRecvStorageSize = " + std::to_string(maxRecvStorageSize));

      for (std::uint8_t typeId = 0; typeId < TypesIds().size; ++typeId) {
        if (!transmissionStats.contains(typeId)) {
          continue;
        }
        type_map::apply(TypesIds(), typeId, [&]<typename T>() {
          infos.append("========== " + hh::tool::typeToStr<T>() + " ==========\n");
        });
        auto transmissionDelays = transmissionStats.at(typeId).transmissionDelays;
        auto packingDelay = transmissionStats.at(typeId).packingDelay;
        auto unpackingDelay = transmissionStats.at(typeId).unpackingDelay;
        auto bandWidth = transmissionStats.at(typeId).bandWidth;
        strAppend(infos, "packing: ", packingDelay, ", (count = ", packingDelay.size(), ")");
        strAppend(infos, "unpacking: ", unpackingDelay, ", (count = ", unpackingDelay.size(), ")");
        strAppend(infos, "bandWidth: ", bandWidth, "MB/s");
        infos.append("transmission: {\\l");
        for (size_t sender = 0; sender < nbProcesses; ++sender) {
          for (size_t receiver = 0; receiver < nbProcesses; ++receiver) {
            if (transmissionDelays[sender * nbProcesses + receiver].empty()) {
              continue;
            }
            strAppend(infos, "    [", sender, " -> ", receiver,
                      "] = ", transmissionDelays[sender * nbProcesses + receiver]);
          }
        }
        strAppend(infos, "}");
      }
    } else {
      communicator_.sendStats();
    }
    // exchange stats
    return infos;
  }

private:
  static std::string durationPrinter(std::chrono::nanoseconds const &ns) {
    std::ostringstream oss;

    // Cast with precision loss
    auto s = std::chrono::duration_cast<std::chrono::seconds>(ns);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(ns);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(ns);

    if (s > std::chrono::seconds::zero()) {
      oss << s.count() << "." << std::setfill('0') << std::setw(3) << (ms - s).count() << "s";
    } else if (ms > std::chrono::milliseconds::zero()) {
      oss << ms.count() << "." << std::setfill('0') << std::setw(3) << (us - ms).count() << "ms";
    } else if (us > std::chrono::microseconds::zero()) {
      oss << us.count() << "." << std::setfill('0') << std::setw(3) << (ns - us).count() << "us";
    } else {
      oss << ns.count() << "ns";
    }
    return oss.str();
  }

  static void strAppend(std::string &str, auto const &...args) {
    std::ostringstream oss;
    (
        [&] {
          if constexpr (std::is_same_v<decltype(args), std::vector<double> const &>) {
            auto avg = computeAvg(args);
            oss << avg.first << " +- " << avg.second;
          } else if constexpr (std::is_same_v<decltype(args), std::vector<std::chrono::nanoseconds> const &>) {
            auto avg = computeAvgDuration(args);
            oss << durationPrinter(avg.first) << " +- " << durationPrinter(avg.second);
          } else {
            oss << args;
          }
        }(),
        ...);
    str.append(oss.str() + "\\l");
  }

  static std::pair<std::chrono::nanoseconds, std::chrono::nanoseconds>
  computeAvgDuration(std::vector<std::chrono::nanoseconds> nss) {
    if (nss.size() == 0) {
      return {std::chrono::nanoseconds::zero(), std::chrono::nanoseconds::zero()};
    }
    std::chrono::nanoseconds sum = std::chrono::nanoseconds::zero(), mean = std::chrono::nanoseconds::zero();
    double                   sd = 0;

    for (auto ns : nss) {
      sum += ns;
    }
    mean = sum / (nss.size());

    for (auto ns : nss) {
      auto diff = (double)(ns.count() - mean.count());
      sd += diff * diff;
    }
    return {mean, std::chrono::nanoseconds((int64_t)std::sqrt(sd / (double)nss.size()))};
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
    std::vector<std::chrono::nanoseconds>              packingDelay;
    std::vector<std::chrono::nanoseconds>              unpackingDelay;
    std::vector<std::vector<std::chrono::nanoseconds>> transmissionDelays;
    std::vector<double>                                bandWidth;
  };

  std::map<std::uint8_t, TransmissionStat>
  computeTransmissionStats(std::vector<comm::CommTaskStats> const &stats) const {
    std::map<std::uint8_t, TransmissionStat> transmissionStats;
    size_t                                         nbProcesses = communicator_.nbProcesses();

    for (auto receiverRank : communicator_.receivers()) {
      for (auto recvStorageStat : stats[receiverRank].storageStats) {
        comm::StorageId     storageId = recvStorageStat.first;
        comm::StorageInfo   recvInfos = recvStorageStat.second;
        std::uint32_t source = storageId.source;
        std::uint8_t  typeId = recvInfos.typeId;

        if (stats[source].storageStats.contains(storageId)) {
          auto sendInfos = stats[source].storageStats.at(storageId);
          if (!transmissionStats.contains(typeId)) {
            transmissionStats.insert({
                typeId,
                TransmissionStat{
                    .packingDelay = {},
                    .unpackingDelay = {},
                    .transmissionDelays = std::vector<std::vector<std::chrono::nanoseconds>>(nbProcesses * nbProcesses),
                    .bandWidth = {},
                },
            });
          }
          auto delay_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(recvInfos.recvtp - sendInfos.sendtp);
          transmissionStats.at(typeId).transmissionDelays[source * nbProcesses + receiverRank].push_back(delay_ns);
          transmissionStats.at(typeId).packingDelay.push_back(sendInfos.packingTime);
          transmissionStats.at(typeId).unpackingDelay.push_back(recvInfos.unpackingTime);
          double dataSizeMB = sendInfos.dataSize / (1024. * 1024.);
          double delay_s = delay_ns.count() / 1'000'000'000.;
          transmissionStats.at(typeId).bandWidth.push_back(dataSizeMB / delay_s);
        }
      }
    }
    return transmissionStats;
  }

public:
  void setMemoryManager(std::shared_ptr<tool::MemoryPool<Types...>> mm) {
    this->mm_ = mm;
  }

  comm::TaskCommunicator<TypesIds> *comm() {
    return &communicator_;
  }

private:
  std::thread                                 deamon_;
  std::shared_ptr<tool::MemoryPool<Types...>> mm_;
  comm::TaskCommunicator<TypesIds>            communicator_;
  bool                                        senderDisconnect_;

private:
  std::vector<bool> connectionsDbg(std::vector<Connection> const &connections) const {
    std::vector<bool> dbgConnections(connections.size(), false);
    for (size_t i = 0; i < connections.size(); ++i) {
      auto connection = connections[i];
      if (connection.connected || connection.recvCount < connection.sendCount) {
        dbgConnections[i] = true;
      }
    }
    return dbgConnections;
  }

  void sendDeamonLoopDbg() {
    static size_t dbg_idx = 0;
    if (dbg_idx++ == 1000) {
      dbg_idx = 0;
      logh::warn("sender still running: channel = ", (int)communicator_.channel(),
                 ", rank = ", communicator_.rank(), ", queue size = ", communicator_.queues().sendOps.size(),
                 ", hasNotifierConnected = ", this->hasNotifierConnected());
    }
  }

  void recvDeamonLoopDbg(std::vector<Connection> const &connections) {
    static size_t dbg_idx = 0;
    if (dbg_idx++ == 4000) {
      dbg_idx = 0;

      if (isConnected(connections)) {
        logh::warn(
            "reciever still running: channel = ", (int)communicator_.channel(), ", rank = ", communicator_.rank(),
            ", data queue size = ", communicator_.queues().createDataQueue.size(),
            ", ops queue size = ", communicator_.queues().recvOps.size(),
            ", connections = ", connectionsDbg(connections), ", hasNotifierConnected = ", this->hasNotifierConnected());
      } else {
        logh::error("non connected receiver still running: channel = ", (int)communicator_.channel(),
                    ", rank = ", communicator_.rank(), ", ops queue size = ", communicator_.queues().recvOps.size(),
                    ", data queue size = ", communicator_.queues().createDataQueue.size());
      }
    }
  }

  void disconnectDbg(std::vector<Connection> const &connections, int source) {
    communicator_.infog(logh::IG::ReceiverDisconnect, "receiver disconnect", "source = ", source,
                         ", connections = ", connectionsDbg(connections));
  }
};

} // end namespace core

} // end namespace hh

#endif
