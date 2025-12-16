#ifndef COMMUNICATOR_COMMUNICATOR_CORE_TASK
#define COMMUNICATOR_COMMUNICATOR_CORE_TASK
#include "../log.hpp"
#include "communicator.hpp"
#include "communicator_memory_manager.hpp"
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
  using TM = typename CommunicatorTask<Types...>::TM;

public:
  CommunicatorCoreTask(CommunicatorTask<Types...> *task, comm::CommService *service, std::string const &name)
      : CommunicatorCoreTaskBase<Types...>(task, name, 1, false),
        communicator_(service),
        senderDisconnect_(false) {}

  ~CommunicatorCoreTask() {}

public:
  struct Connection {
    bool   connected;
    size_t sendCount;
    size_t recvCount;
  };

  std::vector<Connection> createConnectionVector() {
    std::vector<Connection> connections(communicator_.nbProcesses(), Connection{true, 0, 0});
    connections[communicator_.rank()].connected = false;
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
  }

public:
  void run() override {
    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    this->senderDisconnect_ = false;
    this->deamon_ = std::thread(&CommunicatorCoreTask<Types...>::networkDeamon, this);

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
    senderDisconnect_ = true;
  }

  enum class PortState {
    Opened,
    Closing,
    Closed,
  };

  void networkDeamon() {
    using namespace std::chrono_literals;
    auto                    inputPortState = PortState::Opened;
    auto                    outputPortState = PortState::Opened;
    std::vector<Connection> connections = createConnectionVector();

    while (inputPortState != PortState::Closed || outputPortState != PortState::Closed) {
      processInputPort(inputPortState);
      processOutputPort(outputPortState, connections);
      std::this_thread::sleep_for(4ms);
    }
  }

  void processInputPort(PortState &state) {
    switch (state) {
    case PortState::Opened:
      communicator_.processSendOpsQueue(ReturnMemory<Types...>(mm_));
      if (senderDisconnect_) {
        state = PortState::Closing;
      }
      break;
    case PortState::Closing:
      communicator_.processSendOpsQueue(ReturnMemory<Types...>(mm_), true);
      communicator_.notifyDisconnection();
      communicator_.processSendOpsQueue(ReturnMemory<Types...>(mm_), true);
      state = PortState::Closed;
      break;
    case PortState::Closed:
      break;
    }
  }

  void processOutputPort(PortState &state, std::vector<Connection> &connections) {
    comm::Signal signal = comm::Signal::None;
    comm::Header header = {0, 0, 0, 0, 0, 0};
    char         bufMem[100] = {0};
    comm::Buffer buf{bufMem, 100};
    switch (state) {
    case PortState::Opened:
      communicator_.recvSignal(signal, header, buf);

      switch (signal) {
      case comm::Signal::None:
        break;
      case comm::Signal::Disconnect:
        disconnect(connections, header.source, buf);
        break;
      case comm::Signal::Data:
        ++connections[header.source].recvCount;
        break;
      }
      communicator_.processRecvDataQueue(GetMemory<Types...>(mm_));
      communicator_.processRecvOpsQueue(ProcessData(this->task()));

      if (!isConnected(connections) && !communicator_.hasPendingOperations()) {
        state = PortState::Closing;
      }
      break;
    case PortState::Closing:
      communicator_.flushRecvQueueAndWarehouse();
      state = PortState::Closed;
      break;
    case PortState::Closed:
      break;
    }
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
                                                                          communicator_.nbProcesses());
    } else {
      communicator_.sendStats();
    }
    return infos;
  }

public:
  void setMemoryManager(std::shared_ptr<tool::MemoryPool<Types...>> mm) {
    this->mm_ = mm;
  }

  comm::Communicator<TM> *comm() {
    return &communicator_;
  }

private:
  std::thread                                 deamon_;
  std::shared_ptr<tool::MemoryPool<Types...>> mm_;
  comm::Communicator<TM>                      communicator_;
  bool                                        senderDisconnect_;
};

} // end namespace core

} // end namespace hh

#endif
