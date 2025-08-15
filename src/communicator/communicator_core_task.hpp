#ifndef COMMUNICATOR_COMMUNICATOR_CORE_TASK
#define COMMUNICATOR_COMMUNICATOR_CORE_TASK
#include "comm_tools.hpp"
#include "generic_core_task.hpp"

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

    // TODO: add a way to implement a 2 threads version in case a process has to
    // operate both as sender and receiver
    if (std::find(this->task()->receiversRanks().begin(), this->task()->receiversRanks().end(),
                  this->task()->commHandle()->rank) != this->task()->receiversRanks().end()) {
      runAsReceiver();
    } else {
      runAsSender();
      sendSignal({this->task()->receiversRanks()}, this->graphId(), this->task()->taskId(),
                 comm::CommSignal::Disconnect);
    }

    // Do the shutdown phase
    this->postRun();
    // Wake up a node that this node is linked to
    this->wakeUp();
  }

private:
  void runAsReceiver() {
    // TODO: we need to be able to resize this buffer dynamically, the easiest way would be to send the buffer size in
    // the request
    comm::Buffer buf = comm::bufferCreate(1024, 1024);
    std::vector<bool> connections(this->task()->commHandle()->nbProcesses, true);

    // each receiver is connected to all the senders
    for (auto receiverRank : this->task()->receiversRanks()) {
      connections[receiverRank] = false;
    }

    while (true) {
      int graphId = -1, senderId = -1, senderRank = -1;
      comm::CommSignal signal;
      comm::recvSignal(buf, senderRank, graphId, senderId, signal);

      if (graphId != (int)this->graphId()) {
        continue;
      }

      if (senderId != this->task()->taskId()) {
        if (senderId == 0) {
          if (signal == comm::CommSignal::Terminate) {
            break;
          }
        }
        continue;
      }

      if (signal == comm::CommSignal::Terminate) {
        break;
      } else if (signal == comm::CommSignal::Disconnect) {
        connections[senderRank] = false;
        if (!isConnected(connections)) {
          break;
        }
        continue;
      } else if (signal != comm::CommSignal::Data) {
        continue;
      }

      comm::unpackData<TypesIds>(buf, [&]<typename T>(std::shared_ptr<T> data) {
          this->task()->addResult(data);
      });
    }
  }

  void runAsSender() {
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    volatile bool canTerminate = false;

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
};

} // end namespace core

} // end namespace hh

#endif
