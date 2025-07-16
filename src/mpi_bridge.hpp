#ifndef TASK_MULTI_GPU_TASK
#define TASK_MULTI_GPU_TASK
#include "log.hpp"
#include "mpi_bridge_core.hpp"
#include "serializer/tools/type_table.hpp"
#include <hedgehog/hedgehog.h>
#include "mpi.hpp"
#include <mpi.h>
#include <serializer/serializer.hpp>
#include <serializer/tools/macros.hpp>

// Refactor:
// - new idea: we should have a foreign task that runs on a separated node; this
//   way we would truely have a distributed graph

// Interface:
// - A task can inherit from MPITask
// - when the `addResult` method is used, the data is serialized an transmitted
//   to all the receivers
// - when data is received, the buffer is deserialized and the corresponding
//   `execute` method is called
//
// Requirements:
// - we should know the senders
// - all rank communications -> in that case, we need a package id to make sure
//   that the received package is processed only if the package sender is
//   connected to the task
//
// Issues:
// - How does it work if the current task is multi-threaded? (need to rewrite
//   some HH core components in order for it to work)
//
// Plan:
//
// Send data:
// - serialize the data
// - add the sender ID
// - broadcast
//
// Receive data:
// - wait for the reception from any rank
// - if the package does not come from one of my senders: continue
// - deserialize the data
// - transmit the data to the next task and continue waiting

// The idea is to have a class that starts up automatically and runs
// indefinitely until it receives the end signal. When a execute function runs,
// it serializes the objects and sends it to another process. The other process
// receives it, deserializes the data and add the result like a normal task.
//
// We need two threads and two execute functions:
// - one thread will wait for data to be sent
// - the second one will be stuck in waiting data to arrive (that would be great
//   to rewrite the receiver for this)
//
//
// All the nodes should run the same graph. In receiver mode the bridge waits
// for data to be received (it never leaves the execute function until the end
// of the execution), in sender mode, it acts like a normal taks the sends data
// using MPI instead of addResult.
//
// WARN:
// - only the main process should send data to the graph
// - the other processes are only triggerd by the bridges:
//   - some data commes through the bridge from another process and the
//     execution is done in the current process until we found another bridge
//     that sends new data to other processes.
//   - only a portion of the graph will be executed!

namespace hh {

template <typename TypeTable, typename Input>
struct MPIBridgeExecute : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  std::vector<int> receiversRanks_;

public:
  MPIBridgeExecute(std::vector<int> const &receivers_ranks) : receiversRanks_(receivers_ranks) {}

  void execute(std::shared_ptr<Input> data) override {
    if (data == nullptr) {
      return; // return on automatic start
    }
    serializer::Bytes buffer(1024);

    auto id = serializer::tools::getId<Input>(TypeTable());
    buffer.append(0, reinterpret_cast<std::byte*>(&id), sizeof(id));
    buffer.append(sizeof(id), reinterpret_cast<std::byte*>(data.get()), sizeof(int));
    // serializer::serializeWithId<serializer::Serializer<serializer::Bytes, TypeTable>, Input>(buffer, 0, *data);
    for (auto receiver_rank : receiversRanks_) {
      INFO("sending package to " << receiver_rank << " (rank = " << mpi_rank() << ").");
      MPI_Send(buffer.data(), buffer.size(), MPI_BYTE, receiver_rank, 0, MPI_COMM_WORLD);
    }
  }

  void updateReceiversRanks(std::vector<int> ranks) { receiversRanks_ = ranks; }
};

template <typename TypeTable, typename... Inputs> struct MPIBridgeMultiExecute;

template <typename TypeTable, typename... Inputs>
struct MPIBridgeMultiExecute<TypeTable, std::tuple<Inputs...>> : MPIBridgeExecute<TypeTable, Inputs>... {
  MPIBridgeMultiExecute(std::vector<int> const &receivers_ranks)
      : MPIBridgeExecute<TypeTable, Inputs>(receivers_ranks)... {}

  void update_ranks(std::vector<int> ranks) {
    (static_cast<MPIBridgeExecute<TypeTable, Inputs> *>(this)->updateReceiversRanks(ranks), ...);
  }
};

template <typename... Types>
class MPIBridge : public behavior::TaskNode,
                  public behavior::CanTerminate,
                  public behavior::Cleanable,
                  public behavior::Copyable<MPIBridge<Types...>>,
                  public tool::BehaviorMultiReceiversTypeDeducer_t<std::tuple< Types...>>,
                  public MPIBridgeMultiExecute<serializer::tools::TypeTable<Types...>, std::tuple<Types...>>,
                  public tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>> {
private:
  using TypesIDs = serializer::tools::TypeTable<Types...>;
  using CoreTaskType = core::MPIBridgeCore<Types...>;
  using SelfType = MPIBridge<Types...>;
  using Inputs = std::tuple<Types...>;
  using Outputs = std::tuple<Types...>;
  std::shared_ptr<CoreTaskType> const coreTask_ = nullptr;
  std::vector<int> receiversRanks_ = {};

  friend CoreTaskType;

public:
  explicit MPIBridge(std::vector<int> const &receiversRanks = {}, std::string const &name = "MPIBridge")
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, receiversRanks, name)),
        behavior::Copyable<SelfType>(1),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        MPIBridgeMultiExecute<TypesIDs, Inputs>(receiversRanks), receiversRanks_(receiversRanks),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
  }

  ~MPIBridge() override = default;

  [[nodiscard]] size_t graphId() const { return coreTask_->graphId(); }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  [[nodiscard]] std::vector<int> const &receiversRanks() const { return receiversRanks_; }

protected:
  std::shared_ptr<CoreTaskType> const &coreTask() const { return coreTask_; }

  [[nodiscard]] int deviceId() const { return coreTask_->deviceId(); }
};
} // namespace hh

#endif
