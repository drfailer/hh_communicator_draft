#ifndef TASK_MULTI_GPU_TASK
#define TASK_MULTI_GPU_TASK
#include "log.hpp"
#include "mpi.hpp"
#include "mpi_bridge_core.hpp"
#include <hedgehog/hedgehog.h>
#include <mpi.h>
#include <serializer/serializer.hpp>
#include <serializer/tools/macros.hpp>

namespace hh {

template <typename TypesIds, typename Input>
struct MPIBridgeExecute : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  int taskId_ = -1;
  std::vector<int> receiversRanks_;

  // TODO: what if a custom serializer is used?
  template <typename Table = serializer::tools::TypeTable<>>
  using Serializer = serializer::Serializer<serializer::Bytes, Table>;

public:
  MPIBridgeExecute(int taskId, std::vector<int> const &receivers_ranks)
      : taskId_(taskId), receiversRanks_(receivers_ranks) {}

  void execute(std::shared_ptr<Input> data) override {
    namespace ser = serializer;
    ser::Bytes buffer(1024);
    size_t pos = ser::serialize<Serializer<>>(buffer, 0, taskId_);
    DBG(pos);
    pos = ser::serializeWithId<Serializer<TypesIds>, Input>(buffer, pos, data);
    DBG(pos);
    DBG(buffer.size());

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
  MPIBridgeMultiExecute(int taskId, std::vector<int> const &receivers_ranks)
      : MPIBridgeExecute<TypeTable, Inputs>(taskId, receivers_ranks)... {}

  void update_ranks(std::vector<int> ranks) {
    (static_cast<MPIBridgeExecute<TypeTable, Inputs> *>(this)->updateReceiversRanks(ranks), ...);
  }
};

template <typename... Types>
class MPIBridge : public behavior::TaskNode,
                  public behavior::CanTerminate,
                  public behavior::Cleanable,
                  public behavior::Copyable<MPIBridge<Types...>>,
                  public tool::BehaviorMultiReceiversTypeDeducer_t<std::tuple<Types...>>,
                  public MPIBridgeMultiExecute<serializer::tools::TypeTable<Types...>, std::tuple<Types...>>,
                  public tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>> {
private:
  using TypesIds = serializer::tools::TypeTable<Types...>;
  using CoreTaskType = core::MPIBridgeCore<Types...>;
  using SelfType = MPIBridge<Types...>;
  using Inputs = std::tuple<Types...>;
  using Outputs = std::tuple<Types...>;
  friend CoreTaskType;

private:
  std::shared_ptr<CoreTaskType> const coreTask_ = nullptr;
  std::vector<int> receiversRanks_ = {};
  int taskId_ = -1;
  inline static size_t idGenerator_ = 0;

private:
  explicit MPIBridge(int taskId, std::vector<int> const &receiversRanks, std::string const &name)
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, taskId, receiversRanks, name)),
        behavior::Copyable<SelfType>(1),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        MPIBridgeMultiExecute<TypesIds, Inputs>(taskId, receiversRanks),
        receiversRanks_(receiversRanks),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())),
        taskId_(taskId) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
  }

public:
  explicit MPIBridge(std::vector<int> const &receiversRanks, std::string const &name = "MPIBridge")
      : MPIBridge(++idGenerator_, receiversRanks, name) {}

  ~MPIBridge() override = default;

  [[nodiscard]] size_t graphId() const { return coreTask_->graphId(); }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  [[nodiscard]] std::vector<int> const &receiversRanks() const { return receiversRanks_; }

  std::shared_ptr<MPIBridge<Types...>> copy() override final {
      throw std::runtime_error("error: the MPIBridge should not be copied.");
      // return std::make_shared<MPIBridge<Types...>>(taskId_, receiversRanks_,
      //         this->name());
  }
};
} // namespace hh

#endif
