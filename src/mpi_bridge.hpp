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

template <typename TaskType, typename TypesIds, typename Input>
struct MPIBridgeExecute : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  int graphId_ = -1;
  int taskId_ = -1;
  std::vector<int> receiversRanks_;
  int rankIdx_ = 0;
  TaskType *task_ = nullptr;

  // TODO: what if a custom serializer is used?
  template <typename Table = serializer::tools::TypeTable<>>
  using Serializer = serializer::Serializer<serializer::Bytes, Table>;

public:
  MPIBridgeExecute(TaskType *task, int taskId, std::vector<int> const &receivers_ranks)
      : taskId_(taskId), receiversRanks_(receivers_ranks), task_(task) {}

  void execute(std::shared_ptr<Input> data) override {
    int receiverRank = receiversRanks_[rankIdx_];
    rankIdx_ = (rankIdx_ + 1) % receiversRanks_.size();
    if (receiverRank == getMPIRank()) {
        // use add result when we are in the same process
        task_->addResult(data);
    } else {
        sendData<Serializer<TypesIds>>({receiverRank}, graphId_, taskId_, data);
    }
  }

  void updateReceiversRanks(std::vector<int> ranks) { receiversRanks_ = ranks; }

  void graphId(int graphId) { this->graphId_ = graphId; }
};

template <typename TasType, typename TypeTable, typename... Inputs> struct MPIBridgeMultiExecute;

template <typename TaskType, typename TypeTable, typename... Inputs>
struct MPIBridgeMultiExecute<TaskType, TypeTable, std::tuple<Inputs...>> : MPIBridgeExecute<TaskType, TypeTable, Inputs>... {
  MPIBridgeMultiExecute(TaskType *task, int taskId, std::vector<int> const &receivers_ranks)
      : MPIBridgeExecute<TaskType, TypeTable, Inputs>(task, taskId, receivers_ranks)... {}

  void updateAllReceiversRanks(std::vector<int> ranks) {
    (static_cast<MPIBridgeExecute<TaskType, TypeTable, Inputs> *>(this)->updateReceiversRanks(ranks), ...);
  }

  void updateAllGraphIds(int graphId) {
    (static_cast<MPIBridgeExecute<TaskType, TypeTable, Inputs> *>(this)->graphId(graphId), ...);
  }
};

template <typename... Types>
class MPIBridge : public behavior::TaskNode,
                  public behavior::CanTerminate,
                  public behavior::Cleanable,
                  public behavior::Copyable<MPIBridge<Types...>>,
                  public tool::BehaviorMultiReceiversTypeDeducer_t<std::tuple<Types...>>,
                  public MPIBridgeMultiExecute<MPIBridge<Types...>, serializer::tools::TypeTable<Types...>, std::tuple<Types...>>,
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
        MPIBridgeMultiExecute<MPIBridge<Types...>, TypesIds, Inputs>(this, taskId, receiversRanks),
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

  [[nodiscard]] int taskId() const { return taskId_; }

  void setGraphId() {
    using MPIBME = MPIBridgeMultiExecute<MPIBridge<Types...>, TypesIds, std::tuple<Types...>>;
    static_cast<MPIBME*>(this)->updateAllGraphIds(this->graphId());
  }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  [[nodiscard]] std::vector<int> const &receiversRanks() const { return receiversRanks_; }

  std::shared_ptr<MPIBridge<Types...>> copy() override final {
      throw std::runtime_error("error: the MPIBridge should not be copied.");
      // return std::make_shared<MPIBridge<Types...>>(taskId_, receiversRanks_,
      //         this->name());
  }

  using
      tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
