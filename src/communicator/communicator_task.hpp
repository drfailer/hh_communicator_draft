#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "comm_tools.hpp"
#include "communicator_core_task.hpp"
#include <hedgehog/hedgehog.h>

namespace hh {

template <typename TaskType, typename TypesIds, typename Input>
struct CommunicatorSend : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  size_t rankIdx_ = 0;
  TaskType *task_ = nullptr;

public:
  CommunicatorSend(TaskType *task) : task_(task) {}

  void execute(std::shared_ptr<Input> data) override {
    int receiverRank = task_->receiversRanks()[rankIdx_];
    rankIdx_ = (rankIdx_ + 1) % task_->receiversRanks().size();
    if (receiverRank == task_->commHandle()->rank) {
      task_->addResult(data);
    } else {
      comm::sendData<TypesIds>({receiverRank}, task_->graphId(), task_->taskId(), data);
    }
  }
};

template <typename TasType, typename TypeTable, typename... Inputs> struct CommunicatorMultiSend;

template <typename TaskType, typename TypeTable, typename... Inputs>
struct CommunicatorMultiSend<TaskType, TypeTable, std::tuple<Inputs...>>
    : CommunicatorSend<TaskType, TypeTable, Inputs>... {
  CommunicatorMultiSend(TaskType *task) : CommunicatorSend<TaskType, TypeTable, Inputs>(task)... {}
};

template <typename... Types>
class CommunicatorTask
    : public behavior::TaskNode,
      public behavior::CanTerminate,
      public behavior::Cleanable,
      public behavior::Copyable<CommunicatorTask<Types...>>,
      public tool::BehaviorMultiReceiversTypeDeducer_t<std::tuple<Types...>>,
      public CommunicatorMultiSend<CommunicatorTask<Types...>, comm::TypeTable<Types...>, std::tuple<Types...>>,
      public tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>> {
private:
  using TypesIds = comm::TypeTable<Types...>;
  using CoreTaskType = core::CommunicatorCoreTask<Types...>;
  using SelfType = CommunicatorTask<Types...>;
  using Inputs = std::tuple<Types...>;
  using Outputs = std::tuple<Types...>;
  friend CoreTaskType;

private:
  std::shared_ptr<CoreTaskType> const coreTask_ = nullptr;
  std::vector<int> receiversRanks_ = {};
  int taskId_ = -1;
  inline static size_t idGenerator_ = 0;
  comm::CommHandle *commHandle_;

public:
  explicit CommunicatorTask(comm::CommHandle *commHandle, std::vector<int> const &receiversRanks,
                            std::string const &name = "CommunicatorTask")
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, name)), behavior::Copyable<SelfType>(1),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        CommunicatorMultiSend<CommunicatorTask<Types...>, TypesIds, Inputs>(this), receiversRanks_(receiversRanks),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())), taskId_(++commHandle->idGenerator), commHandle_(commHandle) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
  }

  ~CommunicatorTask() override = default;

  [[nodiscard]] size_t graphId() const { return coreTask_->graphId(); }

  [[nodiscard]] int taskId() const { return taskId_; }

  [[nodiscard]] comm::CommHandle *commHandle() const { return commHandle_; }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  [[nodiscard]] std::vector<int> const &receiversRanks() const { return receiversRanks_; }

  std::shared_ptr<CommunicatorTask<Types...>> copy() override final {
    throw std::runtime_error("error: the communicator task should not be copied.");
  }

  using tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
