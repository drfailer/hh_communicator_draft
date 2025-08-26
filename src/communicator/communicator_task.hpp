#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "comm_tools.hpp"
#include "communicator_core_task.hpp"
#include <functional>
#include <hedgehog/hedgehog.h>

namespace hh {

template <typename TaskType, typename TypesIds, typename Input>
struct CommunicatorSend : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  size_t rankIdx_ = 0;
  TaskType *task_ = nullptr;
  bool distribute_ = true;
  bool isReceiver_ = false;
  std::vector<int> otherReceivers_ = {};
  std::function<void(std::shared_ptr<Input>)> preSendCB_ = nullptr;

public:
  CommunicatorSend(TaskType *task, bool distribute) : task_(task), distribute_(distribute) {}

  void execute(std::shared_ptr<Input> data) override {
    if (preSendCB_) {
      preSendCB_(data);
    }

    if (distribute_) {
      int receiver = task_->comm()->receivers[rankIdx_];
      rankIdx_ = (rankIdx_ + 1) % task_->comm()->receivers.size();
      if (receiver == task_->comm()->handle->rank) {
        task_->addResult(data);
      } else {
        comm::commSendData<Input>(task_->comm(), {receiver}, data);
      }
    } else {
      if (isReceiver_) {
        this->task_->addResult(data);
      }
      comm::commSendData(this->task_->comm(), otherReceivers_, data);
    }
  }

  void initialize() {
      for (auto receiver : task_->comm()->receivers) {
        if (receiver == task_->comm()->handle->rank) {
          isReceiver_ = true;
        } else {
          otherReceivers_.push_back(receiver);
        }
      }
  }

  void preSendCB(std::function<void(std::shared_ptr<Input>)> cb) { preSendCB_ = cb; }
};

template <typename TasType, typename TypeTable, typename... Inputs> struct CommunicatorMultiSend;

template <typename TaskType, typename TypeTable, typename... Inputs>
struct CommunicatorMultiSend<TaskType, TypeTable, std::tuple<Inputs...>>
    : CommunicatorSend<TaskType, TypeTable, Inputs>... {
  CommunicatorMultiSend(TaskType *task, bool distribute)
      : CommunicatorSend<TaskType, TypeTable, Inputs>(task, distribute)... {}

  template <typename Input> void preSendCB(std::function<void(std::shared_ptr<Input>)> cb) {
    CommunicatorSend<TaskType, TypeTable, Input>::preSendCB(cb);
  }

  void initialize() {
    (CommunicatorSend<TaskType, TypeTable, Inputs>::initialize(), ...);
  }
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
  comm::CommTaskHandle<TypesIds> commHandle_;

public:
  explicit CommunicatorTask(comm::CommHandle *commHandle, std::vector<int> const &receivers,
                            bool distribute = true, std::string const &name = "CommunicatorTask")
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, name)), behavior::Copyable<SelfType>(1),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        CommunicatorMultiSend<CommunicatorTask<Types...>, TypesIds, Inputs>(this, distribute),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())),
        commHandle_(comm::commTaskHandleCreate<TypesIds>(commHandle, receivers)) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
  }

  ~CommunicatorTask() override = default;

  void initialize() override {
    CommunicatorMultiSend<CommunicatorTask<Types...>, TypesIds, Inputs>::initialize();
  }

  [[nodiscard]] comm::CommTaskHandle<TypesIds> *comm() { return &commHandle_; }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  std::shared_ptr<CommunicatorTask<Types...>> copy() override final {
    throw std::runtime_error("error: the communicator task should not be copied.");
  }

  using tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
