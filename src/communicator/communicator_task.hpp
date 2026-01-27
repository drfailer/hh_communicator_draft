#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "communicator.hpp"
#include "communicator_core_task.hpp"
#include <functional>
#include <hedgehog/hedgehog.h>

namespace hh {

// /!\ the sender list will not contain ranks that both send and receive

template <typename T>
using SendStrategy = std::function<std::vector<comm::rank_t>(std::shared_ptr<T>)>;

template <typename TaskType, typename TM, typename Input>
struct CommunicatorSend : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  TaskType            *task_ = nullptr;
  SendStrategy<Input>  strategy_ = nullptr;

public:
  CommunicatorSend(TaskType *task)
      : task_(task) {}

  void execute(std::shared_ptr<Input> data) override {
    logh::infog(logh::IG::CommunicatorTaskExecute, "communicator task execute", "[", (int)task_->comm()->channel(),
                "]: rank = ", task_->comm()->service()->rank());
    if constexpr (requires { data->preSend(); }) {
      data->preSend();
    }
    auto dests = this->strategy_(data);
    if (dests.size() == 1 && dests[0] == this->task_->comm()->rank()) {
      this->task_->addResult(data);
    } else {
      this->task_->comm()->sendData(dests, data);
    }
  }

  void initialize() {}

  void strategy(SendStrategy<Input> cb) {
    strategy_ = cb;
  }
};

template <typename TasType, typename TM, typename... Inputs>
struct CommunicatorMultiSend;

template <typename TaskType, typename TM, typename... Inputs>
struct CommunicatorMultiSend<TaskType, TM, std::tuple<Inputs...>> : CommunicatorSend<TaskType, TM, Inputs>... {
  CommunicatorMultiSend(TaskType *task)
      : CommunicatorSend<TaskType, TM, Inputs>(task)... {}

  template <typename Input>
  void strategy(SendStrategy<Input> cb) {
    ((CommunicatorSend<TaskType, TM, Input> *)this)->strategy(cb);
  }

  void initialize() {
    (((CommunicatorSend<TaskType, TM, Inputs> *)this)->initialize(), ...);
  }
};

template <typename... Types>
class CommunicatorTask
    : public behavior::TaskNode,
      public behavior::CanTerminate,
      public behavior::Cleanable,
      public behavior::Copyable<CommunicatorTask<Types...>>,
      public tool::BehaviorMultiReceiversTypeDeducer_t<std::tuple<Types...>>,
      public CommunicatorMultiSend<CommunicatorTask<Types...>, comm::TypeMap<Types...>, std::tuple<Types...>>,
      public tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>> {
private:
  using TM = comm::TypeMap<Types...>;
  using CoreTaskType = core::CommunicatorCoreTask<Types...>;
  using SelfType = CommunicatorTask<Types...>;
  using Inputs = std::tuple<Types...>;
  using Outputs = std::tuple<Types...>;
  friend CoreTaskType;

private:
  std::shared_ptr<CoreTaskType> const coreTask_ = nullptr;

public:
  explicit CommunicatorTask(comm::CommService *service, std::string const &name = "CommunicatorTask")
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, service, name)),
        behavior::Copyable<SelfType>(1),
        CommunicatorMultiSend<CommunicatorTask<Types...>, TM, Inputs>(this),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
    this->coreTask_->printOptions().background({0x12, 0x34, 0x56, 0xff});
    this->coreTask_->printOptions().font({0xff, 0xff, 0xff, 0xff});
  }

  void initialize() override {
    CommunicatorMultiSend<CommunicatorTask<Types...>, TM, Inputs>::initialize();
  }

  [[nodiscard]] comm::Communicator<Types...> *comm() const {
    return coreTask_->comm();
  }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  std::shared_ptr<CommunicatorTask<Types...>> copy() override final {
    throw std::runtime_error("error: the communicator task should not be copied.");
  }

  template <typename MM>
  void setMemoryManager(MM mm) {
    this->coreTask_->setMemoryManager(std::make_shared<comm::tool::MemoryManager<Types...>>(mm));
  }

  using tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
