#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "comm_tools.hpp"
#include "communicator_core_task.hpp"
#include <functional>
#include <hedgehog/hedgehog.h>

namespace hh {

// /!\ the sender list will not contain ranks that both send and receive

struct CommunicatorTaskOpt {
  bool sendersAreReceivers = false; // also transmit the data to the current node
  bool scatter = true; // scatter the data between receivers, or send the same data to all
};

template <typename T>
using DestCBType = std::function<std::vector<int>(std::shared_ptr<T>)>;

template <typename TaskType, typename TypesIds, typename Input>
struct CommunicatorSend : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  size_t                                      rankIdx_ = 0;
  TaskType                                   *task_ = nullptr;
  bool                                        isReceiver_ = false;
  std::vector<int>                            receivers_;
  std::function<void(std::shared_ptr<Input>)> preSendCB_ = nullptr;
  DestCBType<Input>                           destCB_ = nullptr;

public:
  CommunicatorSend(TaskType *task)
      : task_(task) {}

  void execute(std::shared_ptr<Input> data) override {
    logh::infog(logh::IG::CommunicatorTaskExecute, "communicator task execute", "[", (int)task_->comm()->channel,
                "]: rank = ", task_->comm()->comm->rank, ", isReceiver_ = ", isReceiver_);
    bool returnMemory = true;
    if (preSendCB_) {
      preSendCB_(data);
    }

    /*
     * The CommunicatorTask has the following behavior:
     * - if the rank is a receiver, then just transmit input data (we stay on one node)
     * - if the rank is a sender:
     *   - if scatter then scatter the data between the receivers (and optionally the current rank)
     *   - else, send the data to all the receivers (and optionally the current rank)
     */
    if (isReceiver_) {
      task_->addResult(data);
    } else {
      if (destCB_) {
        auto dests = destCB_(data);
        auto rankIt = std::find(dests.begin(), dests.end(), task_->comm()->comm->rank);

        if (rankIt != dests.end()) {
          task_->addResult(data);
          dests.erase(rankIt);
          returnMemory = false;
        }
        if (!dests.empty()) {
          comm::commSendData<Input>(task_->comm(), dests, data, returnMemory);
        }
      } else {
        if (task_->options().scatter) {
          int receiver = receivers_[rankIdx_];
          rankIdx_ = (rankIdx_ + 1) % receivers_.size();
          if (receiver == task_->comm()->comm->rank) {
            task_->addResult(data);
          } else {
            comm::commSendData<Input>(task_->comm(), {receiver}, data, returnMemory);
          }
        } else {
          if (task_->options().sendersAreReceivers) {
            task_->addResult(data);
            returnMemory = false;
          }
          comm::commSendData(task_->comm(), receivers_, data, returnMemory);
        }
      }
    }
  }

  void initialize() {
    receivers_ = task_->comm()->receivers;
    isReceiver_ = std::find(receivers_.begin(), receivers_.end(), task_->comm()->comm->rank) != receivers_.end();
    if (!isReceiver_ && task_->options().sendersAreReceivers && task_->options().scatter) {
      receivers_.push_back(task_->comm()->comm->rank);
    }
  }

  void preSendCB(std::function<void(std::shared_ptr<Input>)> cb) {
    preSendCB_ = cb;
  }

  void destCB(DestCBType<Input> cb) {
    destCB_ = cb;
  }
};

template <typename TasType, typename TypeTable, typename... Inputs>
struct CommunicatorMultiSend;

template <typename TaskType, typename TypeTable, typename... Inputs>
struct CommunicatorMultiSend<TaskType, TypeTable, std::tuple<Inputs...>>
    : CommunicatorSend<TaskType, TypeTable, Inputs>... {
  CommunicatorMultiSend(TaskType *task)
      : CommunicatorSend<TaskType, TypeTable, Inputs>(task)... {}

  template <typename Input>
  void preSendCB(std::function<void(std::shared_ptr<Input>)> cb) {
    CommunicatorSend<TaskType, TypeTable, Input>::preSendCB(cb);
  }

  template <typename Input>
  void destCB(DestCBType<Input> cb) {
    ((CommunicatorSend<TaskType, TypeTable, Input> *)this)->destCB(cb);
  }

  void initialize() {
    (((CommunicatorSend<TaskType, TypeTable, Inputs> *)this)->initialize(), ...);
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
  CommunicatorTaskOpt                 options_;

public:
  explicit CommunicatorTask(comm::CommHandle *commHandle, std::vector<int> const &receivers,
                            CommunicatorTaskOpt opt = {}, std::string const &name = "CommunicatorTask")
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, commHandle, receivers, name)),
        behavior::Copyable<SelfType>(1),
        CommunicatorMultiSend<CommunicatorTask<Types...>, TypesIds, Inputs>(this),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())),
        options_(opt) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
    this->coreTask_->printOptions().background({0x12, 0x34, 0x56, 0xff});
    this->coreTask_->printOptions().font({0xff, 0xff, 0xff, 0xff});
  }

  void initialize() override {
    CommunicatorMultiSend<CommunicatorTask<Types...>, TypesIds, Inputs>::initialize();
  }

  [[nodiscard]] comm::CommTaskHandle<TypesIds> *comm() const {
    return coreTask_->comm();
  }

  [[nodiscard]] CommunicatorTaskOpt options() {
    return options_;
  }

  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  std::shared_ptr<CommunicatorTask<Types...>> copy() override final {
    throw std::runtime_error("error: the communicator task should not be copied.");
  }

  template <typename... MMTypes>
  void setMemoryManager(std::shared_ptr<tool::CommunicatorMemoryManager<MMTypes...>> mm) {
    this->coreTask_->setMemoryManager(mm->template convert<Types...>());
  }

  using tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
