#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "communicator_core_task.hpp"
#include "task_communicator.hpp"
#include <functional>
#include <hedgehog/hedgehog.h>

namespace hh {

// /!\ the sender list will not contain ranks that both send and receive

struct CommunicatorTaskOpt {
  bool sendersAreReceivers = false; // also transmit the data to the current node
  bool scatter = true; // scatter the data between receivers, or send the same data to all
};

template <typename T>
using DestCBType = std::function<std::vector<std::uint32_t>(std::shared_ptr<T>)>;

template <typename TaskType, typename TypesIds, typename Input>
struct CommunicatorSend : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  size_t                     rankIdx_ = 0;
  TaskType                  *task_ = nullptr;
  bool                       isReceiver_ = false;
  std::vector<std::uint32_t> receivers_;
  DestCBType<Input>          destCB_ = nullptr;

public:
  CommunicatorSend(TaskType *task)
      : task_(task) {}

  void addResult(std::shared_ptr<Input> data) {
    task_->addResult(data);
  }

  void callPreSend(std::shared_ptr<Input> data) {
    if constexpr (requires { data->preSend(); }) {
      data->preSend();
    }
  }

  void callPostSend(std::shared_ptr<Input> data) {
    if constexpr (requires { data->postSend(); }) {
      data->postSend();
    }
  }

  bool shouldReturnMemory(std::shared_ptr<Input> data, bool isDataProcessedOnThisRank) {
    // FIXME: we consider that canBeRecycled is always correct?
    if constexpr (requires { data->canBeRecycled(); }) {
      return true;
    } else {
      return !isDataProcessedOnThisRank;
    }
  }

  void execute(std::shared_ptr<Input> data) override {
    logh::infog(logh::IG::CommunicatorTaskExecute, "communicator task execute", "[", (int)task_->comm()->channel(),
                "]: rank = ", task_->comm()->communicator()->rank(), ", isReceiver_ = ", isReceiver_);

    /*
     * The CommunicatorTask has the following behavior:
     * - if the rank is a receiver, then just transmit input data (we stay on one node)
     * - if the rank is a sender:
     *   - if the destCB_ has been specified, use it to know the destination ranks
     *   - else if scatter then scatter the data between the receivers (and optionally the current rank)
     *   - else, send the data to all the receivers (and optionally the current rank)
     */
    if (isReceiver_) {
      addResult(data);
    } else {
      callPreSend(data);
      if (destCB_) {
        sendWithDestCB(data);
      } else if (task_->options().scatter) {
        sendScatter(data);
      } else {
        sendDistribute(data);
      }
    }
  }

  void sendWithDestCB(std::shared_ptr<Input> data) {
    auto dests = destCB_(data);
    auto rankIt = std::find(dests.begin(), dests.end(), task_->comm()->communicator()->rank());
    bool isDataProcessedOnThisRank = false;

    if (rankIt != dests.end()) {
      addResult(data);
      dests.erase(rankIt);
      isDataProcessedOnThisRank = true;
    }
    if (!dests.empty()) {
      task_->comm()->sendData(dests, data, shouldReturnMemory(data, isDataProcessedOnThisRank));
    } else {
      callPostSend(data);
    }
  }

  void sendScatter(std::shared_ptr<Input> data) {
    std::uint32_t receiver = receivers_[rankIdx_];

    rankIdx_ = (rankIdx_ + 1) % receivers_.size();
    if (receiver == task_->comm()->communicator()->rank()) {
      addResult(data);
      callPostSend(data);
    } else {
      task_->comm()->sendData({receiver}, data, shouldReturnMemory(data, false));
    }
  }

  void sendDistribute(std::shared_ptr<Input> data) {
    bool isDataProcessedOnThisRank = false;

    if (task_->options().sendersAreReceivers) {
      addResult(data);
      isDataProcessedOnThisRank = true;
    }
    if (!receivers_.empty()) {
      task_->comm()->sendData(receivers_, data, shouldReturnMemory(data, isDataProcessedOnThisRank));
    } else {
      callPostSend(data);
    }
  }

  void initialize() {
    receivers_ = task_->comm()->receivers();
    isReceiver_
        = std::find(receivers_.begin(), receivers_.end(), task_->comm()->communicator()->rank()) != receivers_.end();
    if (!isReceiver_ && task_->options().sendersAreReceivers && task_->options().scatter) {
      receivers_.push_back(task_->comm()->communicator()->rank());
    }
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
  explicit CommunicatorTask(comm::Communicator *communicator, std::vector<int> const &receivers,
                            CommunicatorTaskOpt opt = {}, std::string const &name = "CommunicatorTask")
      : behavior::TaskNode(std::make_shared<CoreTaskType>(this, communicator, receivers, name)),
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

  [[nodiscard]] comm::TaskCommunicator<TypesIds> *comm() const {
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
  void setMemoryManager(std::shared_ptr<tool::MemoryPool<MMTypes...>> mm) {
    this->coreTask_->setMemoryManager(mm->template convert<Types...>());
  }

  using tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
