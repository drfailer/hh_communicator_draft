#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "communicator.hpp"
#include "communicator_core_task.hpp"
#include "send_strategies.hpp"
#include <functional>
#include <hedgehog/hedgehog.h>

/// @brief Hedgehog namespace
namespace hh {

/******************************************************************************/
/*                              CommunicatorSend                              */
/******************************************************************************/

/// @brief Communicator send interface for the `Input` type.
/// @tparam TaskType Type of the communicator task.
/// @tparam Input    Type managed by the interface.
template <typename TaskType, typename Input>
struct CommunicatorSend : tool::BehaviorMultiExecuteTypeDeducer_t<std::tuple<Input>> {
private:
  TaskType           *task_ = nullptr;           ///< Pointer to the task.
  comm::SendStrategy<Input> strategy_ = nullptr; ///< Send strategy (allow to know the destinations for each data).

public:
  /// @brief Constructor from task.
  /// @param task Pointer to the task.
  CommunicatorSend(TaskType *task)
      : task_(task) {}

  /// @brief Implementation of the communicator task execute method for the
  ///        type `Input`. If the send strategy returns a list of destinations
  ///        containing other the ranks of other processes, the data is given
  ///        to the communicator that will handle the network transfer.
  ///        Otherwise, the data is simply propagated to the graph.
  /// @param data Input data to process
  void execute(std::shared_ptr<Input> data) override {
    if constexpr (requires { data->preSend(); }) {
      data->preSend();
    }
    if (!this->strategy_) {
      std::ostringstream oss;
      oss << "error: send strategy not set for task " << hh::tool::typeToStr<TaskType>() << " (type is `"
          << hh::tool::typeToStr<Input>() << "')." << std::endl;
      throw std::runtime_error(oss.str());
    }
    auto dests = this->strategy_(data);
    if (dests.size() == 1 && dests[0] == this->task_->comm()->rank()) {
      this->task_->addResult(data);
    } else {
      this->task_->comm()->sendData(dests, data);
    }
  }

  /// @brief Send strategy setter.
  /// @param strategy Send strategy to set.
  void strategy(comm::SendStrategy<Input> strategy) {
    strategy_ = strategy;
  }
};

/******************************************************************************/
/*                           CommunicatorMultiSend                            */
/******************************************************************************/

/// @brief Intermediate class that simplify the use of `CommunicatorSend` in
///        the `CommunicatorTask`.
/// @tparam TaskType Type of the CommunicatorTask task (using CRTP here allow
///                  using the communicator task methods in `CommunicatorSend`).
/// @tparam Inputs   Types managed by the communicator.
template <typename TaskType, typename... Inputs>
struct CommunicatorMultiSend : CommunicatorSend<TaskType, Inputs>... {
  /// @brief Constructor from the task pointer.
  /// @param task Pointer to the communicator task.
  CommunicatorMultiSend(TaskType *task)
      : CommunicatorSend<TaskType, Inputs>(task)... {}

  /// @brief Call strategy setter for the specified `Input` type.
  /// @tparam Input    Input type for which the strategy should be used.
  /// @param  strategy Send strategy to set.
  template <typename Input>
  void strategy(comm::SendStrategy<Input> strategy) {
    ((CommunicatorSend<TaskType, Input> *)this)->strategy(strategy);
  }
};

/******************************************************************************/
/*                              CommunicatorTask                              */
/******************************************************************************/

/// @brief Communicator task.
/// @tparam Types Types managed by the communicator.
template <typename... Types>
class CommunicatorTask
    : public behavior::TaskNode,
      public behavior::CanTerminate,
      public behavior::Cleanable,
      public behavior::Copyable<CommunicatorTask<Types...>>,
      public tool::BehaviorMultiReceiversTypeDeducer_t<std::tuple<Types...>>,
      public CommunicatorMultiSend<CommunicatorTask<Types...>, Types...>,
      public tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>> {
private:
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
        CommunicatorMultiSend<CommunicatorTask<Types...>, Types...>(this),
        tool::BehaviorTaskMultiSendersTypeDeducer_t<Outputs>((std::dynamic_pointer_cast<CoreTaskType>(this->core()))),
        coreTask_(std::dynamic_pointer_cast<CoreTaskType>(this->core())) {
    if (coreTask_ == nullptr) {
      throw std::runtime_error("The core used by the task should be a CoreTask.");
    }
    this->coreTask_->printOptions().background({0x12, 0x34, 0x56, 0xff});
    this->coreTask_->printOptions().font({0xff, 0xff, 0xff, 0xff});
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
