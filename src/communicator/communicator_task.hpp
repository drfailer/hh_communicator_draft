#ifndef COMMUNICATOR_COMMUNICATOR_TASK
#define COMMUNICATOR_COMMUNICATOR_TASK
#include "communicator.hpp"
#include "hints.hpp"
#include "communicator_core_task.hpp"
#include "send_strategies.hpp"
#include <functional>
#include <hedgehog.h>

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
    if (dests.empty()) return;
    else if (dests.size() == 1 && dests[0] == this->task_->comm()->rank()) {
      this->task_->addResult(std::move(data));
    } else {
      this->task_->comm()->sendData(dests, std::move(data));
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
///
/// The communicator task is a bridge between the different processes. The idea
/// is that there will be one instance of the program per node, and when data
/// needs to be transfered from one node to another, we use the communicator.
///
/// Example:
///
/// @code
/// // create the service
/// bool enabledProfiling = true;
/// hh::comm::MPIService service(&argc, &argv, enabledProfiling);
///
/// // create a memory manager (use the memory pool)
/// auto mm = std::make_shared<hh::comm::tool::MemoryPool<Data1, Data2>>();
/// mm->fill<Data1>(100);
/// mm->fill<Data2>(100);
///
/// // create and setup the communicator
/// auto ct = std::make_shared<hh::comm::CommunicatorTask<Data1, Data2>>(&service, "example");
///
/// // set the memory manager
/// ct->setMemoryManager(mm);
///
/// // set the send strategies for each type
///
/// // the send strategy is a function that returns a list of destination
/// // ranks, we can use a lambda to compute the destination depending on the
/// // input data
/// gatherTask->strategy<Data1>([&](auto data) {
///     hh::comm::rank_t rank = service.rank();
///     size_t           nbProcesses = service.nbProcesses();
///     return std::vector<hh::comm::rank_t>({(rank + 1) % nbProcesses});
/// });
/// // or use some of the generic strategies provided by the library
/// gatherTask->strategy<Data2>(hh::comm::strategy::SendTo(1, 2));
///
/// // use the communicator like any other tasks:
/// graph.edges(otherTask, ct);
/// // ...
/// @endcode
///
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
  using CoreTaskType = core::CommunicatorCoreTask<Types...>; ///< alias for the core task type.
  using SelfType = CommunicatorTask<Types...>;               ///< alias for the self type
  using Inputs = std::tuple<Types...>;                       ///< tuple of inputs used with hedgehog classes.
  using Outputs = std::tuple<Types...>;                      ///< tuple of output used with hedgehog classes.
  friend CoreTaskType;

private:
  std::shared_ptr<CoreTaskType> const coreTask_ = nullptr; ///< Pointer to the core task.

public:
  /// @brief Constructure from the service and the name
  /// @param service Pointer to an implementation of the comm service.
  /// @param name    Name of the task.
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

  /// @brief Accessor to the communicator.
  /// @return Pointer to the communicator.
  [[nodiscard]] comm::Communicator<Types...> *comm() const {
    return coreTask_->comm();
  }

  /// @brief Implementation of the `canTerminate` function.
  /// @return True if the task can terminate.
  [[nodiscard]] bool canTerminate() const override {
    return !coreTask_->hasNotifierConnected() && coreTask_->receiversEmpty();
  }

  /// @brief Implementation of the copy function (the current version of the
  ///        communicator should not be copied).
  /// @throws runtime_error
  /// @return Never returns (always throws an exception).
  std::shared_ptr<CommunicatorTask<Types...>> copy() override final {
    throw std::runtime_error("error: the communicator task should not be copied.");
  }

  /// @brief Set the memory manager.
  /// @tparam MM Memory manager type: used because the custom memory manager
  ///            don't inherit from the `MemoryManager` class.
  /// @param mm Memory manager
  template <typename MM>
  void setMemoryManager(std::shared_ptr<MM> mm) {
    this->coreTask_->setMemoryManager(mm);
  }

  /// @brief Add a new hint to the communicator.
  /// @tparam T Type affected by the hint.
  /// @param hint Hint to add to the list.
  template <typename T>
  void addHint(comm::hint::Hint const &hint) {
    this->comm()->template addHint<T>(hint);
  }

  /// @brief Set the send threshold.
  /// @param threshold Send threshold value.
  void sendThreshold(size_t threshold) {
    this->comm()->sendThreshold(threshold);
  }

  // void strategy(auto s){
  //   CommunicatorMultiSend<CommunicatorTask<Types...>, Types...>::strategy(s);
  // }

  /// @brief Use addResult from `BehaviorTaskMultiSendersTypeDeducer_t`.
  using tool::BehaviorTaskMultiSendersTypeDeducer_t<std::tuple<Types...>>::addResult;
};
} // namespace hh

#endif
