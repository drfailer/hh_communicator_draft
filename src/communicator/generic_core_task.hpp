#ifndef HEDGEHOG_GENERIC_CORE_TASK_H
#define HEDGEHOG_GENERIC_CORE_TASK_H

#include <ostream>
#include <sstream>

#include <hedgehog/hedgehog.h>

/// @brief Hedgehog main namespace
namespace hh {

/// @brief Hedgehog core namespace
namespace core {

/// @brief Type alias for an TaskInputsManagementAbstraction from the list of template parameters
template<size_t Separator, class ...AllTypes>
using TIM = tool::TaskInputsManagementAbstractionTypeDeducer_t<tool::Inputs<Separator, AllTypes...>>;

/// @brief Type alias for an TaskOutputsManagementAbstraction from the list of template parameters
template<size_t Separator, class ...AllTypes>
using TOM = tool::TaskOutputsManagementAbstractionTypeDeducer_t<tool::Outputs<Separator, AllTypes...>>;

/// @brief Task core
/// @tparam Separator Separator position between input types and output types
/// @tparam AllTypes List of input and output types
template<class TaskType, size_t Separator, class ...AllTypes>
class GenericCoreTask
    : public abstraction::TaskNodeAbstraction,
      public abstraction::ClonableAbstraction,
      public abstraction::CleanableAbstraction,
      public abstraction::GroupableAbstraction<TaskType, GenericCoreTask<TaskType, Separator, AllTypes...>>,
      public TIM<Separator, AllTypes...>,
      public TOM<Separator, AllTypes...> {

 protected:
  TaskType *const
      task_ = nullptr; ///< User defined task

  bool const
      automaticStart_ = false; ///< Flag for automatic start

 public:
  /// @brief Create a GenericCoreTask from a user-defined TaskType with one thread
  /// @param task User-defined TaskType
  explicit GenericCoreTask(TaskType *const task) :
      TaskNodeAbstraction("Task", task),
      CleanableAbstraction(static_cast<behavior::Cleanable *>(task)),
      abstraction::GroupableAbstraction<TaskType, GenericCoreTask<TaskType, Separator, AllTypes...>>(
          task, 1
      ),
      TIM<Separator, AllTypes...>(task, this),
      TOM<Separator, AllTypes...>(),
      task_(task),
      automaticStart_(false) {}

  /// @brief Create a GenericCoreTask from a user-defined TaskType, its  name, the number of threads and the automatic
  /// start flag
  /// @param task User-defined TaskType
  /// @param name Task's name
  /// @param numberThreads Number of threads
  /// @param automaticStart Flag for automatic start
  GenericCoreTask(TaskType *const task,
           std::string const &name, size_t const numberThreads, bool const automaticStart) :
      TaskNodeAbstraction(name, task),
      CleanableAbstraction(static_cast<behavior::Cleanable *>(task)),
      abstraction::GroupableAbstraction<TaskType, GenericCoreTask<TaskType, Separator, AllTypes...>>(
          task, numberThreads
      ),
      TIM<Separator, AllTypes...>(task, this),
      TOM<Separator, AllTypes...>(),
      task_(task),
      automaticStart_(automaticStart) {
    if (this->numberThreads() == 0) { throw std::runtime_error("A task needs at least one thread."); }
  }

  /// @brief Construct a task from the user-defined task and its concrete abstraction's implementations
  /// @tparam ConcreteMultiReceivers Type of concrete implementation of ReceiverAbstraction for multiple types
  /// @tparam ConcreteMultiExecutes Type of concrete implementation of ExecuteAbstraction for multiple types
  /// @tparam ConcreteMultiSenders Type of concrete implementation of SenderAbstraction for multiple types
  /// @param task User-defined task
  /// @param name Task's name
  /// @param numberThreads Number of threads for the task
  /// @param automaticStart Flag for automatic start
  /// @param concreteSlot Concrete implementation of SlotAbstraction
  /// @param concreteMultiReceivers Concrete implementation of ReceiverAbstraction for multiple types
  /// @param concreteMultiExecutes Concrete implementation of ExecuteAbstraction for multiple type
  /// @param concreteNotifier Concrete implementation of NotifierAbstraction
  /// @param concreteMultiSenders Concrete implementation of SenderAbstraction for multiple types
  /// @throw std::runtime_error if the number of threads is 0
  template<class ConcreteMultiReceivers, class ConcreteMultiExecutes, class ConcreteMultiSenders>
  GenericCoreTask(TaskType *const task,
           std::string const &name, size_t const numberThreads, bool const automaticStart,
           std::shared_ptr<implementor::ImplementorSlot> concreteSlot,
           std::shared_ptr<ConcreteMultiReceivers> concreteMultiReceivers,
           std::shared_ptr<ConcreteMultiExecutes> concreteMultiExecutes,
           std::shared_ptr<implementor::ImplementorNotifier> concreteNotifier,
           std::shared_ptr<ConcreteMultiSenders> concreteMultiSenders) :
      TaskNodeAbstraction(name, task),
      CleanableAbstraction(static_cast<behavior::Cleanable *>(task)),
      abstraction::GroupableAbstraction<TaskType, GenericCoreTask<TaskType, Separator, AllTypes...>>
          (task, numberThreads),
      TIM<Separator, AllTypes...>(task, this, concreteSlot, concreteMultiReceivers, concreteMultiExecutes),
      TOM<Separator, AllTypes...>(concreteNotifier, concreteMultiSenders),
      task_(task),
      automaticStart_(automaticStart) {
    if (this->numberThreads() == 0) { throw std::runtime_error("A task needs at least one thread."); }
  }

  /// @brief Default destructor
  ~GenericCoreTask() override = default;

  /// @brief Accessor to the memory manager
  /// @return The attached memory manager
  [[nodiscard]] std::shared_ptr<AbstractMemoryManager> memoryManager() const override {
    return this->task_->memoryManager();
  }

  /// @brief Initialize the task
  /// @details Call user define initialize, initialise memory manager if present
  void preRun() override {
    this->nvtxProfiler()->startRangeInitializing();
    this->task_->initialize();
    if (this->task_->memoryManager() != nullptr) {
      this->task_->memoryManager()->profiler(this->nvtxProfiler());
      this->task_->memoryManager()->deviceId(this->deviceId());
      this->task_->memoryManager()->initialize();
    }
    this->nvtxProfiler()->endRangeInitializing();
    this->setInitialized();
  }

  /// @brief Main core task logic
  /// @details
  /// - if automatic start
  ///     - call user-defined task's execute method with nullptr
  /// - while the task runs
  ///     - wait for data or termination
  ///     - if can terminate, break
  ///     - get a piece of data from the queue
  ///     - call user-defined state's execute method with data
  /// - shutdown the task
  /// - notify successors task terminated
  void run() override {
    std::chrono::time_point<std::chrono::system_clock>
        start,
        finish;

    volatile bool canTerminate = false;

    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    if (this->automaticStart_) {  this->callAllExecuteWithNullptr(); }

    // Actual computation loop
    while (!this->canTerminate()) {
      // Wait for a data to arrive or termination
      this->nvtxProfiler()->startRangeWaiting();
      start = std::chrono::system_clock::now();
      canTerminate = this->sleep();
      finish = std::chrono::system_clock::now();
      this->nvtxProfiler()->endRangeWaiting();
      this->incrementWaitDuration(std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start));

      // If loop can terminate break the loop early
      if (canTerminate) { break; }

      // Operate the connectedReceivers to get a data and send it to execute
      this->operateReceivers();
    }

    // Do the shutdown phase
    this->postRun();
    // Wake up a node that this node is linked to
    this->wakeUp();
  }

  /// @brief When a task terminates, set the task to non active, call user-defined shutdown, and disconnect the task to
  /// successor nodes
  void postRun() override {
    this->nvtxProfiler()->startRangeShuttingDown();
    this->isActive(false);
    this->task_->shutdown();
    this->notifyAllTerminated();
    this->nvtxProfiler()->endRangeShuttingDown();
  }

  /// @brief Create a group for this task, and connect each copies to the predecessor and successor nodes
  /// @param map  Map of nodes and groups
  /// @throw std::runtime_error it the task is ill-formed or the copy is not of the right type
  void createGroup(std::map<NodeAbstraction *, std::vector<NodeAbstraction *>> &map) override {
    abstraction::SlotAbstraction *coreCopyAsSlot;
    abstraction::NotifierAbstraction *coreCopyAsNotifier;

    for (size_t threadId = 1; threadId < this->numberThreads(); ++threadId) {
      auto taskCopy = this->callCopyAndRegisterInGroup();

      if (taskCopy == nullptr) {
        std::ostringstream oss;
        oss << "A copy for the task \"" << this->name()
            << "\" has been invoked but return nullptr. To fix this error, overload the TaskType::copy function and "
               "return a valid object.";
        throw (std::runtime_error(oss.str()));
      }

      // Copy the memory manager
      taskCopy->connectMemoryManager(this->task_->memoryManager());

      auto taskCoreCopy = dynamic_cast<GenericCoreTask<TaskType, Separator, AllTypes...> *>(taskCopy->core().get());

      if (taskCoreCopy == nullptr) {
        std::ostringstream oss;
        oss << "A copy for the task \"" << this->name()
            << "\" does not have the same type of cores than the original task.";
        throw (std::runtime_error(oss.str()));
      }

      // Deal with the group registration in the graph
      map.at(static_cast<NodeAbstraction *>(this)).push_back(taskCoreCopy);

      // Copy inner structures
      taskCoreCopy->copyInnerStructure(this);

      // Make necessary connections
      coreCopyAsSlot = static_cast<abstraction::SlotAbstraction *>(taskCoreCopy);
      coreCopyAsNotifier = static_cast<abstraction::NotifierAbstraction *>(taskCoreCopy);

      for (auto predecessorNotifier : static_cast<abstraction::SlotAbstraction *>(this)->connectedNotifiers()) {
        for (auto notifier : predecessorNotifier->notifiers()) {
          for (auto slot : coreCopyAsSlot->slots()) {
            slot->addNotifier(notifier);
            notifier->addSlot(slot);
          }
        }
      }

      for (auto successorSlot : static_cast<abstraction::NotifierAbstraction *>(this)->connectedSlots()) {
        for (auto slot : successorSlot->slots()) {
          for (auto notifier : coreCopyAsNotifier->notifiers()) {
            slot->addNotifier(notifier);
            notifier->addSlot(slot);
          }
        }
      }
    }
  }

  /// @brief Test if a memory manager is attached
  /// @return True if there is a memory manager attached, else false
  [[nodiscard]] bool hasMemoryManagerAttached() const override { return this->memoryManager() != nullptr; }

  /// @brief Accessor to the automatic start flag
  /// @return true if the core start automatically, else false
  [[nodiscard]] bool automaticStart() const { return automaticStart_; }

  /// @brief Accessor to user-defined extra information for the task
  /// @return User-defined extra information for the task
  [[nodiscard]] std::string extraPrintingInformation() const override {
    return this->task_->extraPrintingInformation();
  }

  /// @brief Copy task's inner structure
  /// @param copyableCore Task to copy from
  void copyInnerStructure(GenericCoreTask<TaskType, Separator, AllTypes...> *copyableCore) override {
    TIM<Separator, AllTypes...>::copyInnerStructure(copyableCore);
    TOM<Separator, AllTypes...>::copyInnerStructure(copyableCore);
  }

  /// @brief Node ids [nodeId, nodeGroupId] accessor
  /// @return  Node ids [nodeId, nodeGroupId]
  [[nodiscard]] std::vector<std::pair<std::string const, std::string const>> ids() const override {
    return {{this->id(), this->groupRepresentativeId()}};
  }
  /// @brief Visit the task
  /// @param printer Printer gathering task information
  void visit(Printer *printer) override {
    if (printer->registerNode(this)) {
      printer->printNodeInformation(this);
      TIM<Separator, AllTypes...>::printEdgesInformation(printer);
    }
  }

  /// @brief Clone method, to duplicate a task when it is part of another graph in an execution pipeline
  /// @param correspondenceMap Correspondence map of belonging graph's node
  /// @return Clone of this task
  std::shared_ptr<abstraction::NodeAbstraction> clone(
      [[maybe_unused]] std::map<NodeAbstraction *, std::shared_ptr<NodeAbstraction>> &correspondenceMap) override {
    auto clone = std::dynamic_pointer_cast<TaskType>(this->callCopy());
    if (this->hasMemoryManagerAttached()) { clone->connectMemoryManager(this->memoryManager()->copy()); }
    return clone->core();
  }

  /// @brief Duplicate the task edge
  /// @param mapping Correspondence map of belonging graph's node
  void duplicateEdge(std::map<NodeAbstraction *, std::shared_ptr<NodeAbstraction>> &mapping) override {
    this->duplicateOutputEdges(mapping);
  }

  /// @brief Accessor to the execution duration per input
  /// @return A Map where the key is the type as string, and the value is the associated duration
  [[nodiscard]] std::map<std::string, std::chrono::nanoseconds> const &executionDurationPerInput() const final {
    return this->executionDurationPerInput_;
  }

  /// @brief Accessor to the number of elements per input
  /// @return A Map where the key is the type as string, and the value is the associated number of elements received
  [[nodiscard]] std::map<std::string, std::size_t> const &nbElementsPerInput() const final {
    return this->nbElementsPerInput_;
  }

  /// @brief Accessor to the dequeue + execution duration per input
  /// @return Map in which the key is the type and the value is the duration
  [[nodiscard]] std::map<std::string, std::chrono::nanoseconds> const &dequeueExecutionDurationPerInput() const final {
    return this->dequeueExecutionDurationPerInput_;
  }

  /// @brief Accessor to the task for sub classes
  /// @return Task
  [[nodiscard]] TaskType *task() { return this->task_; }

  /// @brief Accessor to the task for sub classes
  /// @return Task
  [[nodiscard]] TaskType *task() const { return this->task_; }
};
}
}

#endif //HEDGEHOG_CORE_TASK_H
