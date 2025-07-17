#ifndef MPI_BRIDGE_CORE
#define MPI_BRIDGE_CORE
#include "log.hpp"
#include "mpi.hpp"
#include "serializer/serialize.hpp"
#include "serializer/tools/type_table.hpp"
#include <ostream>
#include <serializer/serializer.hpp>
#include <sstream>

#include <hedgehog/hedgehog.h>

/// @brief Hedgehog main namespace
namespace hh {

#ifndef DOXYGEN_SHOULD_SKIP_THIS
/// @brief Forward declaration MPIBridge
/// @tparam Separator Separator position between input types and output types
/// @tparam AllTypes List of input and output types
template <typename... Types> class MPIBridge;
#endif // DOXYGEN_SHOULD_SKIP_THIS

/// @brief Hedgehog core namespace
namespace core {

/// @brief Type alias for an TaskInputsManagementAbstraction from the list of template parameters
template <typename... Types>
using BridgeTIM =
    tool::TaskInputsManagementAbstractionTypeDeducer_t<tool::Inputs<sizeof...(Types), Types..., Types...>>;

/// @brief Type alias for an TaskOutputsManagementAbstraction from the list of template parameters
template <typename... Types>
using BridgeTOM =
    tool::TaskOutputsManagementAbstractionTypeDeducer_t<tool::Outputs<sizeof...(Types), Types..., Types...>>;

/// @brief Task core
/// @tparam Separator Separator position between input types and output types
/// @tparam AllTypes List of input and output types
template <typename... Types>
class MPIBridgeCore : public abstraction::TaskNodeAbstraction,
                      public abstraction::ClonableAbstraction,
                      public abstraction::CleanableAbstraction,
                      public abstraction::GroupableAbstraction<MPIBridge<Types...>, MPIBridgeCore<Types...>>,
                      public BridgeTIM<Types...>,
                      public BridgeTOM<Types...> {
private:
  using MPIBridgeType = MPIBridge<Types...>;
  using SelfType = MPIBridgeCore<Types...>;
  using TypesIds = typename MPIBridgeType::TypesIds;

  // TODO: what if a custom serializer is used?
  template <typename Table = serializer::tools::TypeTable<>>
  using Serializer = serializer::Serializer<serializer::Bytes, Table>;

  MPIBridgeType *const task_ = nullptr; ///< User defined task
  int taskId_ = -1;
  std::vector<int> receiversRanks_ = {};

public:
  /// @brief Create a MPIBridgeCore from a user-defined MPIBridge, its  name, the number of threads and the automatic
  /// start flag
  /// @param task User-defined MPIBridge
  /// @param name Task's name
  /// @param numberThreads Number of threads
  /// @param automaticStart Flag for automatic start
  MPIBridgeCore(MPIBridgeType *const task, int taskId, std::vector<int> const &receiversRanks,
                std::string const &name = "MPIBridge")
      : TaskNodeAbstraction(name, task), CleanableAbstraction(static_cast<behavior::Cleanable *>(task)),
        abstraction::GroupableAbstraction<MPIBridgeType, SelfType>(task, 1), BridgeTIM<Types...>(task, this),
        BridgeTOM<Types...>(), task_(task), taskId_(taskId), receiversRanks_(receiversRanks) {
    if (this->numberThreads() == 0) {
      throw std::runtime_error("A task needs at least one thread.");
    }
  }

  /// @brief Default destructor
  ~MPIBridgeCore() override = default;

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

  void runAsReceiver() {
    namespace ser = serializer;
    MPI_Status status;
    ser::Bytes buffer(1024, 1024);
    auto rank = mpi_rank();
    volatile bool canTerminate = false;

    while (!canTerminate) {
      INFO("wait for reception (rank = " << rank << ")");
      MPI_Recv(buffer.data(), buffer.size(), MPI_BYTE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
      WARN("data received (rank = " << rank << ")");
      int senderId = -1;
      auto pos = ser::deserialize<Serializer<>>(buffer, 0, senderId);
      auto typeId = ser::tools::getId<TypesIds>(buffer, pos);

      DBG(senderId);
      DBG(typeId);

      // verify that the sender is correct
      if (senderId != this->taskId_) {
        if (senderId == 0) {
          WARN("termination signal received (rank = " << rank << ").");
          canTerminate = true;
          break;
        } else {
          ERROR("messaged received from the wrong class");
          // the message comes from the wrong sender, therefore, we don't
          // process the message
          continue;
        }
      }

      // deserialize and transmit the result to the connected nodes
      INFO("package received (rank = " << rank << ", data id = " << typeId << ").");
      serializer::tools::applyId(typeId, TypesIds(), [&]<typename T>() {
        std::shared_ptr<T> data = nullptr;
        ser::deserializeWithId<Serializer<TypesIds>, T>(buffer, pos, data);
        task_->addResult(data);
      });
    }
    WARN("Receiver is terminating (rank = " << rank << ").");
  }

  void runAsSender() {
    std::chrono::time_point<std::chrono::system_clock> start, finish;
    volatile bool canTerminate = false;

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
      if (canTerminate) {
        break;
      }

      // Operate the connectedReceivers to get a data and send it to execute
      this->operateReceivers();
    }
  }

  void run() override {
    this->isActive(true);
    this->nvtxProfiler()->initialize(this->threadId(), this->graphId());
    this->preRun();

    if (std::find(receiversRanks_.begin(), receiversRanks_.end(), mpi_rank()) != receiversRanks_.end()) {
      runAsReceiver();
    } else {
      runAsSender();
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
            << "\" has been invoked but return nullptr. To fix this error, overload the MPIBridge::copy function and "
               "return a valid object.";
        throw(std::runtime_error(oss.str()));
      }

      // Copy the memory manager
      taskCopy->connectMemoryManager(this->task_->memoryManager());

      auto taskCoreCopy = dynamic_cast<SelfType *>(taskCopy->core().get());

      if (taskCoreCopy == nullptr) {
        std::ostringstream oss;
        oss << "A copy for the task \"" << this->name()
            << "\" does not have the same type of cores than the original task.";
        throw(std::runtime_error(oss.str()));
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

  [[nodiscard]] std::vector<int> receiversRanks() const { return receiversRanks_; }

  /// @brief Test if a memory manager is attached
  /// @return True if there is a memory manager attached, else false
  [[nodiscard]] bool hasMemoryManagerAttached() const override { return this->memoryManager() != nullptr; }

  /// @brief Accessor to user-defined extra information for the task
  /// @return User-defined extra information for the task
  [[nodiscard]] std::string extraPrintingInformation() const override {
    return this->task_->extraPrintingInformation();
  }

  /// @brief Copy task's inner structure
  /// @param copyableCore Task to copy from
  void copyInnerStructure(SelfType *copyableCore) override {
    BridgeTIM<Types...>::copyInnerStructure(copyableCore);
    BridgeTOM<Types...>::copyInnerStructure(copyableCore);
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
      BridgeTIM<Types...>::printEdgesInformation(printer);
    }
  }

  /// @brief Clone method, to duplicate a task when it is part of another graph in an execution pipeline
  /// @param correspondenceMap Correspondence map of belonging graph's node
  /// @return Clone of this task
  std::shared_ptr<abstraction::NodeAbstraction>
  clone([[maybe_unused]] std::map<NodeAbstraction *, std::shared_ptr<NodeAbstraction>> &correspondenceMap) override {
    auto clone = std::dynamic_pointer_cast<MPIBridgeType>(this->callCopy());
    if (this->hasMemoryManagerAttached()) {
      clone->connectMemoryManager(this->memoryManager()->copy());
    }
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
};
} // namespace core
} // namespace hh

#endif // HEDGEHOG_CORE_TASK_H
