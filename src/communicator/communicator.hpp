#ifndef COMMUNICATOR_COMMUNICATOR
#define COMMUNICATOR_COMMUNICATOR
#include "tool/log.hpp"
#include "package.hpp"
#include "hints.hpp"
#include "service/comm_service.hpp"
#include "stats.hpp"
#include "tool/memory_manager.hpp"
#include "type_map.hpp"
#include <cassert>
#include <map>
#include <utility>
#include <vector>

// This communicator works the following way:
// 1. Probes for incomming request (data or signal).
//
// Signal is received:
// 2. Handle the signal:
//   - Data: start data reception
//   - Disconnect: disconnect the sender. Terminates when all senders are
//                 disconnected and the core task is terminated (see hedgehog
//                 core task termination makanism).
//
// Data is received:
// 2. wait for the data to be available (get memory from the pool)
// 3. when the data is created, recv all the buffers
// 4. call addResult

/// @brief Hedgehog namespace
namespace hh {
/// @brief Communicator namespace
namespace comm {

/// @brief The communicator handles the communication logic. It uses the comm
///        service to transfert data. Each communicator instance uses a
///        different channel.
/// @tparam Types Types handled by the communicator.
template <typename... Types>
class Communicator {
  /// @param Type map type.
  using TM = comm::TypeMap<Types...>;

public:
  /// @brief Constructor.
  /// @param service Pointer to a comm service implementation.
  Communicator(CommService *service)
      : service_(service),
        channel_(service->newChannel()),
        stats_(TM::size, service->nbProcesses(), service->profilingEnabled()),
        packagesCount_(service->nbProcesses(), 0) {}

public:
  /// @brief Channel id accessor.
  /// @return channel id.
  channel_t channel() const { return channel_; }

  /// @brief Service accessor.
  /// @return service.
  CommService *service() const { return service_; }

  /// @brief Profiling information accessor.
  /// @return Profiling information container.
  CommTaskStats const &stats() const { return stats_; }

  /// @brief rank accessor.
  /// @return Rank.
  rank_t rank() const { return this->service_->rank(); }

  /// @brief Number of processes accessor.
  /// @return Number of processes.
  std::uint32_t nbProcesses() const { return this->service_->nbProcesses(); }

  /// @brief Allow to the memory manager.
  /// @param mm New memory manager.
  void memoryManager(std::shared_ptr<tool::MemoryManager<Types...>> mm) {
    this->mm_ = mm;
  }

  /// TODO: doc
  template <typename T>
  void addHint(hint::Hint const &hint) {
    this->hints_.push_back(HintTracker{hint, TM::template idOf<T>()});
  }

  /// @brief Returns if the queues are empty.
  /// @return True if there are pending information, false otherwise.
  bool hasPendingOperations() const {
      return queueHasPendingOperations(this->sendOps_)
          || queueHasPendingOperations(this->recvOps_)
          || !this->createDataOps_.empty();
  }

  /// @brief Send `data` to the given `dests`.
  /// @tparam T Type of the data to send.
  /// @param dests List of destination ranks.
  /// @param data  Pointer to the data to send.
  template <typename T>
  void sendData(std::vector<rank_t> const &dests, std::shared_ptr<T> data) {
    bool useAddResult = std::find(dests.begin(), dests.end(), this->rank()) != dests.end();
    auto [storageId, storage] = createSendStorage(dests, data, useAddResult);
    Header header(this->rank(), 0, storageId.typeId, this->channel_, 0);

    this->wh_.mutex.lock();
    this->wh_.sendStorage.insert({storageId, storage});
    this->wh_.mutex.unlock();

    for (auto dest : dests) {
      if (dest == this->rank()) {
        continue;
      }
      this->packagesCount_[dest] += 1;
      for (size_t i = 0; i < storage.package.data.size(); ++i) {
        header.bufferId = (buffer_id_t)i;
        std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);
        Request                     request = this->service_->sendAsync(header, dest, storage.package.data[i]);
        this->sendOps_.push_back(CommOperation{
            .bufferId = header.bufferId,
            .request = request,
            .storageId = storageId,
            .hint = -1,
        });
      }
    }
  }

  /// @brief Initialize the communicator.
  void init() {
    this->senderPortState_ = PortState::Opened;
    this->recverPortState_ = PortState::Opened;
    this->connections_ = std::vector(this->nbProcesses(), Connection{true, 0, 0});
    this->connections_[this->rank()].connected = false;
    this->signalBufferMem_ = std::vector<char>(1 + sizeof(size_t) * this->nbProcesses());
    this->sendCountsMap_ = std::vector<size_t>(this->nbProcesses() * this->nbProcesses(), 0);
    this->fini_ = false;

    // TODO: we should post a recv for the disconnection signal here

    initHints();
  }

  /// @brief Run the communicator (should be called in a dedicated thread).
  /// @param addResult Function that gives access to the `task->addResult`
  ///                  method (this is used when receiving data).
  void run(auto addResult) {
    while (this->senderPortState_ != PortState::Closed || this->recverPortState_ != PortState::Closed) {
      progressSender(addResult);
      progressRecver(addResult);
      // progressHints();
    }
  }

  /// @brief This function must be called when the core task terminates to
  ///        notify the communicator thread that no more data will be sent.
  ///        When this function is called, the communicator sends a
  ///        disconnection signal and a list of send counters (number of
  ///        packages sent to each rank) to the master rank. When all the
  ///        senders are disconnected, the master sends the disconnection
  ///        signal to the receivers (with the send counters), and each
  ///        receiver will wait for the reception of all the packages (using
  ///        the counters) before terminating (this is when the communicator
  ///        thread can be joined).
  void fini() {
    this->fini_ = true;
    finalizeHints();
  }

private:
  /// @brief Enum that represents the states of the sender and receiver ports
  ///        (both the sender and the receiver are state machines).
  enum class PortState { Opened, ClosingMaster, ClosingSlave, Closed };

  /// @brief Progress the sender state machine.
  /// @param addResult Function that allow the communicator to call the
  ///                  `addResult` method of the corresponding task. The
  ///                  function is used when one of the send destination is the
  ///                  current rank. The function is only called when all the
  ///                  network send requests are done (because we don't want
  ///                  the data to be mutated before the network transfer is
  ///                  completed, so we wait before sending the data to the
  ///                  other task on the current rank).
  void progressSender(auto addResult) {
    switch (this->senderPortState_) {
    case PortState::Opened:
      processSendOpsQueue(addResult);
      if (this->fini_) {
        this->senderPortState_ = this->rank() == 0 ? PortState::ClosingMaster : PortState::ClosingSlave;
      }
      break;
    case PortState::ClosingMaster:
      if (allDisconnectionSignalsReceived()) {
        processSendOpsQueue(addResult, true);
        assert(this->sendOps_.empty());
        sendDisconnectionSignalToSlaves();
        processSendOpsQueue(addResult, true);
        this->senderPortState_ = PortState::Closed;
      } else {
        processSendOpsQueue(addResult);
      }
      break;
    case PortState::ClosingSlave:
      assert(this->rank() != 0);
      processSendOpsQueue(addResult, true);
      assert(this->sendOps_.empty());
      sendDisconnectionSignalToMaster();
      processSendOpsQueue(addResult, true);
      this->senderPortState_ = PortState::Closed;
      break;
    case PortState::Closed:
      assert(this->sendOps_.empty());
      break;
    }
  }

  /// @brief Progress the receiver state machine.
  /// @param addResult Function that allow the communicator to call the
  ///                  `addResult` method of the corresponding task. The
  ///                  function is used to transfer the received data to the
  ///                  rest of the graph.
  void progressRecver(auto addResult) {
    comm::Signal signal = comm::Signal::None;
    comm::Header header = {0, 0, 0, 0, 0};

    switch (this->recverPortState_) {
    case PortState::Opened:
      recvSignal(signal, header);

      switch (signal) {
      case comm::Signal::None:
        break;
      case comm::Signal::Disconnect:
        if (this->rank() == 0) {
          recvDisconnectionSignalFromSlave(header);
        } else {
          recvDisconnectionSignalFromMaster();
        }
        break;
      case comm::Signal::Data:
        break;
      }
      processCreateDataQueue();
      processRecvOpsQueue(addResult);

      if (!isConnectedOrExpectsMorePackages() && !hasPendingOperations()) {
        this->recverPortState_ = PortState::ClosingSlave;
      }
      break;
    case PortState::ClosingMaster: // fallthrough
    case PortState::ClosingSlave:
      flushRecvQueueAndWarehouse();
      this->recverPortState_ = PortState::Closed;
      break;
    case PortState::Closed:
      break;
    }
  }

private:
  /// @brief Structure that contains connection information for a particular
  ///        rank (used in a list).
  struct Connection {
    bool   connected; ///< connection flag.
    size_t sendCount; ///< number of package sent to the destination.
    size_t recvCount; ///< number of package received by the sender.
  };

  /// @brief Tests if all disconnection signals are received.
  /// @return True if all senders are disconnect, false otherwise.
  bool allDisconnectionSignalsReceived() const {
    for (auto connection : this->connections_) {
      if (connection.connected) {
        return false;
      }
    }
    return true;
  }

  /// @brief Tests if all disconnection signals are received and all the
  ///        packages from each sender are received.
  /// @return True if all senders are disconnect and all packages are received,
  ///         false otherwise.
  bool isConnectedOrExpectsMorePackages() const {
    for (auto connection : this->connections_) {
      if (connection.connected || connection.recvCount < connection.sendCount) {
        return true;
      }
    }
    return false;
  }

  /// @brief Send disconnection signal and package counts to the master process
  ///        (used by slaves sender).
  void sendDisconnectionSignalToMaster() {
    Header header(this->rank(), 1, 0, this->channel_, 0);

    assert(this->sendOps_.empty());

    this->signalBufferMem_[0] = (char)Signal::Disconnect;
    std::memcpy(&this->signalBufferMem_[1], this->packagesCount_.data(), this->nbProcesses() * sizeof(size_t));
    this->service_->send(header, 0, Buffer{this->signalBufferMem_.data(), this->signalBufferMem_.size()});
  }

  /// @brief Send disconnection signal and package counts to the slaves process
  ///        (used by master sender).
  void sendDisconnectionSignalToSlaves() {
    Header header(this->rank(), 1, 0, this->channel_, 0);

    this->signalBufferMem_[0] = (char)Signal::Disconnect;
    for (size_t dest = 1; dest < this->nbProcesses(); ++dest) {
      this->sendCountsMap_[dest * this->nbProcesses()] = this->packagesCount_[dest];
      std::memcpy(&this->signalBufferMem_[1], &this->sendCountsMap_[dest * this->nbProcesses()],
                  this->nbProcesses() * sizeof(size_t));
      this->service_->send(header, dest, Buffer{this->signalBufferMem_.data(), this->signalBufferMem_.size()});
    }
  }

  /// @brief Receive the disconnection signal from the slaves (used by the
  ///        master receiver).
  /// @param header Header of the disconnection request (used to know the source).
  void recvDisconnectionSignalFromSlave(Header const &header) {
    size_t *sendCounts = (size_t *)(&this->signalBufferMem_[1]);

    assert(this->connections_[header.source].connected == true);
    this->connections_[header.source].connected = false;
    this->connections_[header.source].sendCount = sendCounts[0];

    for (size_t dest = 0; dest < this->nbProcesses(); ++dest) {
      this->sendCountsMap_[dest * this->nbProcesses() + header.source] = sendCounts[dest];
    }
  }

  /// @brief Receive the disconnection signal from the master (used by the
  ///        slaves receivers).
  void recvDisconnectionSignalFromMaster() {
    size_t *sendCounts = (size_t *)(&this->signalBufferMem_[1]);

    for (size_t source = 0; source < this->nbProcesses(); ++source) {
      if (source == this->rank()) {
        continue;
      }
      assert(this->connections_[source].connected == true);
      this->connections_[source].connected = false;
      this->connections_[source].sendCount = sendCounts[source];
    }
  }

private:
  /// @brief Forward declaration of HintTracker.
  struct HintTracker;

  /// @brief Structure that contains information about the communication requests.
  struct CommOperation {
    buffer_id_t  bufferId;  ///< Id of the buffer (buffers are sent/received separately).
    Request      request;   ///< Request.
    StorageId    storageId; ///< Id of the storage.
    int          hint;      ///< Hint tracker pointer.
  };

  /// @brief Returns if the given queue has pending operations (do not count the
  ///        hinted requests).
  /// @param queue Queue to test.
  /// @return True if the queue has non hinted pending operations, false otherwise.
  bool queueHasPendingOperations(std::vector<CommOperation> const &queue) const {
    bool result = false;

    for (auto op : queue) {
      if (op.hint == -1) {
        return true;
      }
    }
    return result;
  }

  /******************************************************************************/
  /*                           send queue operations                            */
  /******************************************************************************/

  /// @brief Process the send operation queue.
  /// @param addResult Function that allow transferring data to the result of
  ///                  the graph.
  /// @param flush     Boolean flag that is used when the queue needs to be
  ///                  flushed.
  void processSendOpsQueue(auto addResult, bool flush = false) {
    std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);

    this->stats_.updateSendQueuesInfos(this->sendOps_.size(), this->wh_.sendStorage.size());

    do {
      for (auto it = this->sendOps_.begin(); it != this->sendOps_.end();) {
        if (this->service_->requestCompleted(it->request)) {
          std::lock_guard<std::mutex> whLock(this->wh_.mutex);
          assert(this->wh_.sendStorage.contains(it->storageId));
          StorageSlot<TM> &storage = this->wh_.sendStorage.at(it->storageId);
          ++storage.bufferCount;

          if (storage.bufferCount == storage.ttlBufferCount) {
            postSend(it->storageId, storage, addResult);
            this->wh_.sendStorage.erase(it->storageId);
          }
          this->service_->requestRelease(it->request);
          it = this->sendOps_.erase(it);
        } else {
          it++;
        }
      }
    } while (flush && !this->sendOps_.empty());
  }

  /// @brief Process the data after it is sent.
  /// @param storageId Id of the storage in which the sent data is stored.
  /// @param storage   Package Storage in which the data is stored.
  /// @param addResult Function that allow transferring the data to the rest of
  ///                  the graph (if `addResult` is required, it is done here).
  void postSend(StorageId const &storageId, StorageSlot<TM> const &storage, auto addResult) {
    assert(storageId.typeId < TM::size);
    TM::apply(storageId.typeId, [&]<typename T>() {
      std::shared_ptr<T> data = std::get<std::shared_ptr<T>>(storage.data);
      if constexpr (requires { data->postSend(); }) {
        data->postSend();
      }
      if (storage.useAddResult) {
        addResult(std::move(data));
      } else {
        this->mm_->release(std::move(data));
      }
    });
  }

  /// @brief Create a storage slot for a sent package.
  /// @tparam T Type of the data that is sent.
  /// @param dests Vector that contains the destinations ranks.
  /// @param data  Data that requires to be sent.
  /// @param useAddResult Flag that will be used in  `postSend` to know if
  ///                     `addResult` should be used.
  template <typename T>
  std::pair<StorageId, StorageSlot<TM>> createSendStorage(std::vector<rank_t> const &dests, std::shared_ptr<T> data,
                                                             bool useAddResult) {
    size_t       nbDests = useAddResult ? dests.size() - 1 : dests.size();

    // measure data packing time
    time_t  tpackingStart = std::chrono::system_clock::now();
    Package package = pack(data);
    time_t  tpackingEnd = std::chrono::system_clock::now();
    assert(package.data.size() <= 4);

    // create the storage slot
    StorageSlot<TM> storage = {
        .package = package,
        .bufferCount = 0,
        .ttlBufferCount = package.data.size() * nbDests,
        .data = data,
        .useAddResult = useAddResult,
        .dbgBufferReceived = {false, false, false, false},
    };
    StorageId storageId(this->rank(), TM::template idOf<T>());

    this->stats_.registerSendTimings(storageId, dests,
                                     std::chrono::duration_cast<std::chrono::nanoseconds>(tpackingEnd - tpackingStart),
                                     package.size());
    return {storageId, storage};
  }

  /******************************************************************************/
  /*                           recv queue operations                            */
  /******************************************************************************/

  /// @brief Process the `createDataQueue` that stores recv requests before the
  ///        the data is available for the reception. Here we try to allocate a
  ///        new data using the memory manager. If the memory manager returns
  ///        some data, a new storage slot is created to store the data and the
  ///        request, and we start the reception. Otherwise, the request
  ///        remains in this queue until some memory is available.
  void processCreateDataQueue() {
    std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);

    this->stats_.updateCreateDataQueueInfos(this->createDataOps_.size());

    for (auto it = this->createDataOps_.begin(); it != this->createDataOps_.end();) {
      if (recvData(*it)) {
        it = this->createDataOps_.erase(it);
      } else {
        it++;
      }
    }
  }

  /// @brief Process the recv operations queue. When all the buffers for a
  ///        particular data have arrived, the storage slot is destroyed and the
  ///        data is transferred to the result of the graph using `addResult`.
  /// @param addResult Function that allow to call `task->addResult`
  void processRecvOpsQueue(auto addResult) {
    std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);

    this->stats_.updateRecvQueuesInfos(this->recvOps_.size(), this->wh_.recvStorage.size());

    for (auto it = this->recvOps_.begin(); it != this->recvOps_.end();) {
      if (this->service_->requestCompleted(it->request)) {
        std::lock_guard<std::mutex> whLock(this->wh_.mutex);
        assert(this->wh_.recvStorage.contains(it->storageId));
        auto &storage = this->wh_.recvStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount == storage.ttlBufferCount) {
          postRecv(it->storageId, storage, addResult);
          if (it->hint != -1) {
            hintRequestCompleted(it->hint);
          }
          this->wh_.recvStorage.erase(it->storageId);
        }
        this->service_->requestRelease(it->request);
        it = this->recvOps_.erase(it);
      } else {
        it++;
      }
    }
  }

  /// @brief Probe the network. When a valid message has arrived: if it
  ///        contains a signal, then receive the signal directly (blocking),
  ///        otherwise, add a pending recv data request to the queue.
  /// @param signal Reference to the signal that allow to the receiver state
  ///               machine to know if a signal or some data has arrived.
  /// @param header Reference to a header that is set when the probe is
  ///               successful.
  void recvSignal(Signal &signal, Header &header) {
    Request request = this->service_->probeAsync(this->channel_);

    signal = Signal::None;

    if (this->service_->probeSuccess(request)) {
      header = this->service_->requestHeader(request);
      header.channel = this->channel_;
      assert(header.source != this->rank());

      if (header.signal == 0) {
        std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);
        this->createDataOps_.push_back(header);
        signal = Signal::Data;
        this->service_->requestRelease(request);
      } else {
        comm::Buffer buf{this->signalBufferMem_.data(), this->signalBufferMem_.size()};
        this->service_->recv(request, buf);
        signal = (Signal)buf.mem[0];
      }
      assert(header.source < this->nbProcesses());
    } else {
      this->service_->requestRelease(request);
    }
  }

  /// @brief Try to create a storage slot for the data. If a storage slot can
  ///        be created, the reception of all the buffers is started.
  /// @param header Header of the request fetched by the probe.
  /// @return True if the reception has started.
  bool recvData(Header header, int hint = -1) {
    std::lock_guard<std::mutex> whLock(this->wh_.mutex);
    StorageId                   storageId(header.source, header.typeId);

    if (!createRecvStorage(storageId)) {
      return false;
    }

    auto &storage = this->wh_.recvStorage.at(storageId);
    for (header.bufferId = 0; header.bufferId < storage.package.data.size(); ++header.bufferId) {
      assert(storage.dbgBufferReceived[header.bufferId] == false);
      storage.dbgBufferReceived[header.bufferId] = true;
      this->recvOps_.push_back(CommOperation{
          .bufferId = header.bufferId,
          .request = this->service_->recvAsync(header, storage.package.data[header.bufferId]),
          .storageId = storageId,
          .hint = hint,
      });
    }
    return true;
  }

  /// @brief Process the data after the reception is completed.
  /// @param storageId Id of the storage slot in the warehouse.
  /// @param storage   Storage slot.
  /// @param addResult Function that allow transferring the data to rest of the
  ///                  graph (calls `task->addResult`).
  void postRecv(StorageId const &storageId, StorageSlot<TM> &storage, auto addResult) {
    time_t tunpackingStart, tunpackingEnd;
    assert(storageId.typeId < TM::size);
    TM::apply(storageId.typeId, [&]<typename T>() {
      auto data = std::get<std::shared_ptr<T>>(storage.data);
      tunpackingStart = std::chrono::system_clock::now();
      unpack(std::move(storage.package), data);
      tunpackingEnd = std::chrono::system_clock::now();
      addResult(data);
    });

    this->stats_.registerRecvTimings(
        storageId, std::chrono::duration_cast<std::chrono::nanoseconds>(tunpackingEnd - tunpackingStart),
        storage.package.size());
    ++this->connections_[storageId.source].recvCount;
  }

  /// @brief Try to allocated a new data using the memory manager. If the
  ///        memory manager returns a valid data, creates a new storage slot in
  ///        the warehouse.
  /// @param storageId Id for the new storage slot.
  /// @return True if the storage slot was created, false otherwise.
  bool createRecvStorage(StorageId storageId) {
    bool status = true;

    assert(storageId.typeId < TM::size);
    TM::apply(storageId.typeId, [&]<typename T>() {
      auto data = this->mm_->template allocate<T>();

      if (data == nullptr) {
        status = false;
        return;
      }
      auto               package = packageMem(data);
      StorageSlot<TM> storage{
          .package = package,
          .bufferCount = 0,
          .ttlBufferCount = package.data.size(),
          .data = data,
          .useAddResult = false,
          .dbgBufferReceived = {false, false, false, false},
      };
      this->wh_.recvStorage.insert({storageId, storage});
    });
    return status;
  }

  /// @brief Cancel the remaining received requests and clear the queue.
  ///
  /// Note: with the current implementation, the receiver is forced to receiver
  /// all the requests, which means when this function is called, the queues
  /// are necessarily empty. However, this function may be more useful if this
  /// behavior changes.
  void flushRecvQueueAndWarehouse() {
    if (!this->recvOps_.empty()) {
      log::error("Cancelling ", this->recvOps_.size(), " recv operations.");
    }
    for (auto &op : this->recvOps_) {
      this->service_->requestCancel(op.request);
    }
    this->recvOps_.clear();

    if (!this->createDataOps_.empty()) {
      log::error("Cancelling ", this->createDataOps_.size(), " create data operations.");
    }
    this->createDataOps_.clear();

    if (!this->wh_.recvStorage.empty()) {
      log::error("Removing ", this->wh_.recvStorage.size(), " from storage.");
    }
    for (auto it : this->wh_.recvStorage) {
      auto storageId = it.first;
      auto storage = it.second;
      assert(storageId.typeId < TM::size);

      TM::apply(storageId.typeId, [&]<typename T>() {
        auto data = std::get<std::shared_ptr<T>>(storage.data);
        this->mm_->release(std::move(data));
      });
    }
    this->wh_.recvStorage.clear();
  }

/******************************************************************************/
/*                                   hints                                    */
/******************************************************************************/

private:
  struct HintTracker {
    hint::Hint hint;
    type_id_t typeId;
    size_t activeRequestCount = 0;
    size_t postedRequestCount = 0;
  };

  void hintRequestCompleted(int hintIdx) {
    assert(hintIdx >= 0);
    auto &hint = this->hints_[hintIdx];
    hint.activeRequestCount -= 1;
  }

  void initHints() {
    for (int hintIdx = 0; hintIdx < (int)this->hints_.size(); ++hintIdx) {
      switch (this->hints_[hintIdx].hint.type) {
      case hint::HintType::RecvCountFrom: {
        auto hint = this->hints_[hintIdx].hint.data.recvCountFrom;
        auto typeId = this->hints_[hintIdx].typeId;

        if (hint.source == rank()) {
          continue;
        }

        for (size_t i = 0; i < hint.count; ++i) {
          if (recvData(Header(hint.source, 0, typeId, channel(), 0), hintIdx)) {
            this->hints_[hintIdx].postedRequestCount += 1;
            this->hints_[hintIdx].activeRequestCount += 1;
          }
        }
      } break;
      case hint::HintType::ContinuousRecvFrom: {
        auto hint = this->hints_[hintIdx].hint.data.continuousRecvFrom;
        auto typeId = this->hints_[hintIdx].typeId;

        if (hint.source == rank()) {
          continue;
        }

        for (size_t i = 0; i < hint.poolSize; ++i) {
          if (recvData(Header(hint.source, 0, typeId, channel(), 0), hintIdx)) {
            this->hints_[hintIdx].postedRequestCount += 1;
            this->hints_[hintIdx].activeRequestCount += 1;
          }
        }
      } break;
      }
    }
  }

  void progressHints() {
    for (int hintIdx = 0; hintIdx < (int)this->hints_.size(); ++hintIdx) {
      switch (this->hints_[hintIdx].hint.type) {
      case hint::HintType::RecvCountFrom: {
        auto hint = this->hints_[hintIdx].hint.data.recvCountFrom;
        auto typeId = this->hints_[hintIdx].typeId;

        if (hint.source == rank()) {
          continue;
        }

        for (size_t i = this->hints_[hintIdx].postedRequestCount; i < hint.count; ++i) {
          if (recvData(Header(hint.source, 0, typeId, channel(), 0), hintIdx)) {
            this->hints_[hintIdx].postedRequestCount += 1;
            this->hints_[hintIdx].activeRequestCount += 1;
          }
        }
      } break;
      case hint::HintType::ContinuousRecvFrom: {
        auto hint = this->hints_[hintIdx].hint.data.continuousRecvFrom;
        auto typeId = this->hints_[hintIdx].typeId;

        if (hint.source == rank()) {
          continue;
        }

        for (size_t i = this->hints_[hintIdx].activeRequestCount; i < hint.poolSize; ++i) {
          if (recvData(Header(hint.source, 0, typeId, channel(), 0), hintIdx)) {
            this->hints_[hintIdx].postedRequestCount += 1;
            this->hints_[hintIdx].activeRequestCount += 1;
          }
        }
      } break;
      }
    }
  }

  void finalizeHints() {}

  /******************************************************************************/
  /*                                   stats                                    */
  /******************************************************************************/

public:
  /// @brief Sends all the profiling information to the master process.
  void sendStats() const {
    Header            header(this->rank(), 0, 0, this->channel_, 0);
    std::vector<char> bufMem;
    this->stats_.pack(bufMem);
    this->service_->send(header, 0, Buffer{bufMem.data(), bufMem.size()});
  }

  /// @brief Gather all the profiling information.
  /// @return List of the profiling information of each rank.
  std::vector<CommTaskStats> gatherStats() const {
    std::vector<char>          bufMem;
    std::vector<CommTaskStats> stats(this->nbProcesses());
    size_t                     bufSize;

    stats[0].transmissionStats = std::move(this->stats_.transmissionStats);
    stats[0].maxSendOpsSize = this->stats_.maxSendOpsSize;
    stats[0].maxRecvOpsSize = this->stats_.maxRecvOpsSize;
    stats[0].maxCreateDataQueueSize = this->stats_.maxCreateDataQueueSize;
    stats[0].maxSendStorageSize = this->stats_.maxSendStorageSize;
    stats[0].maxRecvStorageSize = this->stats_.maxRecvStorageSize;
    for (rank_t i = 1; i < this->nbProcesses(); ++i) {
      Request request = this->service_->probe(this->channel_, i);
      bufSize = (size_t)this->service_->bufferSize(request);
      bufMem.resize(bufSize);
      this->service_->recv(request, Buffer{.mem = bufMem.data(), .len = bufMem.size()});
      stats[i].transmissionStats.nbProcesses = this->nbProcesses();
      stats[i].unpack(bufMem);
    }
    return stats;
  }

  /******************************************************************************/
  /*                                 attributes                                 */
  /******************************************************************************/

private:
  CommService *service_ = nullptr; ///< Pointer to the service implementation.
  channel_t    channel_ = 0;       ///< Channel id.

  // progress loop data
  PortState               senderPortState_; ///< State of the sender port.
  PortState               recverPortState_; ///< State of the receiver port.
  std::vector<Connection> connections_;     ///< List of connection information per rank.
  bool                    fini_ = false;    ///< Termination flag.

  // queues
  std::vector<CommOperation> sendOps_;       ///< Queue of send operations.
  std::vector<CommOperation> recvOps_;       ///< Queue of recv operations.
  std::vector<Header>        createDataOps_; ///< Queue of create data operation (wait for available memory).
  std::mutex                 queuesMutex_;   ///< mutex for the queues.

  // packages
  PackageWarehouse<TM> wh_; ///< Package warehouse that stores the data during transmission.

  // stats
  CommTaskStats       stats_;              ///< Profiling informations
  std::vector<size_t> packagesCount_ = {}; ///< Package counters.

  // diconnection buffer
  std::vector<char>   signalBufferMem_; ///< Buffer memory used to send the disconnection signal.
  std::vector<size_t> sendCountsMap_;   ///< 2D array: [send][dest]->packageCount (used by the master process).

  // memory manager
  std::shared_ptr<tool::MemoryManager<Types...>> mm_ = nullptr; ///< Pointer to the memory manager.

  // hints
  std::vector<HintTracker> hints_;
};

} // end namespace comm

} // end namespace hh

#endif
