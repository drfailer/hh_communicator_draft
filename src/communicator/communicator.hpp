#ifndef COMMUNICATOR_COMMUNICATOR
#define COMMUNICATOR_COMMUNICATOR
#include "tool/log.hpp"
#include "tool/queue.hpp"
#include "package.hpp"
#include "service/comm_service.hpp"
#include "memory_manager/memory_manager.hpp"
#include "profiling/communicator_profiler.hpp"
#include "type_map.hpp"
#include <cassert>
#include <atomic>
#include <map>
#include <utility>
#include <vector>
#include <array>
#include <optional>

// This communicator works the following way:
// 1. Probes for incomming request.
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
        createDataOps_(service->nbProcesses(), TM::size),
        wh_(service->nbProcesses(), TM::size),
        profiler_(TM::size, service) {}

public:
  /// @brief Channel id accessor.
  /// @return channel id.
  channel_t channel() const { return channel_; }

  /// @brief Service accessor.
  /// @return service.
  CommService *service() const { return service_; }

  /// @brief rank accessor.
  /// @return Rank.
  rank_t rank() const { return this->service_->rank(); }

  /// @brief Access to the profiler.
  /// @return profiler.
  CommunicatorProfiler const &profiler() const { return this->profiler_; }

  /// @brief Number of processes accessor.
  /// @return Number of processes.
  std::uint32_t nbProcesses() const { return this->service_->nbProcesses(); }

  /// @brief Allow to the memory manager.
  /// @param mm New memory manager.
  void memoryManager(tool::MemoryManager<Types...> *mm) {
    this->mm_ = mm;
  }

  /// @brief Configure the send threshold.
  ///
  /// Note: for now, this threshold is more an estimation and not really the
  /// maximum number of send operations. It is more a maximum number of requests,
  /// where one request can have multiple destinations and multiple buffers
  /// which will all correspond to different send operations. These parameters
  /// should be taken into account when computing the threshold value to get
  /// the proper "maximum number of send".
  ///
  /// @param threshold Threshold value.
  void sendThreshold(size_t threshold) { this->sendThreshold_ = threshold; }

  /// @brief Send `data` to the given `dests`.
  /// @tparam T Type of the data to send.
  /// @param dests List of destination ranks.
  /// @param data  Pointer to the data to send.
  template <typename T>
  void sendData(std::vector<rank_t> const &dests, std::shared_ptr<T> data) {
    if (dests.empty()) return;

    std::lock_guard<std::mutex> queuesLock(this->sendQueueMutex_);
    this->sendQueue_.add(SendRequest{
        .dests = dests,
        .data = std::move(data),
        .typeId = TM::template idOf<T>(),
    });
  }

  /// @brief Initialize the communicator.
  void init() {
    this->fini_.store(false);
  }

  /// @brief Run the communicator (should be called in a dedicated thread).
  /// @param addResult Function that gives access to the `task->addResult`
  ///                  method (this is used when receiving data).
  void run(auto addResult) {
    comm::Header header = {0, 0, 0, 0};

    while (!this->fini_.load()) {
      processSendOpsQueue(addResult);
      probeIncommingRequests(header);
      processCreateDataQueue();
      processRecvOpsQueue(addResult);
    }
    flush();
  }

  /// @brief Wait for the service termination (user calls service.terminate()),
  ///        and terminate run loop. This function must be called in the core
  ///        task.
  void fini() {
    this->service_->waitForTermination();
    this->fini_.store(true);
    this->profiler_.compile<TM>();
    this->profiler_.template generateTransmissionFile<TM>(this->channel());
  }

private:
  struct CreateDataOperation {
    std::array<Request, CommService::MAX_PACKAGE_BUFFER_COUNT> receivedRequests;
    Header header;
  };

  /// @brief Structure used in the send queue when the send threshold is set.
  struct SendRequest {
    std::vector<rank_t> dests; ///< List of destination ranks.
    variant_type_t<TM> data;   ///< Data to send.
    type_id_t typeId;          ///< Type id of the data to send.
  };

  /// @brief Structure that contains information about the communication requests.
  struct CommOperation {
    rank_t       source;    ///< Source that sent the package.
    type_id_t    typeId;    ///< Type id of the sent data.
    buffer_id_t  bufferId;  ///< Id of the buffer (buffers are sent/received separately).
    Request      request;   ///< Request.
    StorageId    storageId; ///< Id of the storage.
    size_t       profileId; ///< Id for the profile info queue.
  };

  /******************************************************************************/
  /*                           send queue operations                            */
  /******************************************************************************/

  /// @brief Package and send `data` to the given destintations.
  /// @tparam T Type of the data to send.
  /// @param dests Destination ranks.
  /// @param data  Data to send.
  template <typename T>
  void processSendRequest(std::vector<rank_t> const &dests, std::shared_ptr<T> data) {
    auto      storage = createSendStorage(dests, std::move(data));
    Header    header(this->rank(), storage.typeId, this->channel_, 0);
    StorageId storageId = this->wh_.sendStorage.add(storage);

    for (auto dest : dests) {
      if (dest == this->rank()) {
        continue;
      }
      size_t bufferCount = storage.package.bufferCount;
      size_t profileId = profiler_.preSend(storage.typeId, dest, bufferCount);
      for (size_t i = 0; i < bufferCount; ++i) {
        header.bufferId = (buffer_id_t)i;
        this->sendOps_.add(CommOperation{
            .source = this->rank(),
            .typeId = storage.typeId,
            .bufferId = header.bufferId,
            .request = this->service_->send(header, dest, storage.package.buffers[i]),
            .storageId = storageId,
            .profileId = profileId,
        });
      }
    }
  }

  /// @brief Process the send queue.
  ///
  /// The send queue is used as a buffer when the send threshold is set.
  /// Elements of the queue are actually sent when the number of operations
  /// does not exceed the threshold.
  void processSendQueue() {
    std::lock_guard<std::mutex> queuesLock(this->sendQueueMutex_);

    this->profiler_.updateProfiledSize(ProfiledSize::MaxSendQueueSize, this->sendQueue_.size());
    for (auto it = this->sendQueue_.begin(); it != this->sendQueue_.end();) {
      if (this->sendThreshold_ > 0 && this->sendOps_.size() >= this->sendThreshold_) {
        break;
      }
      TM::apply(it->typeId, [&]<typename T>() {
        processSendRequest(it->dests, std::move(std::get<std::shared_ptr<T>>(it->data)));
      });
      it = this->sendQueue_.remove(it);
    }
  }

  /// @brief Process the send operation queue.
  /// @param addResult Function that allow transferring data to the result of
  ///                  the graph.
  /// @param flush     Boolean flag that is used when the queue needs to be
  ///                  flushed.
  void processSendOpsQueue(auto addResult, bool flush = false) {
    this->profiler_.updateProfiledSize(ProfiledSize::MaxSendOpsSize, this->sendOps_.size());
    this->profiler_.updateProfiledSize(ProfiledSize::MaxSendStorageSize, this->wh_.sendStorage.size());

    do {
      processSendQueue();
      for (auto it = this->sendOps_.begin(); it != this->sendOps_.end();) {
        if (this->service_->requestCompleted(it->request)) {
          StorageSlot<TM> &storage = this->wh_.sendStorage.at(it->storageId);
          ++storage.bufferCount;

          this->profiler_.postSend(it->profileId, storage.package.size());
          if (storage.bufferCount == storage.ttlBufferCount) {
            postSend(storage, addResult);
            this->wh_.sendStorage.remove(it->storageId);
          }
          this->service_->requestRelease(it->request);
          it = this->sendOps_.remove(it);
        } else {
          it++;
        }
      }
    } while (flush && !this->sendOps_.empty());
  }

  /// @brief Process the data after it is sent.
  /// @param storage   Package Storage in which the data is stored.
  /// @param addResult Function that allow transferring the data to the rest of
  ///                  the graph (if `addResult` is required, it is done here).
  void postSend(StorageSlot<TM> &storage, auto addResult) {
    assert(storage.typeId < TM::size);
    TM::apply(storage.typeId, [&]<typename T>() {
      std::shared_ptr<T> data = std::move(std::get<std::shared_ptr<T>>(storage.data));
      assert(std::get<std::shared_ptr<T>>(storage.data) == nullptr);
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
  StorageSlot<TM> createSendStorage(std::vector<rank_t> const &dests, std::shared_ptr<T> data) {
    bool   useAddResult = std::find(dests.begin(), dests.end(), this->rank()) != dests.end();
    size_t nbDests = useAddResult ? dests.size() - 1 : dests.size();

    // measure data packing time
    time_t  tpackingStart = std::chrono::system_clock::now();
    Package package = pack(data);
    time_t  tpackingEnd = std::chrono::system_clock::now();
    size_t bufferCount = package.bufferCount;
    type_id_t typeId = TM::template idOf<T>();

    this->profiler_.registerPackTime(typeId, computeDuration(tpackingStart, tpackingEnd));

    // create the storage slot
    return StorageSlot<TM>{
        .source = this->rank(),
        .package = std::move(package),
        .bufferCount = 0,
        .ttlBufferCount = bufferCount * nbDests,
        .data = std::move(data),
        .typeId = typeId,
        .useAddResult = useAddResult,
        .receivedBuffers = {},
        .profileId = 0, // unused for the sender
    };
  }

  /******************************************************************************/
  /*                           recv queue operations                            */
  /******************************************************************************/

  void addRecvOpAndUpdateStorage(Header const &header, StorageId storageId, Request request) {
    auto &storage = this->wh_.recvStorage(header.source, header.typeId).at(storageId);
    assert(storage.receivedBuffers.test(header.bufferId) == false);
    this->recvOps_.add(CommOperation{
        .source = header.source,
        .typeId = header.typeId,
        .bufferId = header.bufferId,
        .request = this->service_->recv(request, storage.package.buffers[header.bufferId]),
        .storageId = storageId,
        .profileId = storage.profileId,
    });
    storage.receivedBuffers.set(header.bufferId);
  }

  void registerNewRequest(Header const &header, Request request) {
    // we first lookup in the storage to see if the reception has already
    // started for this package
    if (auto storageId = recvStorageLookup(header)) {
      addRecvOpAndUpdateStorage(header, *storageId, request);
      return;
    }

    // if there is no storage entry, we look for a corresponding entry in the
    // create data queue (in that case, it is possible that at least one buffer
    // of the package has been received, but the memory maanger couldn't allocate
    // the data).
    auto &queue = this->createDataOps_(header.source, header.typeId);
    for (auto op : queue) {
      if (op.receivedRequests[header.bufferId] == nullptr) {
        op.receivedRequests[header.bufferId] = request;
        return;
      }
    }

    // If there are no matching entries neither in the storage or the create
    // data queue, we start the reception for a new package.
    CreateDataOperation op;
    op.header = header;
    memset(op.receivedRequests.data(), 0, sizeof(*op.receivedRequests.data()) * op.receivedRequests.size());
    op.receivedRequests[header.bufferId] = request;
    queue.add(op);
  }

  /// @brief Process the `createDataQueue` that stores recv requests before the
  ///        the data is available for the reception. Here we try to allocate a
  ///        new data using the memory manager. If the memory manager returns
  ///        some data, a new storage slot is created to store the data and the
  ///        request, and we start the reception. Otherwise, the request
  ///        remains in this queue until some memory is available.
  void processCreateDataQueue() {
    size_t queueSize = 0;

    for (rank_t rank = 0; rank < this->nbProcesses(); ++rank) {
      for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
        auto &queue = this->createDataOps_(rank, typeId);
        queueSize += queue.size();
        for (auto it = queue.begin(); it != queue.end();) {
          if (recvData(*it)) {
            it = queue.remove(it);
          } else {
            it++;
          }
        }
      }
    }
    this->profiler_.updateProfiledSize(ProfiledSize::MaxCreateDataQueueSize, queueSize);
  }

  /// @brief Process the recv operations queue. When all the buffers for a
  ///        particular data have arrived, the storage slot is destroyed and the
  ///        data is transferred to the result of the graph using `addResult`.
  /// @param addResult Function that allow to call `task->addResult`
  void processRecvOpsQueue(auto addResult) {
    this->profiler_.updateProfiledSize(ProfiledSize::MaxRecvOpsSize, this->recvOps_.size());
    this->profiler_.updateProfiledSize(ProfiledSize::MaxRecvStorageSize, this->wh_.recvStorageSize);

    for (auto it = this->recvOps_.begin(); it != this->recvOps_.end();) {
      if (this->service_->requestCompleted(it->request)) {
        StorageSlot<TM> &storage = this->wh_.recvStorage(it->source, it->typeId).at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount == storage.ttlBufferCount) {
          postRecv(storage, addResult, it->profileId);
          this->wh_.removeRecvStorageSlot(it->source, it->typeId, it->storageId);
        }
        this->service_->requestRelease(it->request);
        it = this->recvOps_.remove(it);
      } else {
        it++;
      }
    }
  }

  /// @brief Probe the network.
  /// @param header Reference to a header that is set when the probe is
  ///               successful.
  void probeIncommingRequests(Header &header) {
    Request request = this->service_->probe(this->channel_, true);

    if (this->service_->probeSuccess(request)) {
      header = this->service_->requestHeader(request);
      header.channel = this->channel_;
      assert(header.source < this->nbProcesses());
      assert(header.source != this->rank());
      registerNewRequest(header, request);
    }
  }

  /// @brief Try to create a storage slot for the data. If a storage slot can
  ///        be created, the reception of all the buffers is started.
  /// @param header Header of the request fetched by the probe.
  /// @return True if the reception has started.
  bool recvData(CreateDataOperation const &op) {
    Header header = op.header;
    auto storageId = tryCreateRecvStorage(header);

    if (!storageId.has_value()) {
        return false;
    }

    StorageSlot<TM> &storage = this->wh_.recvStorage(header.source, header.typeId).at(*storageId);
    for (header.bufferId = 0; header.bufferId < storage.package.bufferCount; ++header.bufferId) {
      if (op.receivedRequests[header.bufferId] == nullptr) {
          continue;
      }
      addRecvOpAndUpdateStorage(header, *storageId, op.receivedRequests[header.bufferId]);
    }
    return true;
  }

  /// @brief Process the data after the reception is completed.
  /// @param storage   Storage slot.
  /// @param addResult Function that allow transferring the data to rest of the
  ///                  graph (calls `task->addResult`).
  void postRecv(StorageSlot<TM> &storage, auto addResult, size_t profileId) {
    assert(storage.typeId < TM::size);
    TM::apply(storage.typeId, [&]<typename T>() {
      time_t tunpackingStart, tunpackingEnd;
      auto data = std::move(std::get<std::shared_ptr<T>>(storage.data));
      tunpackingStart = std::chrono::system_clock::now();
      unpack(std::move(storage.package), data);
      tunpackingEnd = std::chrono::system_clock::now();
      this->profiler_.registerUnpackTime(storage.typeId, computeDuration(tunpackingStart, tunpackingEnd));
      this->profiler_.postRecv(profileId, storage.package.size());
      if constexpr (requires { data->postRecv(); }) {
        data->postRecv();
      }
      addResult(std::move(data));
    });
  }

  // @brief lookup for existing storage
  //
  // MPI guaranties the reception order for the same communicator/rank/tag.
  // Storage lines are organized by rank and type id, and the underlying MPI
  // tag is composed by the type id and the buffer id. This means that for a
  // same storage line, the order of reception of the buffers with the same id
  // is guaranteed. Here, we iterate the queue in the order of reception, and
  // we look for the first package that misses the newly received buffer.
  //
  // @param header Header of the request to lookup.
  // @return Optional storage id of the storage slot that correspond to the
  //         request.
  std::optional<StorageId> recvStorageLookup(Header const &header) {
    auto &storageLine = this->wh_.recvStorage(header.source, header.typeId);
    for (auto it = storageLine.begin(); it != storageLine.end(); it++) {
      if (!it->receivedBuffers.test(header.bufferId)) {
        return it.idx;
      }
    }
    return std::nullopt;
  }

  /// @brief Try to allocated a new data using the memory manager. If the
  ///        memory manager returns a valid data, creates a new storage slot in
  ///        the warehouse.
  /// @return True and the storage id if the storage slot was created, false
  ///         and 0 otherwise.
  std::optional<StorageId> tryCreateRecvStorage(Header const &header) {
    std::optional<StorageId> storageId = std::nullopt;
    assert(header.typeId < TM::size);
    TM::apply(header.typeId, [&]<typename T>() {
      auto data = this->mm_->template allocate<T>();
      if (data == nullptr) {
        return;
      }
      if constexpr (requires { data->preRecv(); }) {
        data->preRecv();
      }
      storageId = this->wh_.addRecvStorageSlot(
              std::move(data), header.source,
              this->profiler_.preRecv(header.typeId, header.source));
    });
    return storageId;
  }

    /******************************************************************************/
    /*                                   flush                                    */
    /******************************************************************************/

  /// @brief Cancel the remaining received requests and clear the queue.
  ///
  /// Note: with the current implementation, the receiver is forced to receiver
  /// all the requests, which means when this function is called, the queues
  /// are necessarily empty. However, this function may be more useful if this
  /// behavior changes.
  void flush() {
    for (auto &op : this->recvOps_) {
      this->service_->requestCancel(op.request);
    }
    this->recvOps_.clear();
    for (rank_t source = 0; source < this->nbProcesses(); ++source) {
      for (type_id_t typeId = 0; typeId < TM::size; ++typeId) {
        this->createDataOps_(source, typeId).clear();
        auto &recvStorageQueue = this->wh_.recvStorage(source, typeId);
        for (auto &storage : recvStorageQueue) {
          assert(storage.typeId < TM::size);
          TM::apply(storage.typeId, [&]<typename T>() {
            this->mm_->release(std::move(std::get<std::shared_ptr<T>>(storage.data)));
          });
        }
        recvStorageQueue.clear();
      }
    }
    this->sendQueue_.clear();
    for (auto &op : this->sendOps_) {
      this->service_->requestCancel(op.request);
    }
    this->sendOps_.clear();
  }

  /******************************************************************************/
  /*                                 attributes                                 */
  /******************************************************************************/

private:
  CommService *service_ = nullptr; ///< Pointer to the service implementation.
  channel_t    channel_ = 0;       ///< Channel id.

  // progress loop data
  std::atomic<bool>        fini_ = false;    ///< Termination flag.

  // queues
  std::mutex                        sendQueueMutex_; ///< mutex for the send queue.
  Queue<SendRequest>                sendQueue_;      ///< Queue used to limit the number of send operations.
  Queue<CommOperation>              sendOps_;        ///< Queue of send operations.
  Table<Queue<CreateDataOperation>> createDataOps_;  ///< Queue of create data operation (wait for available memory).
  Queue<CommOperation>              recvOps_;        ///< Queue of recv operations.

  // packages
  PackageWarehouse<TM> wh_; ///< Package warehouse that stores the data during transmission.

  // memory manager
  tool::MemoryManager<Types...> *mm_ = nullptr; ///< Pointer to the memory manager.

  size_t sendThreshold_ = 0;       ///< Send threshold.

  // profiler
  CommunicatorProfiler profiler_;
};

} // end namespace comm

} // end namespace hh

#endif
