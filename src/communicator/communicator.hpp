#ifndef COMMUNICATOR_COMMUNICATOR
#define COMMUNICATOR_COMMUNICATOR
#include "tool/log.hpp"
#include "tool/queue.hpp"
#include "package.hpp"
#include "hints.hpp"
#include "service/comm_service.hpp"
#include "tool/memory_manager.hpp"
#include "profiling/communicator_profiler.hpp"
#include "type_map.hpp"
#include <cassert>
#include <atomic>
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
  void memoryManager(std::shared_ptr<tool::MemoryManager<Types...>> mm) {
    this->mm_ = mm;
  }

  /// @brief Add a new hint.
  /// @tparam T Type for which the hint should be used.
  /// @param hint Hint to add to the list of hints.
  template <typename T>
  void addHint(hint::Hint const &hint) {
    this->hints_.push_back(HintTracker{hint, TM::template idOf<T>()});
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
        .data = data,
        .typeId = TM::template idOf<T>(),
    });
  }

  /// @brief Initialize the communicator.
  void init() {
    this->signalBufferMem_ = std::vector<char>(1);
    this->fini_.store(false);

    // TODO: we should post a recv for the disconnection signal here

    initHints();
  }

  /// @brief Run the communicator (should be called in a dedicated thread).
  /// @param addResult Function that gives access to the `task->addResult`
  ///                  method (this is used when receiving data).
  void run(auto addResult) {
    comm::Signal signal = comm::Signal::None;
    comm::Header header = {0, 0, 0, 0, 0};

    while (!this->fini_.load()) {
      processSendOpsQueue(addResult);
      recvSignal(signal, header);
      assert(signal != comm::Signal::Disconnect);
      processCreateDataQueue();
      processRecvOpsQueue(addResult);
      progressHints();
    }
    finalizeHints();
    flush();
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
    this->service_->waitForTermination();
    this->fini_.store(true);
    this->profiler_.compile<TM>();
    this->profiler_.template generateTransmissionFile<TM>(this->channel());
  }

private:
  /// @brief Forward declaration of HintTracker.
  struct HintTracker;

  /// @brief Structure used in the send queue when the send threshold is set.
  struct SendRequest {
    std::vector<rank_t> dests; ///< List of destination ranks.
    variant_type_t<TM> data;   ///< Data to send.
    type_id_t typeId;          ///< Type id of the data to send.
  };

  /// @brief Structure that contains information about the communication requests.
  struct CommOperation {
    buffer_id_t  bufferId;  ///< Id of the buffer (buffers are sent/received separately).
    Request      request;   ///< Request.
    StorageId    storageId; ///< Id of the storage.
    size_t       profileId; ///< Id for the profile info queue.
    int          hint;      ///< Hint tracker pointer.
  };

  /// @brief Returns if the given queue has pending operations (do not count the
  ///        hinted requests).
  /// @param queue Queue to test.
  /// @return True if the queue has non hinted pending operations, false otherwise.
  bool queueHasPendingOperations(Queue<CommOperation> const &queue) const {
    bool result = false;

    for (auto const &op : queue) {
      if (op.hint == -1) {
        return true;
      }
    }
    return result;
  }

  /******************************************************************************/
  /*                           send queue operations                            */
  /******************************************************************************/

  /// @brief Package and send `data` to the given destintations.
  /// @tparam T Type of the data to send.
  /// @param dests Destination ranks.
  /// @param data  Data to send.
  template <typename T>
  void processSendRequest(std::vector<rank_t> const &dests, std::shared_ptr<T> data) {
    auto [storage, packingTime] = createSendStorage(dests, data);
    Header          header(this->rank(), 0, storage.typeId, this->channel_, 0);
    StorageId       storageId = this->wh_.sendStorage.add(storage);

    for (auto dest : dests) {
      if (dest == this->rank()) {
        continue;
      }
      size_t bufferCount = storage.package.data.size();
      size_t profileId = profiler_.preSend(storage.typeId, dest, packingTime, bufferCount);
      for (size_t i = 0; i < bufferCount; ++i) {
        header.bufferId = (buffer_id_t)i;
        this->sendOps_.add(CommOperation{
            .bufferId = header.bufferId,
            .request = this->service_->sendAsync(header, dest, storage.package.data[i]),
            .storageId = storageId,
            .profileId = profileId,
            .hint = -1,
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
        processSendRequest(it->dests, std::get<std::shared_ptr<T>>(it->data));
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
  void postSend(StorageSlot<TM> const &storage, auto addResult) {
    assert(storage.typeId < TM::size);
    TM::apply(storage.typeId, [&]<typename T>() {
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
  std::pair<StorageSlot<TM>, delay_t> createSendStorage(std::vector<rank_t> const &dests, std::shared_ptr<T> data) {
    bool   useAddResult = std::find(dests.begin(), dests.end(), this->rank()) != dests.end();
    size_t nbDests = useAddResult ? dests.size() - 1 : dests.size();

    // measure data packing time
    time_t  tpackingStart = std::chrono::system_clock::now();
    Package package = pack(data);
    time_t  tpackingEnd = std::chrono::system_clock::now();
    assert(package.data.size() <= 4);

    // create the storage slot
    StorageSlot<TM> storage = {
        .source = this->rank(),
        .package = package,
        .bufferCount = 0,
        .ttlBufferCount = package.data.size() * nbDests,
        .data = data,
        .typeId = TM::template idOf<T>(),
        .useAddResult = useAddResult,
        .dbgBufferReceived = {false, false, false, false},
    };
    return std::make_pair(storage, std::chrono::duration_cast<std::chrono::nanoseconds>(tpackingEnd - tpackingStart));
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
    this->profiler_.updateProfiledSize(ProfiledSize::MaxCreateDataQueueSize,
                                      this->createDataOps_.size());

    for (auto it = this->createDataOps_.begin(); it != this->createDataOps_.end();) {
      if (recvData(*it)) {
        it = this->createDataOps_.remove(it);
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
    this->profiler_.updateProfiledSize(ProfiledSize::MaxRecvOpsSize, this->recvOps_.size());
    this->profiler_.updateProfiledSize(ProfiledSize::MaxRecvStorageSize, this->wh_.recvStorage.size());

    for (auto it = this->recvOps_.begin(); it != this->recvOps_.end();) {
      if (this->service_->requestCompleted(it->request)) {
        StorageSlot<TM> &storage = this->wh_.recvStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount == storage.ttlBufferCount) {
          postRecv(storage, addResult, it->profileId);
          if (it->hint != -1) {
            hintRequestCompleted(it->hint);
          }
          this->wh_.recvStorage.remove(it->storageId);
        }
        this->service_->requestRelease(it->request);
        it = this->recvOps_.remove(it);
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
        this->profiler_.incrementProfiledCounter(ProfiledCounter::ProbedRequest);
        this->createDataOps_.add(header);
        signal = Signal::Data;
        this->service_->requestRelease(request);
      } else {
        comm::Buffer buf{this->signalBufferMem_.data(), this->signalBufferMem_.size()};
        this->service_->recv(request, buf);
        signal = (Signal)buf.data()[0];
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
    auto [storageId, ok] = createRecvStorage(header);

    if (!ok) {
      return false;
    }

    StorageSlot<TM> &storage = this->wh_.recvStorage.at(storageId);
    for (header.bufferId = 0; header.bufferId < storage.package.data.size(); ++header.bufferId) {
      assert(storage.dbgBufferReceived[header.bufferId] == false);
      storage.dbgBufferReceived[header.bufferId] = true;
      this->recvOps_.add(CommOperation{
          .bufferId = header.bufferId,
          .request = this->service_->recvAsync(header, storage.package.data[header.bufferId]),
          .storageId = storageId,
          .profileId = this->profiler_.preRecv(header.typeId, header.source),
          .hint = hint,
      });
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
      auto data = std::get<std::shared_ptr<T>>(storage.data);
      tunpackingStart = std::chrono::system_clock::now();
      unpack(std::move(storage.package), data);
      tunpackingEnd = std::chrono::system_clock::now();
      delay_t unpackingTime = std::chrono::duration_cast<std::chrono::nanoseconds>(tunpackingEnd - tunpackingStart);
      this->profiler_.postRecv(profileId, unpackingTime, storage.package.size());

      if constexpr (requires { data->postRecv(); }) {
        data->postRecv();
      }
      addResult(data);
      // TODO: empty the std::shared_ptr<T> to reduce the reference count once done receiving.
      // TODO: this is a bandaid solution, maybe there is a better fix?
      storage.data = std::shared_ptr<T>(nullptr);
    });
  }

  /// @brief Try to allocated a new data using the memory manager. If the
  ///        memory manager returns a valid data, creates a new storage slot in
  ///        the warehouse.
  /// @return True and the storage id if the storage slot was created, false
  ///         and 0 otherwise.
  std::pair<StorageId, bool> createRecvStorage(Header const &header) {
    bool status = true;
    StorageId storageId = 0;

    assert(header.typeId < TM::size);
    TM::apply(header.typeId, [&]<typename T>() {
      auto data = this->mm_->template allocate<T>();

      if (data == nullptr) {
        status = false;
        return;
      }

      if constexpr (requires { data->preRecv(); }) {
        data->preRecv();
      }

      Package         package = packageMem(data);
      StorageSlot<TM> storage{
          .source = header.source,
          .package = package,
          .bufferCount = 0,
          .ttlBufferCount = package.data.size(),
          .data = data,
          .typeId = header.typeId,
          .useAddResult = false,
          .dbgBufferReceived = {false, false, false, false},
      };
      storageId = this->wh_.recvStorage.add(storage);
    });
    return {storageId, status};
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
    this->createDataOps_.clear();
    for (auto &storage : this->wh_.recvStorage) {
      assert(storage.typeId < TM::size);
      TM::apply(storage.typeId, [&]<typename T>() {
        auto data = std::get<std::shared_ptr<T>>(storage.data);
        this->mm_->release(std::move(data));
      });
    }
    this->wh_.recvStorage.clear();
    this->sendQueue_.clear();
    for (auto &op : this->sendOps_) {
      this->service_->requestCancel(op.request);
    }
    this->sendOps_.clear();
  }

/******************************************************************************/
/*                                   hints                                    */
/******************************************************************************/

private:
  /// @brief Tracks the number of requests for each hint.
  struct HintTracker {
    hint::Hint hint;               ///< Hint.
    type_id_t typeId;              ///< Id of the data affected by the hint.
    size_t activeRequestCount = 0; ///< Number of requests in the ops queue.
    size_t postedRequestCount = 0; ///< Total number of posted requests.
  };

  /// @brief Notifies a hint tracker that one of its requests has been completed.
  /// @param hintIdx Index of the hint tracker in the hint list.
  void hintRequestCompleted(int hintIdx) {
    assert(hintIdx >= 0);
    auto &hint = this->hints_[hintIdx];
    hint.activeRequestCount -= 1;
    this->profiler_.incrementProfiledCounter(ProfiledCounter::HintedRequest);
  }

  /// @brief Initialize the hints.
  void initHints() {
    for (int hintIdx = 0; hintIdx < (int)this->hints_.size(); ++hintIdx) {
      switch (this->hints_[hintIdx].hint.type) {
      case hint::HintType::RecvCountFrom: {
        auto &hint = this->hints_[hintIdx].hint.data.recvCountFrom;
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
        auto &hint = this->hints_[hintIdx].hint.data.continuousRecvFrom;
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

  /// @brief Progress the hints.
  void progressHints() {
    for (int hintIdx = 0; hintIdx < (int)this->hints_.size(); ++hintIdx) {
      switch (this->hints_[hintIdx].hint.type) {
      case hint::HintType::RecvCountFrom: {
        auto &hint = this->hints_[hintIdx].hint.data.recvCountFrom;
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
        auto &hint = this->hints_[hintIdx].hint.data.continuousRecvFrom;
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

  /// @brief Finalize the hints.
  void finalizeHints() {}

  /******************************************************************************/
  /*                                 attributes                                 */
  /******************************************************************************/

private:
  CommService *service_ = nullptr; ///< Pointer to the service implementation.
  channel_t    channel_ = 0;       ///< Channel id.

  // progress loop data
  std::atomic<bool>        fini_ = false;    ///< Termination flag.

  // queues
  std::mutex           sendQueueMutex_; ///< mutex for the send queue.
  Queue<SendRequest>   sendQueue_;      ///< Queue used to limit the number of send operations.
  Queue<CommOperation> sendOps_;        ///< Queue of send operations.
  Queue<Header>        createDataOps_;  ///< Queue of create data operation (wait for available memory).
  Queue<CommOperation> recvOps_;        ///< Queue of recv operations.

  // packages
  PackageWarehouse<TM> wh_; ///< Package warehouse that stores the data during transmission.

  // diconnection buffer
  std::vector<char>   signalBufferMem_; ///< Buffer memory used to send the disconnection signal.

  // memory manager
  std::shared_ptr<tool::MemoryManager<Types...>> mm_ = nullptr; ///< Pointer to the memory manager.

  // hints
  std::vector<HintTracker> hints_; ///< Hint tracker list (hint data).
  size_t sendThreshold_ = 0;       ///< Send threshold.

  // profiler
  CommunicatorProfiler profiler_;
};

} // end namespace comm

} // end namespace hh

#endif
