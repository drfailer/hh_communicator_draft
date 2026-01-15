#ifndef COMMUNICATOR_COMMUNICATOR
#define COMMUNICATOR_COMMUNICATOR
#include "package.hpp"
#include "service/comm_service.hpp"
#include "stats.hpp"
#include "type_map.hpp"
#include <cassert>
#include <map>
#include <set>
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

namespace hh {

namespace comm {

template <typename TM>
class Communicator {
public:
  Communicator(CommService *service)
      : service_(service),
        channel_(service->newChannel()),
        stats_(service->collectStats()),
        packagesCount_(service->nbProcesses(), 0) {}

public:
  channel_t channel() const {
    return channel_;
  }

  CommService *service() const {
    return service_;
  }

  CommTaskStats const &stats() const {
    return stats_;
  }

  std::vector<size_t> const &packagesCount() const {
    return packagesCount_;
  }

  rank_t rank() const {
    return this->service_->rank();
  }

  std::uint32_t nbProcesses() const {
    return this->service_->nbProcesses();
  }

  bool hasPendingOperations() const {
    return !this->sendOps_.empty() || !this->recvOps_.empty() || !this->createDataOps_.empty();
  }

  size_t nbSendOps() const {
    return this->sendOps_.size();
  }

  size_t nbRecvOps() const {
    return this->recvOps_.size();
  }

  size_t nbCreateDataOps() const {
    return this->createDataOps_.size();
  }

  /*
   * Send a signal to the given destinations.
   */
  void sendSignal(std::vector<rank_t> const &dests, Signal signal) {
    Header header(this->rank(), 1, 0, this->channel_, 0, 0);
    char   buf[100] = {(char)signal};
    size_t len = 1;

    infog(logh::IG::Comm, "comm", "signal = ", (int)signal);
    for (auto dest : dests) {
      this->service_->send(header, dest, Buffer{buf, len});
    }
  }

  /*
   * Send data to the given destinations.
   */
  template <typename T>
  void sendData(std::vector<rank_t> const &dests, std::shared_ptr<T> data, bool returnMemory = true) {
    auto [storageId, storage] = createSendStorage(dests, data, returnMemory);
    Header header(this->rank(), 0, storageId.typeId, this->channel_, storageId.packageId, 0);

    infog(logh::IG::Comm, "comm", "sendData -> ", " typeId = ", (int)TM::template idOf<T>(),
          " requestId = ", (int)header.packageId);

    this->wh_.mutex.lock();
    this->wh_.sendStorage.insert({storageId, storage});
    this->wh_.mutex.unlock();

    for (auto dest : dests) {
      this->packagesCount_[dest] += 1;
      for (size_t i = 0; i < storage.package.data.size(); ++i) {
        header.bufferId = (buffer_id_t)i;
        std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);
        Request                     request = this->service_->sendAsync(header, dest, storage.package.data[i]);
        this->sendOps_.push_back(CommOperation{
            .packageId = storageId.packageId,
            .bufferId = header.bufferId,
            .request = request,
            .storageId = storageId,
        });
      }
    }
  }

  void init() {
    this->senderPortState_ = PortState::Opened;
    this->recverPortState_ = PortState::Opened;
    this->connections_ = std::vector(this->nbProcesses(), Connection{true, 0, 0});
    this->connections_[this->rank()].connected = false;
  }

  void run(auto allocData, auto releaseData, auto onRecv, auto canTerminate) {
    init();
    while (this->senderPortState_ != PortState::Closed || this->recverPortState_ != PortState::Closed) {
      runSender(releaseData, canTerminate);
      runRecver(allocData, onRecv);
    }
  }

private:
  enum class PortState { Opened, Closing, Closed };

  void runSender(auto releaseData, auto canTerminate) {
    switch (this->senderPortState_) {
    case PortState::Opened:
      processSendOpsQueue(releaseData);
      if (canTerminate()) {
        this->senderPortState_ = PortState::Closing;
      }
      break;
    case PortState::Closing:
      this->processSendOpsQueue(releaseData, true);
      this->notifyDisconnection();
      this->processSendOpsQueue(releaseData, true);
      this->senderPortState_ = PortState::Closed;
      break;
    case PortState::Closed:
      break;
    }
  }

  void runRecver(auto allocData, auto onRecv) {
    comm::Signal signal = comm::Signal::None;
    comm::Header header = {0, 0, 0, 0, 0, 0};
    char         signalBufferMem[100] = {0};
    comm::Buffer signalBuffer{signalBufferMem, 100};

    switch (this->recverPortState_) {
    case PortState::Opened:
      recvSignal(signal, header, signalBuffer);

      switch (signal) {
      case comm::Signal::None:
        break;
      case comm::Signal::Disconnect:
        disconnect(header.source, signalBuffer);
        break;
      case comm::Signal::Data:
        ++this->connections_[header.source].recvCount;
        break;
      }
      processCreateDataQueue(allocData);
      processRecvOpsQueue(onRecv);

      if (!isConnected() && !hasPendingOperations()) {
        this->recverPortState_ = PortState::Closing;
      }
      break;
    case PortState::Closing:
      flushRecvQueueAndWarehouse();
      this->recverPortState_ = PortState::Closed;
      break;
    case PortState::Closed:
      break;
    }
  }

private:
  struct Connection {
    bool   connected;
    size_t sendCount;
    size_t recvCount;
  };

  bool isConnected() const {
    for (auto connection : this->connections_) {
      if (connection.connected || connection.recvCount < connection.sendCount) {
        return true;
      }
    }
    return false;
  }

  void disconnect(comm::rank_t source, comm::Buffer &signalBuffer) {
    assert(this->connections_[source].connected == true);
    this->connections_[source].connected = false;
    std::memcpy(&this->connections_[source].sendCount, &signalBuffer.mem[1], sizeof(size_t));
  }

  /*
   * broadcast disconnection signal
   */
  void notifyDisconnection() {
    Header header(this->rank(), 1, 0, this->channel_, 0, 0);
    char   buf[100] = {(char)Signal::Disconnect};
    size_t len = 1;

    for (size_t dest = 0; dest < this->nbProcesses(); ++dest) {
      if (dest != this->rank()) {
        size_t size = sizeof(this->packagesCount_[dest]);
        std::memcpy(&buf[1], &this->packagesCount_[dest], size);
        len = 1 + size;
        this->service_->send(header, dest, Buffer{buf, len});
      }
    }
  }

private:
  struct CommOperation {
    package_id_t packageId;
    buffer_id_t  bufferId;
    Request      request;
    StorageId    storageId;
  };

  struct CreateDataOperation {
    rank_t  source;
    Header  header;
    Request request;

    bool operator<(CreateDataOperation const &other) const {
      if (this->source == other.source) {
        return this->header < other.header;
      }
      return this->source < other.source;
    }
  };


/******************************************************************************/
/*                           send queue operations                            */
/******************************************************************************/

  /*
   * Process the send operation queue.
   */
  template <typename ReturnDataCB>
  void processSendOpsQueue(ReturnDataCB returnMemory, bool flush = false) {
    std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);

    this->stats_.updateSendQueuesInfos(this->sendOps_.size(), this->wh_.sendStorage.size());

    do {
      for (auto it = this->sendOps_.begin(); it != this->sendOps_.end();) {
        if (this->service_->requestCompleted(it->request)) {
          std::lock_guard<std::mutex> whLock(this->wh_.mutex);
          assert(this->wh_.sendStorage.contains(it->storageId));
          PackageStorage<TM> &storage = this->wh_.sendStorage.at(it->storageId);
          ++storage.bufferCount;

          if (storage.bufferCount == storage.ttlBufferCount) {
            postSend(it->storageId, storage, returnMemory);
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

  /*
   * Manages the data after send.
   */
  template <typename ReturnDataCB>
  void postSend(StorageId storageId, PackageStorage<TM> storage, ReturnDataCB cb) {
    assert(storageId.typeId < TM::size);
    TM::apply(storageId.typeId, [&]<typename T>() {
      std::shared_ptr<T> data = std::get<std::shared_ptr<T>>(storage.data);
      if constexpr (requires { data->postSend(); }) {
        data->postSend();
      }
      if constexpr (!requires { data->pack(); }) {
        // the buffer is dynamically allocated when the data type does not
        // support the 'pack' operation
        delete[] storage.package.data[0].mem;
      }
      if (storage.returnMemory) {
        cb.template operator()<T>(std::move(data));
      }
    });
  }

  template <typename T>
  std::pair<StorageId, PackageStorage<TM>> createSendStorage(std::vector<rank_t> const &dests, std::shared_ptr<T> data,
                                                             bool returnMemory) {
    package_id_t packageId = this->service_->newPackageId(this->channel_);

    // measure data packing time
    time_t  tpackingStart = std::chrono::system_clock::now();
    Package package = pack(data);
    time_t  tpackingEnd = std::chrono::system_clock::now();
    assert(package.data.size() <= 4);

    // create the storage data
    PackageStorage<TM> storage = {
        .package = package,
        .bufferCount = 0,
        .ttlBufferCount = package.data.size() * dests.size(),
        .data = data,
        .returnMemory = returnMemory,
        .dbgBufferReceived = {false, false, false, false},
    };
    StorageId storageId(this->rank(), packageId, TM::template idOf<T>());

    this->stats_.registerSendTimings(storageId, dests,
                                     std::chrono::duration_cast<std::chrono::nanoseconds>(tpackingEnd - tpackingStart),
                                     package.size());
    return {storageId, storage};
  }


/******************************************************************************/
/*                           recv queue operations                            */
/******************************************************************************/

  /*
   * Process the pending recv data queue.
   */
  template <typename CreateDataCB>
  void processCreateDataQueue(CreateDataCB createData) {
    std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);

    this->stats_.updateCreateDataQueueInfos(this->createDataOps_.size());

    for (auto it = this->createDataOps_.begin(); it != this->createDataOps_.end();) {
      if (recvData(*it, createData)) {
        it = this->createDataOps_.erase(it);
      } else {
        it++;
      }
    }
  }

  /*
   * Process the recv operations. This operations are to pending MPI requests
   * that have an associated recv data storage that will store the buffers.
   */
  template <typename ProcessCB>
  void processRecvOpsQueue(ProcessCB processData) {
    std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);

    this->stats_.updateRecvQueuesInfos(this->recvOps_.size(), this->wh_.recvStorage.size());

    for (auto it = this->recvOps_.begin(); it != this->recvOps_.end();) {
      if (this->service_->requestCompleted(it->request)) {
        std::lock_guard<std::mutex> whLock(this->wh_.mutex);
        assert(this->wh_.recvStorage.contains(it->storageId));
        auto &storage = this->wh_.recvStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount == storage.ttlBufferCount) {
          postRecv(it->storageId, storage, processData);
          this->wh_.recvStorage.erase(it->storageId);
        }
        this->service_->requestRelease(it->request);
        it = this->recvOps_.erase(it);
      } else {
        it++;
      }
    }
  }

  /*
   * Probe the network. When a valid message has arrived, if it contains a
   * signal, then receive the signal, otherwise, add a pending recv data request
   * to the queue.
   */
  void recvSignal(Signal &signal, Header &header, Buffer &signalBuffer) {
    Request request = this->service_->probe(this->channel_);

    signal = Signal::None;

    if (this->service_->probeSuccess(request)) {
      header = this->service_->requestHeader(request);
      assert(header.source != this->rank());

      if (header.signal == 0) {
        std::lock_guard<std::mutex> queuesLock(this->queuesMutex_);
        this->createDataOps_.insert(CreateDataOperation{
            .source = header.source,
            .header = header,
            .request = request,
        });
        signal = Signal::Data;
      } else {
        this->service_->recv(request, signalBuffer);
        signal = (Signal)signalBuffer.mem[0];
      }
      assert(header.source < this->nbProcesses());
      infog(logh::IG::Comm, "comm", "recvSignal -> ", " source = ", header.source, " signal = ", (int)signal);
    } else {
      this->service_->requestRelease(request);
    }
  }

  /*
   * Try to create a recv storage and create a recv operation on success.
   * `createRecvStorage` fails when the memory manager (`createData`) returns
   * a nullptr (eg the pool is empty). In this case, the pending recv data
   * requests will remain in the queue util memory is available.
   */
  template <typename CreateDataCB>
  bool recvData(CreateDataOperation const &prd, CreateDataCB createData) {
    auto      packageId = prd.header.packageId;
    auto      bufferId = prd.header.bufferId;
    auto      typeId = prd.header.typeId;
    StorageId storageId(prd.source, packageId, typeId, 0);

    infog(logh::IG::Comm, "comm", "recvData -> ", " source = ", prd.source, " typeId = ", (int)typeId,
          " requestId = ", (int)packageId, " bufferId = ", (int)bufferId);

    std::lock_guard<std::mutex> whLock(this->wh_.mutex);
    if (!this->wh_.recvStorage.contains(storageId)) {
      if (!createRecvStorage(storageId, createData)) {
        infog(logh::IG::Comm, "comm", "createRecvStorage returned false");
        return false;
      }
    }
    auto &storage = this->wh_.recvStorage.at(storageId);
    auto  request = this->service_->recvAsync(prd.request, storage.package.data[bufferId]);
    assert(storage.dbgBufferReceived[bufferId] == false);
    storage.dbgBufferReceived[bufferId] = true;
    this->recvOps_.push_back(CommOperation{
        .packageId = packageId,
        .bufferId = bufferId,
        .request = request,
        .storageId = storageId,
    });
    return true;
  }

  /*
   * Process data after recv.
   */
  template <typename ProcessCB>
  void postRecv(StorageId storageId, PackageStorage<TM> storage, ProcessCB processData) {
    infog(logh::IG::Comm, "comm", "processCreateDataQueue -> unpacking data");
    time_t tunpackingStart, tunpackingEnd;
    assert(storageId.typeId < TM::size);
    TM::apply(storageId.typeId, [&]<typename T>() {
      auto data = std::get<std::shared_ptr<T>>(storage.data);
      tunpackingStart = std::chrono::system_clock::now();
      unpack(std::move(storage.package), data);
      tunpackingEnd = std::chrono::system_clock::now();
      processData.template operator()<T>(data);
    });

    this->stats_.registerRecvTimings(
        storageId, this->service_->rank(),
        std::chrono::duration_cast<std::chrono::nanoseconds>(tunpackingEnd - tunpackingStart), storage.package.size());
  }

  /*
   * If the memory manager (createData) returns a valid pointer, creates a new
   * storage entry in the warehouse.
   */
  template <typename CreateDataCB>
  bool createRecvStorage(StorageId storageId, CreateDataCB createData) {
    bool status = true;

    assert(storageId.typeId < TM::size);
    TM::apply(storageId.typeId, [&]<typename T>() {
      auto data = createData.template operator()<T>();

      if (data == nullptr) {
        status = false;
        return;
      }
      auto               package = packageMem(data);
      PackageStorage<TM> storage{
          .package = package,
          .bufferCount = 0,
          .ttlBufferCount = package.data.size(),
          .data = data,
          .returnMemory = true,
          .dbgBufferReceived = {false, false, false, false},
      };
      this->wh_.recvStorage.insert({storageId, storage});
    });
    return status;
  }

  /*
   * Flush operation queue and remove storage entries from the warehouse.
   */
  void flushRecvQueueAndWarehouse() {
    if (!this->recvOps_.empty()) {
      logh::error("Cancelling ", this->recvOps_.size(), " recv operations.");
    }
    for (auto &op : this->recvOps_) {
      this->service_->requestCancel(op.request);
    }
    this->recvOps_.clear();

    if (!this->createDataOps_.empty()) {
      logh::error("Cancelling ", this->createDataOps_.size(), " create data operations.");
    }
    for (auto &op : this->createDataOps_) {
      this->service_->requestRelease(op.request);
    }
    this->createDataOps_.clear();

    if (!this->wh_.recvStorage.empty()) {
      logh::error("Removing ", this->wh_.recvStorage.size(), " from storage.");
    }
    for (auto it : this->wh_.recvStorage) {
      auto storageId = it.first;
      auto storage = it.second;
      assert(storageId.typeId < TM::size);
      TM::apply(storageId.typeId, [&]<typename T>() {
        if constexpr (!requires(T * data) { data->pack(); }) {
          delete[] storage.package.data[0].mem;
        }
      });
    }
    this->wh_.recvStorage.clear();
  }


/******************************************************************************/
/*                                   stats                                    */
/******************************************************************************/

public:
  /*
   * Send statistics when generating the dot file.
   */
  void sendStats() const {
    Header            header(this->rank(), 0, 0, this->channel_, 0, 0);
    std::vector<char> bufMem;
    this->stats_.pack(bufMem);
    infog(logh::IG::Stats, "stats", "sendStats -> ", " buf size = ", bufMem.size(), ", transmissionStats size = ",
          this->stats_.transmissionStats.sendInfos.size() + this->stats_.transmissionStats.recvInfos.size());
    this->service_->send(header, 0, Buffer{bufMem.data(), bufMem.size()});
  }

  /*
   * Gather statistics on the master rank.
   */
  std::vector<CommTaskStats> gatherStats() const {
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
      while (!this->service_->probeSuccess(request)) {
        this->service_->requestRelease(request);
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
        request = this->service_->probe(this->channel_, i);
      }
      bufSize = (size_t)this->service_->bufferSize(request);

      std::vector<char> bufMem(bufSize);
      Buffer            buf{.mem = bufMem.data(), .len = bufSize};
      this->service_->recv(request, buf);
      stats[i].unpack(bufMem);
      infog(logh::IG::Stats, "stats", "comGather -> ", "target = ", i, " buf size = ", buf.len,
            ", transmissionStats size = ",
            stats[i].transmissionStats.sendInfos.size() + stats[i].transmissionStats.recvInfos.size());
    }
    return stats;
  }

/******************************************************************************/
/*                                    log                                     */
/******************************************************************************/

private:
  template <typename... Ts>
  void infog(logh::IG ig, std::string const &name, Ts &&...args) const {
    if constexpr (sizeof...(Ts)) {
      logh::infog(ig, name, "[", (int)this->channel_, "]: rank = ", this->rank(), ", ", std::forward<Ts>(args)...);
    } else {
      logh::infog(ig, name, "[", (int)this->channel_, "]: rank = ", this->rank());
    }
  }


/******************************************************************************/
/*                                 attributes                                 */
/******************************************************************************/

private:
  CommService *service_ = nullptr;
  channel_t    channel_ = 0;

  // progress loop data
  PortState               senderPortState_;
  PortState               recverPortState_;
  std::vector<Connection> connections_;

  // queues
  std::vector<CommOperation>    sendOps_;
  std::vector<CommOperation>    recvOps_;
  std::set<CreateDataOperation> createDataOps_;
  std::mutex                    queuesMutex_; // the communicator is shared accross instances of a task

  // packages
  PackageWarehouse<TM> wh_;

  // stats
  CommTaskStats       stats_;
  std::vector<size_t> packagesCount_ = {};
};

} // end namespace comm

} // end namespace hh

#endif
