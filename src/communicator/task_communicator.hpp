#ifndef COMMUNICATOR_TASK_COMMUNICATOR
#define COMMUNICATOR_TASK_COMMUNICATOR
#include "comm_service.hpp"
#include "type_map.hpp"
#include <map>
#include <serializer/serializer.hpp>
#include <set>
#include <vector>

namespace hh {

namespace comm {

template <typename TM>
class TaskCommunicator {
public:
  TaskCommunicator(CommService *service, std::vector<std::uint32_t> const &receivers)
      : service_(service),
        channel_(service->generateId()),
        receivers_(receivers),
        packagesCount_(service->nbProcesses(), 0) {}

public:
  std::uint8_t channel() const {
    return channel_;
  }

  std::vector<std::uint32_t> const &receivers() const {
    return receivers_;
  }

  CommService *service() const {
    return service_;
  }

  CommQueues const &queues() const {
    return queues_;
  }

  CommTaskStats const &stats() const {
    return stats_;
  }

  std::vector<size_t> const &packagesCount() const {
    return packagesCount_;
  }

  std::uint32_t rank() const {
      return this->service_->rank();
  }

  std::uint32_t nbProcesses() const {
      return this->service_->nbProcesses();
  }

public:
  /*
   * Send a signal to the given destinations.
   */
  void sendSignal(std::vector<std::uint32_t> const &dests, Signal signal) {
    Header header = {
        .source = (std::uint32_t)this->service_->rank(),
        .signal = 1,
        .typeId = 0,
        .channel = this->channel_,
        .packageId = 0,
        .bufferId = 0,
    };
    char          buf[100] = {(char)signal};
    std::uint64_t len = 1;

    infog(logh::IG::Comm, "comm", "signal = ", (int)signal);
    for (auto dest : dests) {
      if (signal == Signal::Disconnect) {
        size_t size = sizeof(this->packagesCount_[dest]);
        std::memcpy(&buf[1], &this->packagesCount_[dest], size);
        len = 1 + size;
      }
      this->service_->send(header, dest, Buffer{buf, len});
    }
  }

  /*
   * Send data to the given destinations.
   */
  template <typename T>
  void sendData(std::vector<std::uint32_t> const &dests, std::shared_ptr<T> data, bool returnMemory = true) {
    auto [storageId, storage] = createSendStorage(dests, data, returnMemory);
    Header header = {
        .source = (std::uint32_t)this->service_->rank(),
        .signal = 0,
        .typeId = storage.typeId,
        .channel = this->channel_,
        .packageId = storageId.packageId,
        .bufferId = 0,
    };

    infog(logh::IG::Comm, "comm", "sendData -> ", " typeId = ", (int)get_id<T>(TM()),
          " requestId = ", (int)header.packageId);

    this->wh_.mutex.lock();
    this->wh_.sendStorage.insert({storageId, storage});
    this->wh_.mutex.unlock();

    for (auto dest : dests) {
      this->packagesCount_[dest] += 1;
      for (size_t i = 0; i < storage.package.data.size(); ++i) {
        header.bufferId = (std::uint8_t)i;
        std::lock_guard<std::mutex> queuesLock(this->queues_.mutex);
        Request                     request = this->service_->sendAsync(header, dest, storage.package.data[i]);
        this->queues_.sendOps.push_back(CommOperation{
            .packageId = storageId.packageId,
            .bufferId = header.bufferId,
            .request = request,
            .storageId = storageId,
        });
      }
    }
  }

  /*
   * Probe the network. When a valid message has arrived, if it contains a
   * signal, then receive the signal, otherwise, add a pending recv data request
   * to the queue.
   */
  void recvSignal(std::uint32_t &source, Signal &signal, Header &header, Buffer &buf) {
    std::uint64_t tag = 0;
    Request       request = this->service_->probe(this->channel_);

    signal = Signal::None;
    source = -1;

    // TODO: probe for my rank / my channel
    if (request->data.probe.result) {
      tag = request->data.probe.sender_tag;
      header = tagToHeader(tag);

      assert(header.source != this->service_->rank());

      if (header.signal == 0) {
        std::lock_guard<std::mutex> queuesLock(this->queues_.mutex);
        this->queues_.createDataQueue.insert(CommPendingRecvData{
            .source = header.source,
            .header = header,
            .request = request,
        });
        signal = Signal::Data;
      } else {
        // TODO: do we want async here?
        this->service_->recv(request, buf);
        signal = (Signal)buf.mem[0];
      }
      source = header.source;
      assert(source < this->service_->nbProcesses());
      infog(logh::IG::Comm, "comm", "recvSignal -> ", " source = ", source, " signal = ", (int)signal);
    }
  }

  /*
   * Try to create a recv storage and create a recv operation on success.
   * `createRecvStorage` fails when the memory manager (`createData`) returns
   * a nullptr (eg the pool is empty). In this case, the pending recv data
   * requests will remain in the queue util memory is available.
   */
  template <typename CreateDataCB>
  bool recvData(CommPendingRecvData const &prd, CreateDataCB createData) {
    auto packageId = prd.header.packageId;
    auto bufferId = prd.header.bufferId;
    auto typeId = prd.header.typeId;
    auto storageId = StorageId{
        .source = (std::uint32_t)prd.source,
        .packageId = packageId,
    };

    infog(logh::IG::Comm, "comm", "recvData -> ", " source = ", prd.source, " typeId = ", (int)typeId,
          " requestId = ", (int)packageId, " bufferId = ", (int)bufferId);

    std::lock_guard<std::mutex> whLock(this->wh_.mutex);
    if (!this->wh_.recvStorage.contains(storageId)) {
      if (!createRecvStorage(storageId, typeId, createData)) {
        infog(logh::IG::Comm, "comm", "createRecvStorage returned false");
        return false;
      }
    }
    auto &storage = this->wh_.recvStorage.at(storageId);
    auto *request = this->service_->recvAsync(prd.request, storage.package.data[bufferId]);
    this->queues_.recvOps.push_back(CommOperation{
        .packageId = packageId,
        .bufferId = bufferId,
        .request = request,
        .storageId = storageId,
    });
    return true;
  }

public:
  /*
   * Manages the data after send.
   */
  template <typename ReturnDataCB>
  void postSend(StorageId, PackageStorage<TM> storage, ReturnDataCB cb) {
    type_map::apply(TM(), storage.typeId, [&]<typename T>() {
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
        cb.template operator()<T>(data);
      }
    });
  }

  /*
   * Process the send operation queue.
   */
  template <typename ReturnDataCB>
  void processSendOpsQueue(ReturnDataCB returnMemory, bool flush = false) {
    std::lock_guard<std::mutex> queuesLock(this->queues_.mutex);

    if (this->service_->collectStats()) {
      std::lock_guard<std::mutex> statsLock(this->stats_.mutex);
      this->stats_.maxSendOpsSize = std::max(this->stats_.maxSendOpsSize, this->queues_.sendOps.size());
      this->stats_.maxSendStorageSize = std::max(this->stats_.maxSendStorageSize, this->wh_.sendStorage.size());
    }

    do {
      for (auto it = this->queues_.sendOps.begin(); it != this->queues_.sendOps.end();) {
        if (this->service_->request_completed(it->request)) {
          std::lock_guard<std::mutex> whLock(this->wh_.mutex);
          assert(this->wh_.sendStorage.contains(it->storageId));
          PackageStorage<TM> &storage = this->wh_.sendStorage.at(it->storageId);
          ++storage.bufferCount;

          if (storage.bufferCount >= storage.ttlBufferCount) {
            postSend(it->storageId, storage, returnMemory);
            this->wh_.sendStorage.erase(it->storageId);
          }
          this->service_->request_release(it->request);
          it = this->queues_.sendOps.erase(it);
        } else {
          it++;
        }
      }
    } while (flush && !this->queues_.sendOps.empty());
  }

  template <typename T>
  std::pair<StorageId, PackageStorage<TM>> createSendStorage(std::vector<std::uint32_t> const &dests,
                                                             std::shared_ptr<T> data, bool returnMemory) {
    std::uint16_t packageId = generatePackageId();

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
        .typeId = type_map::get_id<T>(TM()),
        .data = data,
        .returnMemory = returnMemory,
    };
    StorageId storageId = {
        .source = (std::uint32_t)this->service_->rank(),
        .packageId = packageId,
    };

    if (this->service_->collectStats()) {
      std::lock_guard<std::mutex> statsLock(this->stats_.mutex);
      this->stats_.storageStats.insert({storageId, StorageInfo{}});
      this->stats_.storageStats.at(storageId).typeId = storage.typeId;
      this->stats_.storageStats.at(storageId).packingCount += 1;
      this->stats_.storageStats.at(storageId).packingTime
          += std::chrono::duration_cast<std::chrono::nanoseconds>(tpackingEnd - tpackingStart);
      this->stats_.storageStats.at(storageId).sendtp = std::chrono::system_clock::now();
      this->stats_.storageStats.at(storageId).dataSize = package.size();
    }
    return {storageId, storage};
  }

  /*
   * Flush operation queue and remove storage entries from the warehouse.
   */
  void flushQueueAndWarehouse(std::vector<CommOperation> &queue, std::map<StorageId, PackageStorage<TM>> &wh) {
    std::vector<Request> requests;

    for (auto &op : queue) {
      logh::error("request canceled");
      this->service_->request_cancel(op.request);
      requests.push_back(op.request);
    }
    queue.clear();

    for (auto it : wh) {
      auto storage = it.second;
      type_map::apply(TM(), storage.typeId, [&]<typename T>() {
        if constexpr (!requires(T * data) { data->pack(); }) {
          delete[] storage.package.data[0].mem;
        }
      });
    }
    wh.clear();
  }

  /*
   * If the memory manager (createData) returns a valid pointer, creates a new
   * storage entry in the warehouse.
   */
  template <typename CreateDataCB>
  bool createRecvStorage(StorageId storageId, std::uint8_t typeId, CreateDataCB createData) {
    bool status = true;

    type_map::apply(TM(), typeId, [&]<typename T>() {
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
          .typeId = typeId,
          .data = data,
          .returnMemory = true,
      };
      this->wh_.recvStorage.insert({storageId, storage});
    });

    if (this->service_->collectStats()) {
      std::lock_guard<std::mutex> statsLock(this->stats_.mutex);
      this->stats_.storageStats.insert({storageId, {}});
      this->stats_.storageStats.at(storageId).typeId = typeId;
    }
    return status;
  }

  /*
   * Process the pending recv data queue.
   *
   * FIXME: for now, there is no timeout condition, therefore if the memory
   *        manager keeps returning nullptr, the program will never terminate.
   */
  template <typename CreateDataCB>
  void processRecvDataQueue(CreateDataCB createData) {
    std::lock_guard<std::mutex> queuesLock(this->queues_.mutex);

    if (this->service_->collectStats()) {
      std::lock_guard<std::mutex> statsLock(this->stats_.mutex);
      this->stats_.maxCreateDataQueueSize
          = std::max(this->stats_.maxCreateDataQueueSize, this->queues_.createDataQueue.size());
    }

    for (auto it = this->queues_.createDataQueue.begin(); it != this->queues_.createDataQueue.end();) {
      if (recvData(*it, createData)) {
        it = this->queues_.createDataQueue.erase(it);
      } else {
        it++;
      }
    }
  }

  /*
   * Process data after recv.
   */
  template <typename ProcessCB>
  void postRecv(StorageId storageId, PackageStorage<TM> storage, ProcessCB processData) {
    infog(logh::IG::Comm, "comm", "processRecvDataQueue -> unpacking data");
    time_t tunpackingStart, tunpackingEnd;
    type_map::apply(TM(), storage.typeId, [&]<typename T>() {
      auto data = std::get<std::shared_ptr<T>>(storage.data);
      tunpackingStart = std::chrono::system_clock::now();
      unpack(std::move(storage.package), data);
      tunpackingEnd = std::chrono::system_clock::now();
      processData.template operator()<T>(data);
    });

    if (this->service_->collectStats()) {
      std::lock_guard<std::mutex> statsLock(this->stats_.mutex);
      this->stats_.storageStats.at(storageId).recvtp = std::chrono::system_clock::now();
      this->stats_.storageStats.at(storageId).unpackingTime
          += std::chrono::duration_cast<std::chrono::nanoseconds>(tunpackingEnd - tunpackingStart);
      this->stats_.storageStats.at(storageId).unpackingCount += 1;
    }
  }

  /*
   * Process the recv operations. This operations are to pending MPI requests
   * that have an associated recv data storage that will store the buffers.
   */
  template <typename ProcessCB>
  void processRecvOpsQueue(ProcessCB processData, [[maybe_unused]] bool flush = false) {
    std::lock_guard<std::mutex> queuesLock(this->queues_.mutex);

    if (this->service_->collectStats()) {
      std::lock_guard<std::mutex> statsLock(this->stats_.mutex);
      this->stats_.maxRecvOpsSize = std::max(this->stats_.maxRecvOpsSize, this->queues_.recvOps.size());
      this->stats_.maxRecvStorageSize = std::max(this->stats_.maxRecvStorageSize, this->wh_.recvStorage.size());
    }

    for (auto it = this->queues_.recvOps.begin(); it != this->queues_.recvOps.end();) {
      if (this->service_->request_completed(it->request)) {
        std::lock_guard<std::mutex> whLock(this->wh_.mutex);
        assert(this->wh_.recvStorage.contains(it->storageId));
        auto &storage = this->wh_.recvStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount >= storage.ttlBufferCount) {
          postRecv(it->storageId, storage, processData);
          this->wh_.recvStorage.erase(it->storageId);
        }
        this->service_->request_release(it->request);
        it = this->queues_.recvOps.erase(it);
      } else {
        it++;
      }
    }

    // TODO: test if this works fine
    // if (flush) {
    //   flushQueueAndWarehouse(this->queues_.recvOps, this->wh_.recvStorage);
    // }
  }

public:
  template <typename... Ts>
  void infog(logh::IG ig, std::string const &name, Ts &&...args) const {
    if constexpr (sizeof...(Ts)) {
      logh::infog(ig, name, "[", (int)this->channel_, "]: rank = ", this->service_->rank(), ", ",
                  std::forward<Ts>(args)...);
    } else {
      logh::infog(ig, name, "[", (int)this->channel_, "]: rank = ", this->service_->rank());
    }
  }

public:
  /*
   * Send statistics when generating the dot file.
   */
  void sendStats() const {
    serializer::Bytes buf;
    Header            header = {
                   .source = this->service_->rank(),
                   .signal = 0,
                   .typeId = 0,
                   .channel = this->channel_,
                   .packageId = 0,
                   .bufferId = 0,
    };

    using Serializer = serializer::Serializer<serializer::Bytes>;
    serializer::serialize<Serializer>(buf, 0, this->stats_.maxSendOpsSize, this->stats_.maxRecvOpsSize,
                                      this->stats_.maxCreateDataQueueSize, this->stats_.maxSendStorageSize,
                                      this->stats_.maxRecvStorageSize, this->stats_.storageStats);
    infog(logh::IG::Stats, "stats", "sendStats -> ", " buf size = ", buf.size(),
          ", storageStats size = ", this->stats_.storageStats.size());
    this->service_->send(header, 0, Buffer{std::bit_cast<char *>(buf.data()), buf.size()});
  }

  /*
   * Gather statistics on the master rank.
   */
  std::vector<CommTaskStats> gatherStats() const {
    std::vector<CommTaskStats> stats(this->service_->nbProcesses());
    int                        bufSize;
    Header                     header = {
                            .source = 0,
                            .signal = 0,
                            .typeId = 0,
                            .channel = this->channel_,
                            .packageId = 0,
                            .bufferId = 0,
    };

    stats[0].storageStats = std::move(this->stats_.storageStats);
    stats[0].maxSendOpsSize = this->stats_.maxSendOpsSize;
    stats[0].maxRecvOpsSize = this->stats_.maxRecvOpsSize;
    stats[0].maxCreateDataQueueSize = this->stats_.maxCreateDataQueueSize;
    stats[0].maxSendStorageSize = this->stats_.maxSendStorageSize;
    stats[0].maxRecvStorageSize = this->stats_.maxRecvStorageSize;
    for (std::uint32_t i = 1; i < this->service_->nbProcesses(); ++i) {
      header.source = i;
      Request       request = this->service_->probe(this->channel_, i);
      while (!request->data.probe.result) {
        this->service_->request_release(request);
        using namespace std::chrono_literals;
        std::this_thread::sleep_for(1ms);
        request = this->service_->probe(this->channel_, i);;
      }
      bufSize = this->service_->buffer_len(request);

      serializer::Bytes buf(bufSize, bufSize);
      this->service_->recv(request, Buffer{std::bit_cast<char *>(buf.data()), buf.size()});

      using Serializer = serializer::Serializer<serializer::Bytes>;
      serializer::deserialize<Serializer>(buf, 0, stats[i].maxSendOpsSize, stats[i].maxRecvOpsSize,
                                          stats[i].maxCreateDataQueueSize, stats[i].maxSendStorageSize,
                                          stats[i].maxRecvStorageSize, stats[i].storageStats);
      infog(logh::IG::Stats, "stats", "comGather -> ", "target = ", i, " buf size = ", buf.size(),
            ", storageStats size = ", stats[i].storageStats.size());
    }
    return stats;
  }

private:
  CommService               *service_ = nullptr;
  std::uint8_t               channel_ = 0;
  std::vector<std::uint32_t> receivers_ = {};
  CommQueues                 queues_;
  PackageWarehouse<TM>       wh_;
  CommTaskStats              stats_;
  std::vector<size_t>        packagesCount_ = {};
};

} // end namespace comm

} // end namespace hh

#endif
