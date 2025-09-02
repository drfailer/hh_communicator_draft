#ifndef COMMUNICATOR_COMM_TOOLS
#define COMMUNICATOR_COMM_TOOLS
#include "../log.hpp"
#include "serializer/serialize.hpp"
#include "type_map.hpp"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <map>
#include <mpi.h>
#include <serializer/serializer.hpp>
#include <set>
#include <thread>
#include <variant>

/*
 * Hedgehog communicator backend implementation with serializer-cpp and MPI
 */

namespace hh {

namespace comm {

// Type map ////////////////////////////////////////////////////////////////////

template <typename... Types> using TypeTable = type_map::TypeMap<unsigned char, Types...>;

template <typename TM> struct variant_type;

template <typename... Types> struct variant_type<TypeTable<Types...>> {
  using type = std::variant<std::shared_ptr<Types>...>;
};

template <typename TM> using variant_type_t = typename variant_type<TM>::type;

// Comm ////////////////////////////////////////////////////////////////////////

struct CommHandle {
  int rank;
  int nbProcesses;
  unsigned char idGenerator;
  MPI_Comm comm;
  std::mutex mpiMutex;
  bool collectStats;
};

inline CommHandle *commCreate(bool collectStats = false) {
  CommHandle *handle;
  handle = new CommHandle();
  handle->rank = -1;
  handle->nbProcesses = -1;
  handle->idGenerator = 0;
  handle->comm = MPI_COMM_WORLD;
  handle->collectStats = collectStats;
  MPI_Comm_rank(handle->comm, &handle->rank);
  MPI_Comm_size(handle->comm, &handle->nbProcesses);
  return handle;
}

inline void commDestroy(CommHandle *handle) { delete handle; }

// transmission data types /////////////////////////////////////////////////////

using u64 = size_t;
using u32 = unsigned int;
using u16 = unsigned short;
using u8 = unsigned char;

struct Buffer {
  char *mem;
  u64 len;
};

struct Package {
  std::vector<Buffer> data;
};

struct Header {
  u8 channel : 8;     // 255 channels (number of CommTasks)
  u8 signal : 1;      // 0 -> data | 1 -> signal
  u8 typeId : 6;      // 64 types (number of types managed by one task)
  u16 packageId : 14; // 16384 packages
  u8 bufferId : 2;    // 4 buffers per package
};
static_assert(sizeof(Header) <= sizeof(u32));

struct StorageId {
  u32 source;
  u16 packageId;
};
static_assert(sizeof(StorageId) == sizeof(u64));

inline bool operator<(StorageId const &lhs, StorageId const &rhs) {
  return std::bit_cast<u64>(lhs) < std::bit_cast<u64>(rhs);
}

struct CommOperation {
  u16 packageId;
  u8 bufferId;
  MPI_Request request;
  StorageId storageId;
};

struct CommPendingRecvData {
  int source;
  Header header;
};

inline bool operator<(CommPendingRecvData const &lhs, CommPendingRecvData const &rhs) {
  static_assert(sizeof(CommPendingRecvData) == sizeof(u64));
  return std::bit_cast<u64>(lhs) < std::bit_cast<u64>(rhs);
}

struct CommQueues {
  std::vector<CommOperation> sendOps;
  std::vector<CommOperation> recvOps;
  std::set<CommPendingRecvData> recvDataQueue;
  std::mutex mutex;
};

template <typename TM> struct PackageStorage {
  Package package;
  u64 bufferCount;
  u64 ttlBufferCount;
  u8 typeId;
  variant_type_t<TM> data;
};

template <typename TM> struct PackageWarehouse {
  std::map<StorageId, PackageStorage<TM>> sendStorage;
  std::map<StorageId, PackageStorage<TM>> recvStorage;
  std::mutex mutex;
};

enum class CommSignal : unsigned char {
  None,
  Data,
  Disconnect,
};

// CommTaskHandle //////////////////////////////////////////////////////////////

/*
 * Stats we want:
 * - max send/recv queue size
 * - max send/recv storage size
 * - average transmission time
 * - average transmission time per data type
 * - average transmission time per source
 * - average wait time (no signal received/no element in the queue)
 * - packing/unpacking time per type
 * - memory manager related data...
 */

using time_t = std::chrono::time_point<std::chrono::system_clock>;
using delay_t = std::chrono::duration<double>;

struct StorageInfo {
  time_t sendtp;
  time_t recvtp;
  delay_t packingTime;
  delay_t unpackingTime;
  size_t packingCount;
  size_t unpackingCount;
  u8 typeId;
  size_t dataSize;
};

// TODO: we may have to define a limit on the size of storageStats
struct CommTaskStats {
  std::map<StorageId, StorageInfo> storageStats;
  size_t maxSendOpsSize;
  size_t maxRecvOpsSize;
  size_t maxRecvDataQueueSize;
  size_t maxSendStorageSize;
  size_t maxRecvStorageSize;
  std::mutex mutex;
};

template <typename TM> struct CommTaskHandle {
  CommHandle *comm;
  unsigned char channel;
  std::vector<int> receivers;
  CommQueues queues;
  PackageWarehouse<TM> wh;
  CommTaskStats stats;
};

template <typename TM> CommTaskHandle<TM> *commTaskHandleCreate(CommHandle *handle, std::vector<int> const &receivers) {
  CommTaskHandle<TM> *taskHandle = new CommTaskHandle<TM>();
  taskHandle->comm = handle;
  taskHandle->channel = ++handle->idGenerator;
  taskHandle->receivers = receivers;
  taskHandle->stats.maxSendOpsSize = 0;
  taskHandle->stats.maxRecvOpsSize = 0;
  taskHandle->stats.maxRecvDataQueueSize = 0;
  taskHandle->stats.maxSendStorageSize = 0;
  taskHandle->stats.maxRecvStorageSize = 0;
  return taskHandle;
}

template <typename TM> void commTaskHandleDestroy(CommTaskHandle<TM> *taskHandle) { delete taskHandle; }

// Packing functions ///////////////////////////////////////////////////////////

template <typename T> Package commPack(std::shared_ptr<T> data) {
  Package package;

  if constexpr (requires { data->pack(); }) {
    package.data = data->pack();
  } else {
    serializer::Bytes bytes(64, 64);
    size_t size = serializer::serialize<serializer::Serializer<serializer::Bytes>>(bytes, 0, data);
    Buffer buf = Buffer{.mem = bytes.dropMem<char *>(), .len = size};
    package.data.push_back(buf);
  }
  return package;
}

template <typename T> Package commPackage(std::shared_ptr<T> data) {
  if constexpr (requires { data->package(); }) {
    return data->package();
  } else {
    return Package{.data = std::vector<Buffer>({Buffer{new char[64], 64}})};
  }
}

template <typename T> void commUnpack(Package package, std::shared_ptr<T> data) {
  if constexpr (requires { data->unpack(); }) {
    data->unpack(package);
  } else {
    serializer::Bytes bytes(std::bit_cast<std::byte *>(package.data.front().mem), package.data.front().len,
                            package.data.front().len);
    serializer::deserialize<serializer::Serializer<serializer::Bytes>>(bytes, 0, data);
  }
}

inline u16 commGeneratePackageId() {
  static u16 curPackageId = 0;
  u16 result = curPackageId;
  curPackageId = (curPackageId + 1) & 0b0011111111111111; // update the id and make sure it stays on 14 bits
  return result;
}

// Helper //////////////////////////////////////////////////////////////////////

template <typename TM>
void commFlushQueueAndWarehouse(CommTaskHandle<TM> *handle, std::vector<CommOperation> &queue,
                                std::map<StorageId, PackageStorage<TM>> &wh) {
  std::vector<MPI_Request> requests;

  for (auto &op : queue) {
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    MPI_Cancel(&op.request);
    requests.push_back(op.request);
  }

  int done = false;
  do {
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(4ms);
    MPI_Testall(requests.size(), requests.data(), &done, MPI_STATUSES_IGNORE);
  } while (!done);
  queue.clear();

  for (auto it : wh) {
    auto storage = it.second;
    type_map::apply(TM(), storage.typeId, [&]<typename T>() {
      if constexpr (!requires(T *data) { data->pack(); }) {
        delete[] storage.package.data[0].mem;
      }
    });
  }
  wh.clear();
}

template <typename TM, typename CreateDataCB>
bool commCreateRecvStorage(CommTaskHandle<TM> *handle, StorageId storageId, u8 typeId, CreateDataCB createData) {
  bool status = true;

  type_map::apply(TM(), typeId, [&]<typename T>() {
    auto data = createData.template operator()<T>();

    if (data == nullptr) {
      status = false;
      return;
    }
    auto package = commPackage(data);
    PackageStorage<TM> storage{
        .package = package,
        .bufferCount = 0,
        .ttlBufferCount = package.data.size(),
        .typeId = typeId,
        .data = data,
    };
    handle->wh.recvStorage.insert({storageId, storage});
  });

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.storageStats.insert({storageId, {}});
  }
  return status;
}

// Send ////////////////////////////////////////////////////////////////////////

inline void commSend(CommHandle *handle, Header const &header, int dest, Buffer const &buf) {
  MPI_Send(buf.mem, buf.len, MPI_BYTE, dest, std::bit_cast<int>(header), handle->comm);
}

template <typename RT>
void commSendAsync(CommHandle *handle, Header const &header, int dest, Buffer const &buf, RT request) {
  MPI_Isend(buf.mem, buf.len, MPI_BYTE, dest, std::bit_cast<int>(header), handle->comm, request);
}

template <typename TM> void commSendSignal(CommTaskHandle<TM> *handle, std::vector<int> dests, CommSignal signal) {
  Header header = {.channel = handle->channel, .signal = 1, .typeId = 0, .packageId = 0, .bufferId = 0};

  logh::infog(logh::IG::Comm, "COMM", "commSendSignal: channel = ", (int)handle->channel, " signal = ", (int)signal);
  for (auto dest : dests) {
    char buf[1] = {(char)signal};
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    commSend(handle->comm, header, dest, Buffer{buf, 1});
  }
}

template <typename T, typename TM>
void commSendData(CommTaskHandle<TM> *handle, std::vector<int> dests, std::shared_ptr<T> data) {
  time_t tpackingStart, tpackingEnd;
  u16 packageId = commGeneratePackageId();
  Header header = {
      .channel = handle->channel,
      .signal = 0,
      .typeId = type_map::get_id<T>(TM()),
      .packageId = packageId,
      .bufferId = 0,
  };

  // measure data packing time
  tpackingStart = std::chrono::system_clock::now();
  Package package = commPack(data);
  tpackingEnd = std::chrono::system_clock::now();

  // create the storage data
  PackageStorage<TM> storage = {
      .package = package,
      .bufferCount = 0,
      .ttlBufferCount = package.data.size() * dests.size(),
      .typeId = header.typeId,
      .data = data,
  };
  StorageId storageId = {
      .source = (u32)handle->comm->rank,
      .packageId = packageId,
  };

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.storageStats.insert({storageId, StorageInfo{}});
    handle->stats.storageStats.at(storageId).typeId = header.typeId;
    handle->stats.storageStats.at(storageId).packingCount += 1;
    handle->stats.storageStats.at(storageId).packingTime += tpackingEnd - tpackingStart;

    size_t dataSize = 0;
    for (auto buffer : package.data) {
        dataSize += buffer.len;
    }
    handle->stats.storageStats.at(storageId).dataSize = dataSize;
  }

  assert(package.data.size() <= 4);
  logh::infog(logh::IG::Comm, "COMM", "commSendData: channel = ", (int)handle->channel, " typeId = ", (int)get_id<T>(TM()),
              " requestId = ", (int)header.packageId);

  handle->wh.mutex.lock();
  handle->wh.sendStorage.insert({storageId, storage});
  handle->wh.mutex.unlock();

  // TODO: how do we want to order the loops?
  for (size_t i = 0; i < package.data.size(); ++i) {
    header.bufferId = (u8)i;
    for (auto dest : dests) {
      std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
      handle->queues.sendOps.push_back(CommOperation{
          .packageId = packageId,
          .bufferId = header.bufferId,
          .request = {},
          .storageId = storageId,
      });
      std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
      commSendAsync(handle->comm, header, dest, package.data[i], &handle->queues.sendOps.back().request);
    }
  }
}

template <typename TM, typename ReturnDataCB>
void commProcessSendOpsQueue(CommTaskHandle<TM> *handle, ReturnDataCB cb, bool flush = false) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    // lock wh?
    handle->stats.maxSendOpsSize = std::max(handle->stats.maxSendOpsSize, handle->queues.sendOps.size());
    handle->stats.maxSendStorageSize = std::max(handle->stats.maxSendStorageSize, handle->wh.sendStorage.size());
  }

  do {
    for (auto it = handle->queues.sendOps.begin(); it != handle->queues.sendOps.end();) {
      int flag = 0;
      MPI_Status status;
      std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
      MPI_Test(&it->request, &flag, &status);

      if (flag) {
        std::lock_guard<std::mutex> whLock(handle->wh.mutex);
        assert(handle->wh.sendStorage.contains(it->storageId));
        PackageStorage<TM> &storage = handle->wh.sendStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount >= storage.ttlBufferCount) {
          type_map::apply(TM(), storage.typeId, [&]<typename T>() {
            if constexpr (!requires(T *data) { data->pack(); }) {
              // the buffer is dynamically allocated when the data type does not
              // support the 'pack' operation
              delete[] storage.package.data[0].mem;
            }
            cb.template operator()<T>(std::get<std::shared_ptr<T>>(storage.data));
          });
          handle->wh.sendStorage.erase(it->storageId);

          if (handle->comm->collectStats) {
            std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
            handle->stats.storageStats.at(it->storageId).sendtp = std::chrono::system_clock::now();
          }
        }
        handle->queues.sendOps.erase(it);
      } else {
        it++;
      }
    }
  } while (flush && !handle->queues.sendOps.empty());
}

// Recv ////////////////////////////////////////////////////////////////////////

inline void commRecv(CommHandle *handle, int source, int tag, Buffer const &buf, MPI_Status *status) {
  MPI_Recv(buf.mem, buf.len, MPI_BYTE, source, tag, handle->comm, status);
}

inline void commRecvAsync(CommHandle *handle, int source, int tag, Buffer const &buf, MPI_Request *request) {
  MPI_Irecv(buf.mem, buf.len, MPI_BYTE, source, tag, handle->comm, request);
}

template <typename TM>
void commRecvSignal(CommTaskHandle<TM> *handle, int &source, CommSignal &signal, Header &header) {
  MPI_Status status;
  int flag = false;

  signal = CommSignal::None;
  source = -1;
  std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
  MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, handle->comm->comm, &flag, &status);

  if (flag) {
    header = *std::bit_cast<Header *>(&status.MPI_TAG);

    if (header.channel != handle->channel) {
      return;
    }

    if (header.signal == 0) {
      std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
      handle->queues.recvDataQueue.insert(CommPendingRecvData{
          .source = status.MPI_SOURCE,
          .header = header,
      });
      signal = CommSignal::Data;
    } else {
      char buf[1];
      // TODO: do we want async here?
      commRecv(handle->comm, status.MPI_SOURCE, status.MPI_TAG, Buffer{buf, 1}, &status);
      signal = (CommSignal)buf[0];
    }
    source = status.MPI_SOURCE;
    logh::infog(logh::IG::Comm, "COMM", "commRecvSignal: source = ", status.MPI_SOURCE,
                " channel = ", (int)handle->channel, " signal = ", (int)signal);
  }
}

template <typename TM, typename CreateDataCB>
bool commRecvData(CommTaskHandle<TM> *handle, CommPendingRecvData const &prd, CreateDataCB createData) {
  auto packageId = prd.header.packageId;
  auto bufferId = prd.header.bufferId;
  auto typeId = prd.header.typeId;
  auto storageId = StorageId{
      .source = (u32)prd.source,
      .packageId = packageId,
  };

  logh::infog(logh::IG::Comm, "COMM", "commRecvData: channel = ", (int)handle->channel, " typeId = ", (int)typeId,
              " requestId = ", (int)packageId, " bufferId = ", (int)bufferId);

  std::lock_guard<std::mutex> whLock(handle->wh.mutex);
  if (!handle->wh.recvStorage.contains(storageId)) {
    if (!commCreateRecvStorage(handle, storageId, typeId, createData)) {
      logh::infog(logh::IG::Comm, "COMM", "commCreateRecvStorage returned false");
      return false;
    }
  }
  auto &storage = handle->wh.recvStorage.at(storageId);
  handle->queues.recvOps.push_back(CommOperation{
      .packageId = packageId,
      .bufferId = bufferId,
      .request = {},
      .storageId = storageId,
  });
  std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
  commRecvAsync(handle->comm, prd.source, std::bit_cast<int>(prd.header), storage.package.data[bufferId],
                &handle->queues.recvOps.back().request);
  return true;
}

template <typename TM, typename CreateDataCB>
void commProcessRecvDataQueue(CommTaskHandle<TM> *handle, CreateDataCB createData) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.maxRecvDataQueueSize =
        std::max(handle->stats.maxRecvDataQueueSize, handle->queues.recvDataQueue.size());
  }

  for (auto it = handle->queues.recvDataQueue.begin(); it != handle->queues.recvDataQueue.end();) {
    if (commRecvData(handle, *it, createData)) {
      handle->queues.recvDataQueue.erase(it++);
    } else {
      it++;
    }
  }
}

template <typename TM, typename ProcessCB>
void commProcessRecvOpsQueue(CommTaskHandle<TM> *handle, ProcessCB cb, bool flush = false) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
  for (auto it = handle->queues.recvOps.begin(); it != handle->queues.recvOps.end();) {
    int flag = 0;
    MPI_Status status;
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    MPI_Test(&it->request, &flag, &status);

    if (handle->comm->collectStats) {
      std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
      handle->stats.maxRecvOpsSize = std::max(handle->stats.maxRecvOpsSize, handle->queues.recvOps.size());
      handle->stats.maxRecvStorageSize = std::max(handle->stats.maxRecvStorageSize, handle->wh.recvStorage.size());
    }

    if (flag) {
      std::lock_guard<std::mutex> whLock(handle->wh.mutex);
      assert(handle->wh.recvStorage.contains(it->storageId));
      auto &storage = handle->wh.recvStorage.at(it->storageId);
      ++storage.bufferCount;

      if (storage.bufferCount >= storage.ttlBufferCount) {
        logh::infog(logh::IG::Comm, "COMM", "commProcessRecvDataQueue: unpacking data");
        time_t tunpackingStart, tunpackingEnd;
        type_map::apply(TM(), storage.typeId, [&]<typename T>() {
          auto data = std::get<std::shared_ptr<T>>(storage.data);
          tunpackingStart = std::chrono::system_clock::now();
          commUnpack(storage.package, data);
          tunpackingEnd = std::chrono::system_clock::now();
          cb.template operator()<T>(data);
        });
        handle->wh.recvStorage.erase(it->storageId);

        if (handle->comm->collectStats) {
          std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
          handle->stats.storageStats.at(it->storageId).recvtp = std::chrono::system_clock::now();
          handle->stats.storageStats.at(it->storageId).unpackingTime += tunpackingEnd - tunpackingStart;
          handle->stats.storageStats.at(it->storageId).unpackingCount += 1;
        }
      }
      handle->queues.recvOps.erase(it);
    } else {
      it++;
    }
  }

  if (flush) {
    commFlushQueueAndWarehouse(handle, handle->queues.recvOps, handle->wh.recvStorage);
  }
}

// Other functions /////////////////////////////////////////////////////////////

inline void commBarrier() { MPI_Barrier(MPI_COMM_WORLD); }

inline void commInit(int argc, char **argv) {
  int32_t provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
}

inline void commFinalize() { MPI_Finalize(); }

// stats exchange //////////////////////////////////////////////////////////////

template <typename TM> void commSendStats(CommTaskHandle<TM> *handle) {
  std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
  serializer::Bytes buf;
  Header header = {
      .channel = handle->channel,
      .signal = 0,
      .typeId = 0,
      .packageId = 0,
      .bufferId = 0,
  };

  using Serializer = serializer::Serializer<serializer::Bytes>;
  serializer::serialize<Serializer>(buf, 0, handle->stats.maxSendOpsSize, handle->stats.maxRecvOpsSize,
                                    handle->stats.maxRecvDataQueueSize, handle->stats.maxSendStorageSize,
                                    handle->stats.maxRecvStorageSize, handle->stats.storageStats);

  logh::infog(logh::IG::Stats, "STATS", "commSendStats: rank = ", handle->comm->rank, " buf size = ", buf.size());
  commSend(handle->comm, header, 0, Buffer{std::bit_cast<char *>(buf.data()), buf.size()});
}

template <typename TM> std::vector<CommTaskStats> commGatherStats(CommTaskHandle<TM> *handle) {
  int bufSize;
  std::vector<CommTaskStats> stats(handle->comm->nbProcesses);

  stats[0].storageStats = std::move(handle->stats.storageStats);
  stats[0].maxSendOpsSize = handle->stats.maxSendOpsSize;
  stats[0].maxRecvOpsSize = handle->stats.maxRecvOpsSize;
  stats[0].maxRecvDataQueueSize = handle->stats.maxRecvDataQueueSize;
  stats[0].maxSendStorageSize = handle->stats.maxSendStorageSize;
  stats[0].maxRecvStorageSize = handle->stats.maxRecvStorageSize;
  for (int i = 1; i < handle->comm->nbProcesses; ++i) {
    logh::infog(logh::IG::Stats, "STATS", "commGatherStats: rank = ", handle->comm->rank, " target = ", i);
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    MPI_Status status;
    MPI_Probe(i, MPI_ANY_TAG, handle->comm->comm, &status);
    MPI_Get_count(&status, MPI_BYTE, &bufSize);

    serializer::Bytes buf(bufSize, bufSize);
    commRecv(handle->comm, i, status.MPI_TAG, Buffer{std::bit_cast<char *>(buf.data()), buf.size()}, &status);

    using Serializer = serializer::Serializer<serializer::Bytes>;
    serializer::deserialize<Serializer>(buf, 0, stats[i].maxSendOpsSize, stats[i].maxRecvOpsSize,
                                        stats[i].maxRecvDataQueueSize, stats[i].maxSendStorageSize,
                                        stats[i].maxRecvStorageSize, stats[i].storageStats);
  }
  return stats;
}

} // end namespace comm

} // end namespace hh

#endif
