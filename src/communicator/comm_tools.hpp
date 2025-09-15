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
 *
 * # Current implementation:
 *
 * ## Vocabulary
 *
 * - Buffer: ptr + len
 * - Package: list of buffers (max 4 buffer per package)
 * - PackageWarehouse: save packages until transmission is completed.
 * - Storage: entry in the PackageWarehouse (storage id = packageId + source).
 * - Operations: packageId + bufferId + mpi request + storageId -> request that
 *               corresponds to a particular buffer that bellongs to a certain
 *               package stored in the PackageStorage.
 *
 * ## Send
 *
 * - The sender tasks can use the `commSendSignal` or `commSendData` functions.
 * - The `commProcessSendOpsQueue` processes the send operations queue.
 * - Sending a signal is blocking since the signal is small (1 byte).
 * - To send data, a type can implement the `pack` method. The transmission
 *   will start by creating a storage in the warehouse to store the package.
 *   Then, a new send operation will be added to the sendOps queue. The
 *   `commProcessSendOpsQueue` is called in a busy loop to remove packages that
 *   have been sent.
 *
 * ## Recv
 *
 * - The receiver tasks can use the `commRecvSignal` function.
 * - The `commProcessRecvDataQueue` and `commProcessRecvOpsQueue` can be used
 *   to process the recv queues.
 * - Receiving a signal is blocking.
 * - When receiving a data signal, a new pending recv data request is added to
 *   a queue. The `commProcessRecvDataQueue` will then try to allocate a new
 *   storage entry. If the memory manager returns a valid pointer, then a new
 *   storage entry is created and a recv operations is added to the queue
 *   (creation of the MPI request). Finally, `commProcessRecvOpsQueue`
 *   processes the recv operations. An operation is completed if all the
 *   buffers of the package are received. When it is the case, the `unpack`
 *   method can be called to process the package.
 *
 * ## Protocol detail
 *
 * - Header (MPI tag): [channel(8b), signal(1b), typeId(6b), packageId(14b), bufferId(2b)]
 * - Content (buffer): [Signal(8bits)|Data(?)]
 *
 * Header components:
 * - channel: identifier of the CommunicatorTask ------------------- [limit = 256]
 * - signal: boolean (signal or data?) ----------------------------- [NA]
 * - typeId: identifier of the transimitted type (cf type map) ----- [limit = 64]
 * - packageId: identifier of the package (cf commGeneratePackageId) [limit = 16384]
 * - bufferId: buffer index in the package ------------------------- [limit = 4]
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
  u8 padding : 1;     // must be a positive number... :D
  u8 signal : 1;      // 0 -> data | 1 -> signal
  u8 typeId : 6;      // 64 types (number of types managed by one task)
  u8 channel : 8;     // 256 channels (number of CommTasks)
  u16 packageId : 14; // 16384 packages
  u8 bufferId : 2;    // 4 buffers per package
};
static_assert(sizeof(Header) <= sizeof(u32));

enum class CommSignal : unsigned char {
  None,
  Data,
  Disconnect,
};

// Warehouse ///////////////////////////////////////////////////////////////////

struct StorageId {
  u32 source;
  u16 packageId;
};
static_assert(sizeof(StorageId) == sizeof(u64));

inline bool operator<(StorageId const &lhs, StorageId const &rhs) {
  return std::bit_cast<u64>(lhs) < std::bit_cast<u64>(rhs);
}

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

// Queues //////////////////////////////////////////////////////////////////////

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
  std::vector<CommOperation> sendOps;          // send operations (MPI_Request)
  std::vector<CommOperation> recvOps;          // recv operations (MPI_Request)
  std::set<CommPendingRecvData> recvDataQueue; // recv data queue (wait for memory manager)
  std::mutex mutex;
};

// Stats container /////////////////////////////////////////////////////////////

using time_t = std::chrono::time_point<std::chrono::system_clock>;
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

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

// Task Handle /////////////////////////////////////////////////////////////////

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

/*
 * If the data type is packable, the use the `pack` function, otherwise,
 * default to serializer.
 */
template <typename T> Package commPack(std::shared_ptr<T> data) {
  Package package;

  if constexpr (requires { data->pack(); }) {
    package = data->pack();
  } else {
    serializer::Bytes bytes(64, 64);
    size_t size = serializer::serialize<serializer::Serializer<serializer::Bytes>>(bytes, 0, data);
    Buffer buf = Buffer{.mem = bytes.dropMem<char *>(), .len = size};
    package.data.push_back(buf);
  }
  return package;
}

/*
 * If the data type is packable, it should implement a `package` method that
 * returns the package memory. Otherwise, allocate a buffer on the heap.
 */
template <typename T> Package commPackage(std::shared_ptr<T> data) {
  if constexpr (requires { data->package(); }) {
    return data->package();
  } else {
    return Package{.data = std::vector<Buffer>({Buffer{new char[64], 64}})};
  }
}

/*
 * If the data type is unpackable, call the `unpack` method, otherwise, default
 * to serializer.
 */
template <typename T> void commUnpack(Package &&package, std::shared_ptr<T> data) {
  if constexpr (requires { data->unpack(std::move(package)); }) {
    data->unpack(std::move(package));
  } else if constexpr (!requires { data->package(); }) {
    serializer::Bytes bytes(std::bit_cast<std::byte *>(package.data.front().mem), package.data.front().len,
                            package.data.front().len);
    serializer::deserialize<serializer::Serializer<serializer::Bytes>>(bytes, 0, data);
  }
}

/*
 * Generate a package id on 14 bits using a counter. Each rank will have its
 * own counter that will loop when after the 16384 package is sent.
 */
inline u16 commGeneratePackageId() {
  static u16 curPackageId = 0;
  u16 result = curPackageId;
  curPackageId = (curPackageId + 1) & 0b0011111111111111; // update the id and make sure it stays on 14 bits
  return result;
}

// Helper //////////////////////////////////////////////////////////////////////

inline void dbg_print_binary(int i) {
  std::string str = "0b00000000000000000000000000000000";
  size_t idx = 0;

  while (idx < 32) {
    str[str.size() - idx - 1] = (i >> idx) % 2 == 0 ? '0' : '1';
    ++idx;
  }
  printf("%d = %s\n", i, str.c_str());
}

inline int headerToTag(Header header) {
  int tag = 0;
  tag |= 0b01000000000000000000000000000000 & (header.signal << (2 + 14 + 8 + 6));
  tag |= 0b00111111000000000000000000000000 & (header.typeId << (2 + 14 + 8));
  tag |= 0b00000000111111110000000000000000 & (header.channel << (2 + 14));
  tag |= 0b00000000000000001111111111111100 & (header.packageId << (2));
  tag |= 0b00000000000000000000000000000011 & (header.bufferId);
  return tag;
}

inline Header tagToHeader(int tag) {
  Header header;
  header.padding = 0;
  header.signal = (tag & 0b01000000000000000000000000000000) >> (2 + 14 + 8 + 6);
  header.typeId = (tag & 0b00111111000000000000000000000000) >> (2 + 14 + 8);
  header.channel = (tag & 0b00000000111111110000000000000000) >> (2 + 14);
  header.packageId = (tag & 0b00000000000000001111111111111100) >> (2);
  header.bufferId = (tag & 0b00000000000000000000000000000011);
  return header;
}

template <typename... Ts> void commInfog(logh::IG ig, std::string const &name, auto handle, Ts &&...args) {
  if constexpr (sizeof...(Ts)) {
    logh::infog(ig, name, "[", (int)handle->channel, "]: rank = ", handle->comm->rank, ", ", std::forward<Ts>(args)...);
  } else {
    logh::infog(ig, name, "[", (int)handle->channel, "]: rank = ", handle->comm->rank);
  }
}

inline void checkMPI(int code) {
    if (code == 0) {
        return;
    }
    char error[100] = {0};
    int len = 0;
    MPI_Error_string(code, error, &len);
    logh::error("mpi error: ", std::string(error, error + len));
}

/*
 * UNUSED
 * Flush operation queue and remove storage entries from the warehouse.
 */
template <typename TM>
void commFlushQueueAndWarehouse(CommTaskHandle<TM> *handle, std::vector<CommOperation> &queue,
                                std::map<StorageId, PackageStorage<TM>> &wh) {
  std::vector<MPI_Request> requests;

  for (auto &op : queue) {
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    checkMPI(MPI_Cancel(&op.request));
    requests.push_back(op.request);
  }

  int done = false;
  do {
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(4ms);
    checkMPI(MPI_Testall(requests.size(), requests.data(), &done, MPI_STATUSES_IGNORE));
  } while (!done);
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
    handle->stats.storageStats.at(storageId).typeId = typeId;
  }
  return status;
}

// Send ////////////////////////////////////////////////////////////////////////

/*
 * Interface to MPI_Send.
 */
inline void commSend(CommHandle *handle, Header const &header, int dest, Buffer const &buf) {
  int tag = headerToTag(header);
  checkMPI(MPI_Send(buf.mem, buf.len, MPI_BYTE, dest, tag, handle->comm));
}

/*
 * Interface to MPI_Isend.
 */
template <typename RT>
void commSendAsync(CommHandle *handle, Header const &header, int dest, Buffer const &buf, RT request) {
  int tag = headerToTag(header);
  ;
  checkMPI(MPI_Isend(buf.mem, buf.len, MPI_BYTE, dest, tag, handle->comm, request));
}

/*
 * Send a signal to the given destinations.
 */
template <typename TM> void commSendSignal(CommTaskHandle<TM> *handle, std::vector<int> dests, CommSignal signal) {
  Header header = {
      .padding = 0,
      .signal = 1,
      .typeId = 0,
      .channel = handle->channel,
      .packageId = 0,
      .bufferId = 0,
  };

  comm::commInfog(logh::IG::Comm, "comm", handle, "signal = ", (int)signal);
  for (auto dest : dests) {
    char buf[1] = {(char)signal};
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    commSend(handle->comm, header, dest, Buffer{buf, 1});
  }
}

/*
 * Send data to the given destinations.
 */
template <typename T, typename TM>
void commSendData(CommTaskHandle<TM> *handle, std::vector<int> dests, std::shared_ptr<T> data) {
  time_t tpackingStart, tpackingEnd;
  u16 packageId = commGeneratePackageId();
  Header header = {
      .padding = 0,
      .signal = 0,
      .typeId = type_map::get_id<T>(TM()),
      .channel = handle->channel,
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
    handle->stats.storageStats.at(storageId).typeId = storage.typeId;
    handle->stats.storageStats.at(storageId).packingCount += 1;
    handle->stats.storageStats.at(storageId).packingTime +=
        std::chrono::duration_cast<std::chrono::nanoseconds>(tpackingEnd - tpackingStart);
    handle->stats.storageStats.at(storageId).sendtp = std::chrono::system_clock::now();

    size_t dataSize = 0;
    for (auto buffer : package.data) {
      dataSize += buffer.len;
    }
    handle->stats.storageStats.at(storageId).dataSize = dataSize;
  }

  assert(package.data.size() <= 4);
  comm::commInfog(logh::IG::Comm, "comm", handle, "commSendData -> ", " typeId = ", (int)get_id<T>(TM()),
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

/*
 * Process the send operation queue.
 */
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
      checkMPI(MPI_Test(&it->request, &flag, &status));

      if (flag) {
        std::lock_guard<std::mutex> whLock(handle->wh.mutex);
        assert(handle->wh.sendStorage.contains(it->storageId));
        PackageStorage<TM> &storage = handle->wh.sendStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount >= storage.ttlBufferCount) {
          type_map::apply(TM(), storage.typeId, [&]<typename T>() {
            if constexpr (!requires(T * data) { data->pack(); }) {
              // the buffer is dynamically allocated when the data type does not
              // support the 'pack' operation
              delete[] storage.package.data[0].mem;
            }
            cb.template operator()<T>(std::get<std::shared_ptr<T>>(storage.data));
          });
          handle->wh.sendStorage.erase(it->storageId);
        }
        handle->queues.sendOps.erase(it);
      } else {
        it++;
      }
    }
  } while (flush && !handle->queues.sendOps.empty());
}

// Recv ////////////////////////////////////////////////////////////////////////

/*
 * Interface to MPI_Recv.
 */
inline void commRecv(CommHandle *handle, int source, int tag, Buffer const &buf, MPI_Status *status) {
  checkMPI(MPI_Recv(buf.mem, buf.len, MPI_BYTE, source, tag, handle->comm, status));
}

/*
 * Interface to MPI_Irecv.
 */
inline void commRecvAsync(CommHandle *handle, int source, Header header, Buffer const &buf, MPI_Request *request) {
  int tag = headerToTag(header);
  checkMPI(MPI_Irecv(buf.mem, buf.len, MPI_BYTE, source, tag, handle->comm, request));
}

/*
 * Probe the network. When a valid message has arrived, if it contains a
 * signal, then receive the signal, otherwise, add a pending recv data request
 * to the queue.
 */
template <typename TM>
void commRecvSignal(CommTaskHandle<TM> *handle, int &source, CommSignal &signal, Header &header) {
  MPI_Status status;
  int flag = false;

  signal = CommSignal::None;
  source = -1;
  std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
  checkMPI(MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, handle->comm->comm, &flag, &status));

  if (flag) {
    header = tagToHeader(status.MPI_TAG);

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
    comm::commInfog(logh::IG::Comm, "comm", handle, "commRecvSignal -> ", " source = ", status.MPI_SOURCE,
                    " signal = ", (int)signal);
  }
}

/*
 * Try to create a recv storage and create a recv operation on success.
 * `commCreateRecvStorage` fails when the memory manager (`createData`) returns
 * a nullptr (eg the pool is empty). In this case, the pending recv data
 * requests will remain in the queue util memory is available.
 */
template <typename TM, typename CreateDataCB>
bool commRecvData(CommTaskHandle<TM> *handle, CommPendingRecvData const &prd, CreateDataCB createData) {
  auto packageId = prd.header.packageId;
  auto bufferId = prd.header.bufferId;
  auto typeId = prd.header.typeId;
  auto storageId = StorageId{
      .source = (u32)prd.source,
      .packageId = packageId,
  };

  comm::commInfog(logh::IG::Comm, "comm", handle, "commRecvData -> ", " source = ", prd.source,
                  " typeId = ", (int)typeId, " requestId = ", (int)packageId, " bufferId = ", (int)bufferId);

  std::lock_guard<std::mutex> whLock(handle->wh.mutex);
  if (!handle->wh.recvStorage.contains(storageId)) {
    if (!commCreateRecvStorage(handle, storageId, typeId, createData)) {
      commInfog(logh::IG::Comm, "comm", handle, "commCreateRecvStorage returned false");
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
  commRecvAsync(handle->comm, prd.source, prd.header, storage.package.data[bufferId],
                &handle->queues.recvOps.back().request);
  return true;
}

/*
 * Process the pending recv data queue.
 *
 * FIXME: for now, there is no timeout condition, therefore if the memory
 *        manager keeps returning nullptr, the program will never terminate.
 */
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

/*
 * Process the recv operations. This operations are to pending MPI requests
 * that have an associated recv data storage that will store the buffers.
 */
template <typename TM, typename ProcessCB>
void commProcessRecvOpsQueue(CommTaskHandle<TM> *handle, ProcessCB cb, bool flush = false) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
  for (auto it = handle->queues.recvOps.begin(); it != handle->queues.recvOps.end();) {
    int flag = 0;
    MPI_Status status;

    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    checkMPI(MPI_Test(&it->request, &flag, &status));

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
        commInfog(logh::IG::Comm, "comm", handle, "commProcessRecvDataQueue -> unpacking data");
        time_t tunpackingStart, tunpackingEnd;
        type_map::apply(TM(), storage.typeId, [&]<typename T>() {
          auto data = std::get<std::shared_ptr<T>>(storage.data);
          tunpackingStart = std::chrono::system_clock::now();
          commUnpack(std::move(storage.package), data);
          tunpackingEnd = std::chrono::system_clock::now();
          cb.template operator()<T>(data);
        });

        if (handle->comm->collectStats) {
          std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
          handle->stats.storageStats.at(it->storageId).recvtp = std::chrono::system_clock::now();
          handle->stats.storageStats.at(it->storageId).unpackingTime +=
              std::chrono::duration_cast<std::chrono::nanoseconds>(tunpackingEnd - tunpackingStart);
          handle->stats.storageStats.at(it->storageId).unpackingCount += 1;
        }

        handle->wh.recvStorage.erase(it->storageId);
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

/*
 * Send statistics when generating the dot file.
 */
template <typename TM> void commSendStats(CommTaskHandle<TM> *handle) {
  std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
  serializer::Bytes buf;
  Header header = {
      .padding = 0,
      .signal = 0,
      .typeId = 0,
      .channel = handle->channel,
      .packageId = 0,
      .bufferId = 0,
  };

  using Serializer = serializer::Serializer<serializer::Bytes>;
  serializer::serialize<Serializer>(buf, 0, handle->stats.maxSendOpsSize, handle->stats.maxRecvOpsSize,
                                    handle->stats.maxRecvDataQueueSize, handle->stats.maxSendStorageSize,
                                    handle->stats.maxRecvStorageSize, handle->stats.storageStats);
  comm::commInfog(logh::IG::Stats, "stats", handle, "commSendStats -> ", " buf size = ", buf.size());
  commSend(handle->comm, header, 0, Buffer{std::bit_cast<char *>(buf.data()), buf.size()});
}

/*
 * Gather statistics on the master rank.
 */
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
    comm::commInfog(logh::IG::Stats, "stats", handle, "commGatherStats -> ", " target = ", i);
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
