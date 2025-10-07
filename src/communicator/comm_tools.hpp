#ifndef COMMUNICATOR_COMM_TOOLS
#define COMMUNICATOR_COMM_TOOLS
#include "../../../clh/clh/buffer.h"
#include "../../../clh/clh/clh.h"
#include "../log.hpp"
#include "hedgehog/src/tools/meta_functions.h"
#include "type_map.hpp"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <map>
#include <serializer/serializer.hpp>
#include <set>
#include <stdexcept>
#include <thread>
#include <type_traits>
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
 * - Header (tag):     [source(32b), channel(8b), signal(1b), typeId(6b), packageId(14b), bufferId(2b)]
 * - Content (buffer): [Signal(8bits)|Data(?)]
 *
 * Header components:
 * - channel: identifier of the CommunicatorTask ------------------- [limit = MAX_UINT]
 * - channel: identifier of the CommunicatorTask ------------------- [limit = 256]
 * - signal: boolean (signal or data?) ----------------------------- [NA]
 * - typeId: identifier of the transimitted type (cf type map) ----- [limit = 64]
 * - packageId: identifier of the package (cf commGeneratePackageId) [limit = 16384]
 * - bufferId: buffer index in the package ------------------------- [limit = 4]
 */

namespace hh {

namespace comm {

// Type map ////////////////////////////////////////////////////////////////////

template <typename... Types>
using TypeTable = type_map::TypeMap<unsigned char, Types...>;

template <typename TM>
struct variant_type;

template <typename... Types>
struct variant_type<TypeTable<Types...>> {
  using type = std::variant<std::shared_ptr<Types>...>;
};

template <typename TM>
using variant_type_t = typename variant_type<TM>::type;

// Comm ////////////////////////////////////////////////////////////////////////

struct CommHandle {
  int           rank;
  int           nbProcesses;
  unsigned char idGenerator;
  bool          collectStats;
  CLH_Handle    clh;
};

inline CommHandle *commCreate(bool collectStats = false) {
  CommHandle *handle;
  handle = new CommHandle();
  handle->rank = -1;
  handle->nbProcesses = -1;
  handle->idGenerator = 0;
  handle->collectStats = collectStats;
  handle->clh = nullptr;
  return handle;
}

inline void commDestroy(CommHandle *handle) {
  delete handle;
}

// transmission data types /////////////////////////////////////////////////////

using u64 = size_t;
using u32 = unsigned int;
using u16 = unsigned short;
using u8 = unsigned char;

using Buffer = CLH_Buffer;

struct Package {
  std::vector<Buffer> data;
};

inline size_t commPackageSize(Package const &package) {
  size_t dataSize = 0;
  for (auto buffer : package.data) {
    dataSize += buffer.len;
  }
  return dataSize;
}

struct Header {
  u32 source : 32;
  u8  signal : 2; // 0 -> data | 1 -> signal
  u8  typeId : 6; // 64 types (number of types managed by one task)
  u8  channel : 8; // 256 channels (number of CommTasks)
  u16 packageId : 14; // 16384 packages
  u8  bufferId : 2; // 4 buffers per package
};
static_assert(sizeof(Header) <= sizeof(u64));

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

inline bool operator<(StorageId const &lhs, StorageId const &rhs) {
  if (lhs.source == rhs.source) {
    return lhs.packageId < rhs.packageId;
  }
  return lhs.source < rhs.source;
}

template <typename TM>
struct PackageStorage {
  Package            package;
  u64                bufferCount;
  u64                ttlBufferCount;
  u8                 typeId;
  variant_type_t<TM> data;
  bool               returnMemory;
};

template <typename TM>
struct PackageWarehouse {
  std::map<StorageId, PackageStorage<TM>> sendStorage;
  std::map<StorageId, PackageStorage<TM>> recvStorage;
  std::mutex                              mutex;
};

// Queues //////////////////////////////////////////////////////////////////////

struct CommOperation {
  u16          packageId;
  u8           bufferId;
  CLH_Request *request;
  StorageId    storageId;
};

struct CommPendingRecvData {
  int          source;
  Header       header;
  CLH_Request *request;
};

inline u64  headerToTag(Header header);
inline bool operator<(CommPendingRecvData const &lhs, CommPendingRecvData const &rhs) {
  if (lhs.source == rhs.source) {
    return headerToTag(lhs.header) < headerToTag(rhs.header);
  }
  return lhs.source < rhs.source;
}

struct CommQueues {
  std::vector<CommOperation>    sendOps; // send operations (CLH_Request*)
  std::vector<CommOperation>    recvOps; // recv operations (CLH_Request*)
  std::set<CommPendingRecvData> createDataQueue; // wait for memory manager
  std::mutex                    mutex;
};

// Stats container /////////////////////////////////////////////////////////////

using time_t = std::chrono::time_point<std::chrono::system_clock>;
using delay_t = std::chrono::duration<long int, std::ratio<1, 1000000000>>;

struct StorageInfo {
  time_t  sendtp;
  time_t  recvtp;
  delay_t packingTime;
  delay_t unpackingTime;
  size_t  packingCount;
  size_t  unpackingCount;
  u8      typeId;
  size_t  dataSize;
};

// TODO: we may have to define a limit on the size of storageStats
struct CommTaskStats {
  std::map<StorageId, StorageInfo> storageStats;
  size_t                           maxSendOpsSize;
  size_t                           maxRecvOpsSize;
  size_t                           maxCreateDataQueueSize;
  size_t                           maxSendStorageSize;
  size_t                           maxRecvStorageSize;
  std::mutex                       mutex;
};

// Task Handle /////////////////////////////////////////////////////////////////

template <typename TM>
struct CommTaskHandle {
  CommHandle          *comm;
  unsigned char        channel;
  std::vector<int>     receivers;
  CommQueues           queues;
  PackageWarehouse<TM> wh;
  CommTaskStats        stats;
  std::vector<size_t>  packagesCount;
};

template <typename TM>
CommTaskHandle<TM> *commTaskHandleCreate(CommHandle *handle, std::vector<int> const &receivers) {
  CommTaskHandle<TM> *taskHandle = new CommTaskHandle<TM>();
  taskHandle->comm = handle;
  taskHandle->channel = ++handle->idGenerator;
  taskHandle->receivers = receivers;
  taskHandle->stats.maxSendOpsSize = 0;
  taskHandle->stats.maxRecvOpsSize = 0;
  taskHandle->stats.maxCreateDataQueueSize = 0;
  taskHandle->stats.maxSendStorageSize = 0;
  taskHandle->stats.maxRecvStorageSize = 0;
  taskHandle->packagesCount = std::vector<size_t>(handle->nbProcesses, 0);
  return taskHandle;
}

template <typename TM>
void commTaskHandleDestroy(CommTaskHandle<TM> *taskHandle) {
  delete taskHandle;
}

// Packing functions ///////////////////////////////////////////////////////////

/*
 * If the data type is packable, the use the `pack` function, otherwise,
 * default to serializer.
 */
template <typename T>
Package commPack(std::shared_ptr<T> data) {
  Package package;

  if constexpr (requires { data->pack(); }) {
    package = data->pack();
  } else if constexpr (std::is_trivially_copyable_v<T>) {
    Buffer buf = Buffer{.mem = new char[sizeof(T)], .len = sizeof(T)};
    std::memcpy(buf.mem, data.get(), sizeof(T));
    package.data.push_back(buf);
  } else {
    throw std::invalid_argument("type " + tool::typeToStr<T>()
                                + " does not implement `pack()` and is not trivially copyable.");
  }
  return package;
}

/*
 * If the data type is packable, it should implement a `package` method that
 * returns the package memory. Otherwise, allocate a buffer on the heap.
 */
template <typename T>
Package commPackage(std::shared_ptr<T> data) {
  if constexpr (requires { data->package(); }) {
    return data->package();
  } else if constexpr (std::is_trivially_copyable_v<T>) {
    return Package{.data = std::vector<Buffer>({Buffer{new char[sizeof(T)], sizeof(T)}})};
  } else {
    throw std::invalid_argument("type " + tool::typeToStr<T>()
                                + " does not implement `package()` and is not trivially copyable.");
  }
}

/*
 * If the data type is unpackable, call the `unpack` method, otherwise, default
 * to serializer.
 */
template <typename T>
void commUnpack(Package &&package, std::shared_ptr<T> data) {
  if constexpr (requires { data->unpack(std::move(package)); }) {
    data->unpack(std::move(package));
  } else if constexpr (std::is_trivially_copyable_v<T>) {
    std::memcpy(data.get(), package.data[0].mem, sizeof(T));
    delete[] package.data[0].mem;
  } else {
    throw std::invalid_argument("type " + tool::typeToStr<T>()
                                + " does not implement `unpack(Package)` and is not trivially copyable.");
  }
}

/*
 * Generate a package id on 14 bits using a counter. Each rank will have its
 * own counter that will loop when after the 16384 package is sent.
 */
inline u16 commGeneratePackageId() {
  static u16 curPackageId = 0;
  u16        result = curPackageId;
  curPackageId = (curPackageId + 1) % 16384; // update the id and make sure it stays on 14 bits
  // curPackageId = (curPackageId + 1) & 0b0011111111111111; // update the id and make sure it stays on 14 bits
  return result;
}

// Helper //////////////////////////////////////////////////////////////////////

inline void dbg_print_binary(u64 i) {
  std::string str = "0b0000000000000000000000000000000000000000000000000000000000000000";
  size_t      idx = 0;

  while (idx < 64) {
    str[str.size() - idx - 1] = (i >> idx) % 2 == 0 ? '0' : '1';
    ++idx;
  }
  printf("%ld = %s\n", i, str.c_str());
}

enum HeaderFields {
  SOURCE = 0,
  SIGNAL = 1,
  TYPE_ID = 2,
  CHANNEL = 3,
  PACKAGE_ID = 4,
  BUFFER_ID = 5,
};

struct HeaderFieldInfo {
  size_t offset;
  u64    mask;
};

constexpr HeaderFieldInfo HEADER_FIELDS[]{
    {.offset = 32, .mask = 0b1111111111111111111111111111111100000000000000000000000000000000},
    {.offset = 30, .mask = 0b0000000000000000000000000000000011000000000000000000000000000000},
    {.offset = 24, .mask = 0b0000000000000000000000000000000000111111000000000000000000000000},
    {.offset = 16, .mask = 0b0000000000000000000000000000000000000000111111110000000000000000},
    {.offset = 2, .mask = 0b0000000000000000000000000000000000000000000000001111111111111100},
    {.offset = 0, .mask = 0b0000000000000000000000000000000000000000000000000000000000000011},
};

inline u64 headerToTag(Header header) {
  u64 tag = 0;
  tag |= (u64)header.source << HEADER_FIELDS[SOURCE].offset;
  tag |= (u64)header.signal << HEADER_FIELDS[SIGNAL].offset;
  tag |= (u64)header.typeId << HEADER_FIELDS[TYPE_ID].offset;
  tag |= (u64)header.channel << HEADER_FIELDS[CHANNEL].offset;
  tag |= (u64)header.packageId << HEADER_FIELDS[PACKAGE_ID].offset;
  tag |= (u64)header.bufferId << HEADER_FIELDS[BUFFER_ID].offset;
  return tag;
}

inline Header tagToHeader(u64 tag) {
  Header header;
  header.source = (tag & HEADER_FIELDS[SOURCE].mask) >> HEADER_FIELDS[SOURCE].offset;
  header.signal = (tag & HEADER_FIELDS[SIGNAL].mask) >> HEADER_FIELDS[SIGNAL].offset;
  header.typeId = (tag & HEADER_FIELDS[TYPE_ID].mask) >> HEADER_FIELDS[TYPE_ID].offset;
  header.channel = (tag & HEADER_FIELDS[CHANNEL].mask) >> HEADER_FIELDS[CHANNEL].offset;
  header.packageId = (tag & HEADER_FIELDS[PACKAGE_ID].mask) >> HEADER_FIELDS[PACKAGE_ID].offset;
  header.bufferId = (tag & HEADER_FIELDS[BUFFER_ID].mask) >> HEADER_FIELDS[BUFFER_ID].offset;
  return header;
}

template <typename... Ts>
void commInfog(logh::IG ig, std::string const &name, auto handle, Ts &&...args) {
  if constexpr (sizeof...(Ts)) {
    logh::infog(ig, name, "[", (int)handle->channel, "]: rank = ", handle->comm->rank, ", ", std::forward<Ts>(args)...);
  } else {
    logh::infog(ig, name, "[", (int)handle->channel, "]: rank = ", handle->comm->rank);
  }
}

inline bool checkCLH(CLH_Status code) {
  if (code == CLH_STATUS_SUCCESS) {
    return true;
  }
  logh::error("clh error: ", clh_status_string(code));
  return false;
}

/*
 * Flush operation queue and remove storage entries from the warehouse.
 */
template <typename TM>
void commFlushQueueAndWarehouse(CommTaskHandle<TM> *handle, std::vector<CommOperation> &queue,
                                std::map<StorageId, PackageStorage<TM>> &wh) {
  std::vector<CLH_Request *> requests;

  for (auto &op : queue) {
    logh::error("request canceled");
    clh_cancel(handle->comm->clh, op.request);
    requests.push_back(op.request);
  }
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
    auto               package = commPackage(data);
    PackageStorage<TM> storage{
        .package = package,
        .bufferCount = 0,
        .ttlBufferCount = package.data.size(),
        .typeId = typeId,
        .data = data,
        .returnMemory = true,
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
 * Interface to clh_send
 */
inline void commSend(CommHandle *handle, Header const &header, int dest, Buffer const &buf) {
  u64          tag = headerToTag(header);
  CLH_Request *request = clh_send(handle->clh, dest, tag, buf);
  checkCLH(clh_wait(handle->clh, request));
  clh_request_release(handle->clh, request);
}

/*
 * Interface to clh_send
 */
inline CLH_Request *commSendAsync(CommHandle *handle, Header const &header, int dest, Buffer const &buf) {
  u64          tag = headerToTag(header);
  CLH_Request *request = clh_send(handle->clh, dest, tag, buf);
  // checkCLH(clh_wait(handle->clh, request));
  return request;
}

/*
 * Send a signal to the given destinations.
 */
template <typename TM>
void commSendSignal(CommTaskHandle<TM> *handle, std::vector<int> const &dests, CommSignal signal) {
  Header header = {
      .source = (u32)handle->comm->rank,
      .signal = 1,
      .typeId = 0,
      .channel = handle->channel,
      .packageId = 0,
      .bufferId = 0,
  };
  char buf[100] = {(char)signal};
  u64  len = 1;

  comm::commInfog(logh::IG::Comm, "comm", handle, "signal = ", (int)signal);
  for (auto dest : dests) {
    if (signal == CommSignal::Disconnect) {
      size_t size = sizeof(handle->packagesCount[dest]);
      std::memcpy(&buf[1], &handle->packagesCount[dest], size);
      len = 1 + size;
    }
    commSend(handle->comm, header, dest, Buffer{buf, len});
  }
}

template <typename T, typename TM>
std::pair<StorageId, PackageStorage<TM>> commCreateSendStorage(CommTaskHandle<TM>     *handle,
                                                               std::vector<int> const &dests, std::shared_ptr<T> data,
                                                               bool returnMemory) {
  u16 packageId = commGeneratePackageId();

  // measure data packing time
  time_t  tpackingStart = std::chrono::system_clock::now();
  Package package = commPack(data);
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
      .source = (u32)handle->comm->rank,
      .packageId = packageId,
  };

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.storageStats.insert({storageId, StorageInfo{}});
    handle->stats.storageStats.at(storageId).typeId = storage.typeId;
    handle->stats.storageStats.at(storageId).packingCount += 1;
    handle->stats.storageStats.at(storageId).packingTime
        += std::chrono::duration_cast<std::chrono::nanoseconds>(tpackingEnd - tpackingStart);
    handle->stats.storageStats.at(storageId).sendtp = std::chrono::system_clock::now();
    handle->stats.storageStats.at(storageId).dataSize = commPackageSize(package);
  }
  return {storageId, storage};
}

/*
 * Send data to the given destinations.
 */
template <typename T, typename TM>
void commSendData(CommTaskHandle<TM> *handle, std::vector<int> const &dests, std::shared_ptr<T> data,
                  bool returnMemory = true) {
  auto [storageId, storage] = commCreateSendStorage(handle, dests, data, returnMemory);
  Header header = {
      .source = (u32)handle->comm->rank,
      .signal = 0,
      .typeId = storage.typeId,
      .channel = handle->channel,
      .packageId = storageId.packageId,
      .bufferId = 0,
  };

  comm::commInfog(logh::IG::Comm, "comm", handle, "commSendData -> ", " typeId = ", (int)get_id<T>(TM()),
                  " requestId = ", (int)header.packageId);

  handle->wh.mutex.lock();
  handle->wh.sendStorage.insert({storageId, storage});
  handle->wh.mutex.unlock();

  for (auto dest : dests) {
    ++handle->packagesCount[dest];
    for (size_t i = 0; i < storage.package.data.size(); ++i) {
      header.bufferId = (u8)i;
      std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
      CLH_Request                *request = commSendAsync(handle->comm, header, dest, storage.package.data[i]);
      handle->queues.sendOps.push_back(CommOperation{
          .packageId = storageId.packageId,
          .bufferId = header.bufferId,
          .request = request,
          .storageId = storageId,
      });
    }
  }
}

/*
 * Manages the data after send.
 */
template <typename TM, typename ReturnDataCB>
void commPostSend(CommTaskHandle<TM> *, StorageId, PackageStorage<TM> storage, ReturnDataCB cb) {
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
template <typename TM, typename ReturnDataCB>
void commProcessSendOpsQueue(CommTaskHandle<TM> *handle, ReturnDataCB returnMemory, bool flush = false) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.maxSendOpsSize = std::max(handle->stats.maxSendOpsSize, handle->queues.sendOps.size());
    handle->stats.maxSendStorageSize = std::max(handle->stats.maxSendStorageSize, handle->wh.sendStorage.size());
  }

  do {
    // clh_progress_all(handle->comm->clh); // ???
    for (auto it = handle->queues.sendOps.begin(); it != handle->queues.sendOps.end();) {
      if (clh_request_completed(handle->comm->clh, it->request)) {
        std::lock_guard<std::mutex> whLock(handle->wh.mutex);
        assert(handle->wh.sendStorage.contains(it->storageId));
        PackageStorage<TM> &storage = handle->wh.sendStorage.at(it->storageId);
        ++storage.bufferCount;

        if (storage.bufferCount >= storage.ttlBufferCount) {
          commPostSend(handle, it->storageId, storage, returnMemory);
          handle->wh.sendStorage.erase(it->storageId);
        }
        clh_request_release(handle->comm->clh, it->request);
        it = handle->queues.sendOps.erase(it);
      } else {
        it++;
      }
    }
  } while (flush && !handle->queues.sendOps.empty());
}

// Recv ////////////////////////////////////////////////////////////////////////

/*
 * Interface to clh_recv.
 */
inline void commRecv(CommHandle *handle, CLH_Request *probe_request, Buffer const &buf) {
  CLH_Request *request = clh_request_recv(handle->clh, probe_request, buf);
  checkCLH(clh_wait(handle->clh, request));
  clh_request_release(handle->clh, request);
}

/*
 * Interface to clh_recv.
 */
inline CLH_Request *commRecvAsync(CommHandle *handle, CLH_Request *probe_request, Buffer const &buf) {
  CLH_Request *request = clh_request_recv(handle->clh, probe_request, buf);
  // checkCLH(clh_wait(handle->clh, request));
  return request;
}

template <typename TM>
inline CLH_Request *commProbe(CommTaskHandle<TM> *handle) {
  Header header = {
      .source = 0,
      .signal = 0,
      .typeId = 0,
      .channel = handle->channel,
      .packageId = 0,
      .bufferId = 0,
  };
  u64 tag = headerToTag(header), tagMask = HEADER_FIELDS[CHANNEL].mask;
  return clh_probe(handle->comm->clh, tag, tagMask, true);
}

/*
 * Probe the network. When a valid message has arrived, if it contains a
 * signal, then receive the signal, otherwise, add a pending recv data request
 * to the queue.
 */
template <typename TM>
void commRecvSignal(CommTaskHandle<TM> *handle, int &source, CommSignal &signal, Header &header, Buffer &buf) {
  u64          tag = 0;
  CLH_Request *request = commProbe(handle);

  signal = CommSignal::None;
  source = -1;

  // TODO: probe for my rank / my channel
  if (request->data.probe.result) {
    tag = request->data.probe.sender_tag;
    header = tagToHeader(tag);

    assert(header.source != (u32)clh_node_id(handle->comm->clh));

    if (header.signal == 0) {
      std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
      handle->queues.createDataQueue.insert(CommPendingRecvData{
          .source = (int)header.source,
          .header = header,
          .request = request,
      });
      signal = CommSignal::Data;
    } else {
      // TODO: do we want async here?
      commRecv(handle->comm, request, buf);
      signal = (CommSignal)buf.mem[0];
    }
    source = (int)header.source;
    comm::commInfog(logh::IG::Comm, "comm", handle, "commRecvSignal -> ", " source = ", source,
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
  auto *request = commRecvAsync(handle->comm, prd.request, storage.package.data[bufferId]);
  handle->queues.recvOps.push_back(CommOperation{
      .packageId = packageId,
      .bufferId = bufferId,
      .request = request,
      .storageId = storageId,
  });
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
    handle->stats.maxCreateDataQueueSize
        = std::max(handle->stats.maxCreateDataQueueSize, handle->queues.createDataQueue.size());
  }

  for (auto it = handle->queues.createDataQueue.begin(); it != handle->queues.createDataQueue.end();) {
    if (commRecvData(handle, *it, createData)) {
      it = handle->queues.createDataQueue.erase(it);
    } else {
      it++;
    }
  }
}

/*
 * Process data after recv.
 */
template <typename TM, typename ProcessCB>
void commPostRecv(CommTaskHandle<TM> *handle, StorageId storageId, PackageStorage<TM> storage, ProcessCB processData) {
  commInfog(logh::IG::Comm, "comm", handle, "commProcessRecvDataQueue -> unpacking data");
  time_t tunpackingStart, tunpackingEnd;
  type_map::apply(TM(), storage.typeId, [&]<typename T>() {
    auto data = std::get<std::shared_ptr<T>>(storage.data);
    tunpackingStart = std::chrono::system_clock::now();
    commUnpack(std::move(storage.package), data);
    tunpackingEnd = std::chrono::system_clock::now();
    processData.template operator()<T>(data);
  });

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.storageStats.at(storageId).recvtp = std::chrono::system_clock::now();
    handle->stats.storageStats.at(storageId).unpackingTime
        += std::chrono::duration_cast<std::chrono::nanoseconds>(tunpackingEnd - tunpackingStart);
    handle->stats.storageStats.at(storageId).unpackingCount += 1;
  }
}

/*
 * Process the recv operations. This operations are to pending MPI requests
 * that have an associated recv data storage that will store the buffers.
 */
template <typename TM, typename ProcessCB>
void commProcessRecvOpsQueue(CommTaskHandle<TM> *handle, ProcessCB processData, [[maybe_unused]] bool flush = false) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);

  if (handle->comm->collectStats) {
    std::lock_guard<std::mutex> statsLock(handle->stats.mutex);
    handle->stats.maxRecvOpsSize = std::max(handle->stats.maxRecvOpsSize, handle->queues.recvOps.size());
    handle->stats.maxRecvStorageSize = std::max(handle->stats.maxRecvStorageSize, handle->wh.recvStorage.size());
  }

  // clh_progress_all(handle->comm->clh); // ???
  for (auto it = handle->queues.recvOps.begin(); it != handle->queues.recvOps.end();) {
    if (clh_request_completed(handle->comm->clh, it->request)) {
      std::lock_guard<std::mutex> whLock(handle->wh.mutex);
      assert(handle->wh.recvStorage.contains(it->storageId));
      auto &storage = handle->wh.recvStorage.at(it->storageId);
      ++storage.bufferCount;

      if (storage.bufferCount >= storage.ttlBufferCount) {
        commPostRecv(handle, it->storageId, storage, processData);
        handle->wh.recvStorage.erase(it->storageId);
      }
      clh_request_release(handle->comm->clh, it->request);
      it = handle->queues.recvOps.erase(it);
    } else {
      it++;
    }
  }

  // if (flush) {
  //   commFlushQueueAndWarehouse(handle, handle->queues.recvOps, handle->wh.recvStorage);
  // }
}

// Other functions /////////////////////////////////////////////////////////////

inline void commBarrier(CommHandle *handle) {
  clh_barrier(handle->clh);
}

inline void commInit(CommHandle *handle, int *, char ***) {
  clh_init(&handle->clh);
  handle->rank = clh_node_id(handle->clh);
  handle->nbProcesses = clh_nb_nodes(handle->clh);
}

inline void commFinalize(CommHandle *handle) {
  clh_finalize(handle->clh);
}

// stats exchange //////////////////////////////////////////////////////////////

/*
 * Send statistics when generating the dot file.
 */
template <typename TM>
void commSendStats(CommTaskHandle<TM> *handle) {
  serializer::Bytes buf;
  Header            header = {
                 .source = (u32)handle->comm->rank,
                 .signal = 0,
                 .typeId = 0,
                 .channel = handle->channel,
                 .packageId = 0,
                 .bufferId = 0,
  };

  using Serializer = serializer::Serializer<serializer::Bytes>;
  serializer::serialize<Serializer>(buf, 0, handle->stats.maxSendOpsSize, handle->stats.maxRecvOpsSize,
                                    handle->stats.maxCreateDataQueueSize, handle->stats.maxSendStorageSize,
                                    handle->stats.maxRecvStorageSize, handle->stats.storageStats);
  comm::commInfog(logh::IG::Stats, "stats", handle, "commSendStats -> ", " buf size = ", buf.size(),
                  ", storageStats size = ", handle->stats.storageStats.size());
  commSend(handle->comm, header, 0, Buffer{std::bit_cast<char *>(buf.data()), buf.size()});
  // clh_progress_all(handle->comm->clh);
}

/*
 * Gather statistics on the master rank.
 */
template <typename TM>
std::vector<CommTaskStats> commGatherStats(CommTaskHandle<TM> *handle) {
  std::vector<CommTaskStats> stats(handle->comm->nbProcesses);
  int                        bufSize;
  Header                     header = {
                          .source = 0,
                          .signal = 0,
                          .typeId = 0,
                          .channel = handle->channel,
                          .packageId = 0,
                          .bufferId = 0,
  };

  stats[0].storageStats = std::move(handle->stats.storageStats);
  stats[0].maxSendOpsSize = handle->stats.maxSendOpsSize;
  stats[0].maxRecvOpsSize = handle->stats.maxRecvOpsSize;
  stats[0].maxCreateDataQueueSize = handle->stats.maxCreateDataQueueSize;
  stats[0].maxSendStorageSize = handle->stats.maxSendStorageSize;
  stats[0].maxRecvStorageSize = handle->stats.maxRecvStorageSize;
  for (int i = 1; i < handle->comm->nbProcesses; ++i) {
    header.source = i;
    u64          tag = headerToTag(header);
    u64          tagMask = HEADER_FIELDS[SOURCE].mask | HEADER_FIELDS[CHANNEL].mask;
    CLH_Request *request = clh_probe(handle->comm->clh, tag, tagMask, true);
    while (!request->data.probe.result) {
      clh_request_release(handle->comm->clh, request);
      using namespace std::chrono_literals;
      std::this_thread::sleep_for(1ms);
      request = clh_probe(handle->comm->clh, tag, tagMask, true);
    }
    bufSize = clh_request_buffer_len(request);

    serializer::Bytes buf(bufSize, bufSize);
    commRecv(handle->comm, request, Buffer{std::bit_cast<char *>(buf.data()), buf.size()});

    using Serializer = serializer::Serializer<serializer::Bytes>;
    serializer::deserialize<Serializer>(buf, 0, stats[i].maxSendOpsSize, stats[i].maxRecvOpsSize,
                                        stats[i].maxCreateDataQueueSize, stats[i].maxSendStorageSize,
                                        stats[i].maxRecvStorageSize, stats[i].storageStats);
    comm::commInfog(logh::IG::Stats, "stats", handle, "comGather -> ", "target = ", i, " buf size = ", buf.size(),
                    ", storageStats size = ", stats[i].storageStats.size());
  }
  return stats;
}

} // end namespace comm

} // end namespace hh

#endif
