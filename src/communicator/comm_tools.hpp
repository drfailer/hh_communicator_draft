#ifndef COMMUNICATOR_COMM_TOOLS
#define COMMUNICATOR_COMM_TOOLS
#include "serializer/serialize.hpp"
#include "type_map.hpp"
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <map>
#include <mpi.h>
#include <serializer/serializer.hpp>
#include <thread>
#include <variant>

#define HH_COMM_DEBUG
#ifdef HH_COMM_DEBUG
#define hh_comm_log(...) fprintf(stderr, "[HH COMM] " __VA_ARGS__)
#else
#define hh_comm_log(...)
#endif

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

template <typename TM> using variant_type_t = variant_type<TM>::type;

// Comm ////////////////////////////////////////////////////////////////////////

struct CommHandle {
  int rank;
  int nbProcesses;
  unsigned char idGenerator;
  MPI_Comm comm;
  std::mutex mpiMutex;
};

inline CommHandle *commCreate() {
  CommHandle *handle;
  handle = new CommHandle();
  handle->rank = -1;
  handle->nbProcesses = -1;
  handle->idGenerator = 0;
  handle->comm = MPI_COMM_WORLD;
  MPI_Comm_rank(handle->comm, &handle->rank);
  MPI_Comm_size(handle->comm, &handle->nbProcesses);
  return handle;
}

inline void commDestroy(CommHandle *handle) { delete handle; }

// transmission data types /////////////////////////////////////////////////////

using u64 = size_t;
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
static_assert(sizeof(Header) <= sizeof(int));

struct CommOperation {
  u16 packageId;
  u8 bufferId;
  MPI_Request request;
};

struct CommQueues {
  std::vector<CommOperation> sendOps;
  std::vector<CommOperation> recvOps;
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
  std::map<u16, PackageStorage<TM>> sendStorage;
  std::map<u16, PackageStorage<TM>> recvStorage;
  std::mutex mutex;
};

enum class CommSignal : unsigned char {
  None,
  Data,
  Disconnect,
};

// CommTaskHandle //////////////////////////////////////////////////////////////

template <typename TM> struct CommTaskHandle {
  CommHandle *comm;
  unsigned char channel;
  std::vector<int> receivers;
  CommQueues queues;
  PackageWarehouse<TM> wh;
};

template <typename TM> CommTaskHandle<TM> commTaskHandleCreate(CommHandle *handle, std::vector<int> receivers) {
  return CommTaskHandle<TM>{
      .comm = handle,
      .channel = ++handle->idGenerator,
      .receivers = receivers,
      .queues = {},
      .wh = {},
  };
}

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
                                std::map<u16, PackageStorage<TM>> &wh) {
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

// Send ////////////////////////////////////////////////////////////////////////

inline void commSend(CommHandle *handle, Header const &header, int dest, Buffer const &buf) {
  MPI_Send(buf.mem, buf.len, MPI_BYTE, dest, std::bit_cast<int>(header), handle->comm);
}

template <typename RT>
inline void commSendAsync(CommHandle *handle, Header const &header, int dest, Buffer const &buf, RT request) {
  MPI_Isend(buf.mem, buf.len, MPI_BYTE, dest, std::bit_cast<int>(header), handle->comm, request);
}

template <typename TM>
inline void commSendSignal(CommTaskHandle<TM> *handle, std::vector<int> dests, CommSignal signal) {
  Header header = {.channel = handle->channel, .signal = 1, .typeId = 0, .packageId = 0, .bufferId = 0};

  hh_comm_log("commSendSignal: channel = %d, signal = %d\n", handle->channel, (int)signal);
  for (auto dest : dests) {
    char buf[1] = {(char)signal};
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    commSend(handle->comm, header, dest, Buffer{buf, 1});
  }
}

template <typename T, typename TM>
inline void commSendData(CommTaskHandle<TM> *handle, std::vector<int> dests, std::shared_ptr<T> data) {
  u16 packageId = commGeneratePackageId();
  Header header = {
      .channel = handle->channel,
      .signal = 0,
      .typeId = type_map::get_id<T>(TM()),
      .packageId = packageId,
      .bufferId = 0,
  };
  Package package = commPack(data);
  PackageStorage<TM> storage = {
      .package = package,
      .bufferCount = 0,
      .ttlBufferCount = package.data.size() * dests.size(),
      .typeId = header.typeId,
      .data = data,
  };

  assert(package.data.size() <= 4);
  hh_comm_log("commSendData: channel = %d, typeId = %d, requestId = %d\n", handle->channel, get_id<T>(TM()),
              (int)header.packageId);

  handle->wh.mutex.lock();
  handle->wh.sendStorage.insert({packageId, storage});
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
      });
      std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
      commSendAsync(handle->comm, header, dest, package.data[i], &handle->queues.sendOps.back().request);
    }
  }
}

template <typename TM, typename ReturnDataCB>
void commProcessSendQueue(CommTaskHandle<TM> *handle, ReturnDataCB cb, bool flush = false) {
  do {
    std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);

    for (auto it = handle->queues.sendOps.begin(); it != handle->queues.sendOps.end();) {
      int flag = 0;
      MPI_Status status;
      std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
      MPI_Test(&it->request, &flag, &status);

      if (flag) {
        std::lock_guard<std::mutex> whLock(handle->wh.mutex);
        assert(handle->wh.sendStorage.contains(it->packageId));
        PackageStorage<TM> &storage = handle->wh.sendStorage.at(it->packageId);
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
          handle->wh.sendStorage.erase(it->packageId);
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
inline void commRecvSignal(CommTaskHandle<TM> *handle, int &source, CommSignal &signal, Header &header) {
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
      signal = CommSignal::Data;
    } else {
      char buf[1];
      // TODO: do we want async here?
      commRecv(handle->comm, status.MPI_SOURCE, status.MPI_TAG, Buffer{buf, 1}, &status);
      signal = (CommSignal)buf[0];
    }
    source = status.MPI_SOURCE;
    hh_comm_log("commRecvSignal: source = %d, channel = %d, signal = %d\n", status.MPI_SOURCE, handle->channel,
                (int)signal);
  }
}

template <typename TM, typename CreateDataCB>
inline void commRecvData(CommTaskHandle<TM> *handle, int source, Header const &header, CreateDataCB createData) {
  auto packageId = header.packageId;
  auto bufferId = header.bufferId;
  auto typeId = header.typeId;

  hh_comm_log("commRecvData: channel = %d, typeId = %d, requestId = %d, bufferId = %d\n", handle->channel, (int)typeId,
              (int)packageId, (int)bufferId);

  type_map::apply(TM(), typeId, [&]<typename T>() {
    if (!handle->wh.recvStorage.contains(packageId)) {
      std::lock_guard<std::mutex> whLock(handle->wh.mutex);
      // TODO: the current memory manager implementation does not support multiple
      //       data types -> the type map might be usefull for this
      auto data = createData.template operator()<T>();
      auto package = commPackage(data);
      PackageStorage<TM> storage{
          .package = package,
          .bufferCount = 0,
          .ttlBufferCount = package.data.size(),
          .typeId = typeId,
          .data = data,
      };
      handle->wh.recvStorage.insert({packageId, storage});
    }
    auto &storage = handle->wh.recvStorage.at(packageId);
    std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
    handle->queues.recvOps.push_back(CommOperation{
        .packageId = packageId,
        .bufferId = bufferId,
        .request = {},
    });
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    commRecvAsync(handle->comm, source, std::bit_cast<int>(header), storage.package.data[bufferId],
                  &handle->queues.recvOps.back().request);
  });
}

template <typename TM, typename ProcessCB>
void commProcessRecvDataQueue(CommTaskHandle<TM> *handle, ProcessCB cb, bool flush = false) {
  std::lock_guard<std::mutex> queuesLock(handle->queues.mutex);
  for (auto it = handle->queues.recvOps.begin(); it != handle->queues.recvOps.end();) {
    int flag = 0;
    MPI_Status status;
    std::lock_guard<std::mutex> mpiLock(handle->comm->mpiMutex);
    MPI_Test(&it->request, &flag, &status);

    if (flag) {
      assert(handle->wh.recvStorage.contains(it->packageId));
      auto &storage = handle->wh.recvStorage.at(it->packageId);
      ++storage.bufferCount;

      if (storage.bufferCount >= storage.ttlBufferCount) {
        hh_comm_log("commProcessRecvDataQueue: unpacking data\n");
        type_map::apply(TM(), storage.typeId, [&]<typename T>() {
          auto data = std::get<std::shared_ptr<T>>(storage.data);
          commUnpack(storage.package, data);
          cb.template operator()<T>(data);
        });
        std::lock_guard<std::mutex> whLock(handle->wh.mutex);
        handle->wh.recvStorage.erase(it->packageId);
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

inline void commBarrier() {
  MPI_Barrier(MPI_COMM_WORLD);
}

inline void commInit(int argc, char **argv) {
  int32_t provided = 0;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
}

inline void commFinalize() { MPI_Finalize(); }

} // end namespace comm

} // end namespace hh

#endif
