#ifndef COMMUNICATOR_PROTOCOL
#define COMMUNICATOR_PROTOCOL
#include <vector>
#include <memory>
#include <cstring>
#include "hedgehog/src/tools/meta_functions.h"
#include "type_map.hpp"
#include <variant>
#include <map>
#include <set>

namespace hh {

namespace comm {

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

using Buffer = CLH_Buffer;
using Request = CLH_Request *;


// Header //////////////////////////////////////////////////////////////////////

struct Header {
  std::uint32_t source : 32;
  std::uint8_t  signal : 2; // 0 -> data | 1 -> signal
  std::uint8_t  typeId : 6; // 64 types (number of types managed by one task)
  std::uint8_t  channel : 8; // 256 channels (number of CommTasks)
  std::uint16_t packageId : 14; // 16384 packages
  std::uint8_t  bufferId : 2; // 4 buffers per package
};
static_assert(sizeof(Header) <= sizeof(std::uint64_t));

enum HeaderFields {
  SOURCE = 0,
  SIGNAL = 1,
  TYPE_ID = 2,
  CHANNEL = 3,
  PACKAGE_ID = 4,
  BUFFER_ID = 5,
};

struct HeaderFieldInfo {
  size_t        offset;
  std::uint64_t mask;
};

constexpr HeaderFieldInfo HEADER_FIELDS[]{
    {.offset = 32, .mask = 0b1111111111111111111111111111111100000000000000000000000000000000},
    {.offset = 30, .mask = 0b0000000000000000000000000000000011000000000000000000000000000000},
    {.offset = 24, .mask = 0b0000000000000000000000000000000000111111000000000000000000000000},
    {.offset = 16, .mask = 0b0000000000000000000000000000000000000000111111110000000000000000},
    {.offset = 2, .mask = 0b0000000000000000000000000000000000000000000000001111111111111100},
    {.offset = 0, .mask = 0b0000000000000000000000000000000000000000000000000000000000000011},
};

inline std::uint64_t headerToTag(Header header) {
  std::uint64_t tag = 0;
  tag |= (std::uint64_t)header.source << HEADER_FIELDS[SOURCE].offset;
  tag |= (std::uint64_t)header.signal << HEADER_FIELDS[SIGNAL].offset;
  tag |= (std::uint64_t)header.typeId << HEADER_FIELDS[TYPE_ID].offset;
  tag |= (std::uint64_t)header.channel << HEADER_FIELDS[CHANNEL].offset;
  tag |= (std::uint64_t)header.packageId << HEADER_FIELDS[PACKAGE_ID].offset;
  tag |= (std::uint64_t)header.bufferId << HEADER_FIELDS[BUFFER_ID].offset;
  return tag;
}

inline Header tagToHeader(std::uint64_t tag) {
  Header header;
  header.source = (tag & HEADER_FIELDS[SOURCE].mask) >> HEADER_FIELDS[SOURCE].offset;
  header.signal = (tag & HEADER_FIELDS[SIGNAL].mask) >> HEADER_FIELDS[SIGNAL].offset;
  header.typeId = (tag & HEADER_FIELDS[TYPE_ID].mask) >> HEADER_FIELDS[TYPE_ID].offset;
  header.channel = (tag & HEADER_FIELDS[CHANNEL].mask) >> HEADER_FIELDS[CHANNEL].offset;
  header.packageId = (tag & HEADER_FIELDS[PACKAGE_ID].mask) >> HEADER_FIELDS[PACKAGE_ID].offset;
  header.bufferId = (tag & HEADER_FIELDS[BUFFER_ID].mask) >> HEADER_FIELDS[BUFFER_ID].offset;
  return header;
}

// Signal //////////////////////////////////////////////////////////////////////

enum class Signal : std::uint8_t {
  None,
  Data,
  Disconnect,
};

// Package /////////////////////////////////////////////////////////////////////

struct Package {
  std::vector<Buffer> data;

  size_t size() const {
    size_t result = 0;
    for (auto buffer : this->data) {
      result += buffer.len;
    }
    return result;
  }
};

template <typename T>
Package pack(std::shared_ptr<T> data) {
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
 * TODO: rename packageMem
 */
template <typename T>
Package packageMem(std::shared_ptr<T> data) {
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
void unpack(Package &&package, std::shared_ptr<T> data) {
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
inline std::uint16_t generatePackageId() {
  static std::uint16_t curPackageId = 0;
  std::uint16_t        result = curPackageId;
  curPackageId = (curPackageId + 1) % 16384; // update the id and make sure it stays on 14 bits
  return result;
}

// TODO: should not be here???
// Warehouse ///////////////////////////////////////////////////////////////////

struct StorageId {
  std::uint32_t source;
  std::uint16_t packageId;
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
  std::uint64_t      bufferCount;
  std::uint64_t      ttlBufferCount;
  std::uint8_t       typeId;
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
  std::uint16_t          packageId;
  std::uint8_t           bufferId;
  CLH_Request *request;
  StorageId    storageId;
};

struct CommPendingRecvData {
  int          source;
  Header       header;
  CLH_Request *request;
};

inline std::uint64_t  headerToTag(Header header);
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
  std::uint8_t      typeId;
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

} // end namespace comm

} // end namespace hh

#endif
