#ifndef COMMUNICATOR_PACKAGE
#define COMMUNICATOR_PACKAGE
#include "protocol.hpp"
#include "type_map.hpp"
#include "service/comm_service.hpp"
#include "tool/queue.hpp"
#include "tool/table.hpp"
#include <cstring>
#include <memory>
#include <vector>
#include <initializer_list>
#include <bitset>
#include <hedgehog.h>

/// @brief Hedgehog namespace
namespace hh {
/// @brief Comm namespace
namespace comm {

/******************************************************************************/
/*                                  Package                                   */
/******************************************************************************/

/// @brief Data structure that represent the packages that are transmitted by
///        the communicator tark. A package is a list of buffers, and a buffer
///        is a pointer and an length.
struct Package {
  Buffer buffers[CommService::MAX_PACKAGE_BUFFER_COUNT] = {}; ///< List of buffers.
  size_t bufferCount = 0;                                     ///< Number of buffers.

  /// @brief Default constructor.
  Package() = default;

  /// @brief Variadic constructor.
  /// @param bufferList List of buffers.
  Package(auto &&...bufferList)
      : buffers(std::forward<decltype(bufferList)>(bufferList)...),
        bufferCount(sizeof...(bufferList)) {
    static_assert(sizeof...(bufferList) < CommService::MAX_PACKAGE_BUFFER_COUNT,
            "try to construct package with too many buffers");
  }

  /// @brief Constructor from initializer list.
  /// @param bufferList Initializer list of buffers.
  Package(std::initializer_list<Buffer> bufferList) {
    assert(bufferList.size() < CommService::MAX_PACKAGE_BUFFER_COUNT);
    for (auto buffer : bufferList) {
      this->buffers[this->bufferCount] = buffer;
      this->bufferCount += 1;
    }
  }

  /// @brief Compute the total size if the package (used to determin the
  ///         bandwidth in the profiling information).
  size_t size() const {
    size_t result = 0;
    for (size_t i = 0; i < this->bufferCount; ++i) {
      result += this->buffers[i].size();
    }
    return result;
  }
};

/// @param Utility function that pack the given data.
///
/// - If the type `T` implements the `pack` method, we use it (we use static
///   dispatch to avoid inheritance and virtual functions).
/// - If `T` does not implement `pack`, but is trivially copiable, the data is
///   given directly to the buffer.
/// - Otherwise, an excpetion is thrown.
///
/// @tparam T Type of the data to pack.
/// @param data Data to pack.
/// @throws invalid_argument
/// @return The package containing the data to send.
template <typename T>
Package pack(std::shared_ptr<T> data) {
  Package package;

  if constexpr (requires { data->pack(); }) {
    package = data->pack();
  } else if constexpr (std::is_trivially_copyable_v<T>) {
    package = {Buffer{(char*)data.get(), sizeof(T)}};
  } else {
    throw std::invalid_argument("type " + hh::tool::typeToStr<T>()
                                + " does not implement `pack()` and is not trivially copyable.");
  }
  return package;
}

/// @brief Utility function to get access to the package of a data.
///
/// Reception is done in two steps:
/// 1. We get the package of a data that we use for the receive.
/// 2. We unpack the data (deserialization): since the package system allows
///    sending multiple buffers, this step can be skiped in most cases. Instead
///    of serializing the data into a dedicated buffer, the `pack` and
///    `package` methods of the data can simply return a pointer to the data
///    itself (meta-data), plus one or more pointers (extra data, or GPU memory).
///
/// - If `T` implements the `package` method, this method is used and returns
///   the memory for the package reception.
/// - If `T` is trivially copyable, the data is returned directly.
/// - Otherwise, an exception is thrown.
///
/// @tparam T Type of the data.
/// @param data Data for which we need the package memory.
/// @throws invalid_argument
/// @return The package memory used to receive the data.
template <typename T>
Package packageMem(std::shared_ptr<T> data) {
  if constexpr (requires { data->package(); }) {
    return data->package();
  } else if constexpr (std::is_trivially_copyable_v<T>) {
    return Package{.data = std::vector<Buffer>({Buffer{(char*)data.get(), sizeof(T)}})};
  } else {
    throw std::invalid_argument("type " + hh::tool::typeToStr<T>()
                                + " does not implement `package()` and is not trivially copyable.");
  }
}

/// @brief Utility function use to unpack the data (desirialize if needed).
///
/// - If the data implements the `unpack` method, this method is used.
/// - If the data is trivialy copyable, nothing is done.
/// - Otherwise, an excpetion is thrown.
///
/// @tparam T Type of the data to unpack.
/// @param package Package to unpack.
/// @param data    Data that should unpack the package.
/// @throws invalid_argument
template <typename T>
void unpack(Package &&package, std::shared_ptr<T> data) {
  if constexpr (requires { data->unpack(std::move(package)); }) {
    data->unpack(std::move(package));
  } else if constexpr (std::is_trivially_copyable_v<T>) {
    // there is nothing to do
  } else {
    throw std::invalid_argument("type " + hh::tool::typeToStr<T>()
                                + " does not implement `unpack(Package)` and is not trivially copyable.");
  }
}

/******************************************************************************/
/*                             Package Wharehouse                             */
/******************************************************************************/

/// @brief Type alias for the storage id.
using StorageId = size_t;

/// @brief Warehouse storage slot.
/// @tparam TM Type map type (used in the communicator).
template <typename TM>
struct StorageSlot {
  rank_t             source;         ///< source.
  Package            package;        ///< Package that is beeing sent/received.
  size_t             bufferCount;    ///< Number of buffers sent/received.
  size_t             ttlBufferCount; ///< Total number of buffer sent/received (when sending, it is equal to `nbDests * nbBuffers`)
  variant_type_t<TM> data;           ///< Variant containing the data that is sent (keep the shared_ptr alive).
  type_id_t          typeId;         ///< type of the data stored in the storage.
  bool               useAddResult;   ///< Flag used to know if the data must be transfered or released after all the buffers are sent on the network.
  std::bitset<CommService::MAX_PACKAGE_BUFFER_COUNT> receivedBuffers; ///< Allow to keep track of the received buffers.
  size_t profileId; ///< Profiler entry id.
};

/// @brief Package warehouse type.
/// @tparam TM Type map type (used in the communicator).
template <typename TM>
struct PackageWarehouse {
  Queue<StorageSlot<TM>>        sendStorage;         ///< Send queue.
  Table<Queue<StorageSlot<TM>>> recvStorage;         ///< Recv queue Table(source, type).
  size_t                        recvStorageSize = 0; ///< Recv storage size.

  /// @brief Constructor
  /// @param nbProcesses Number of processes
  /// @param typeCount   Number of types managed by the communicator.
  PackageWarehouse(size_t nbProcesses, size_t typeCount)
      : recvStorage(nbProcesses, typeCount) {}

  /// @brief Add a storage slot in the receive storage.
  /// @tparam T type of the data to store.
  /// @param data   Data to store.
  /// @param source Rank of the sender of the package to which the storage slot
  ///               is dedicated.
  template <typename T>
  StorageId addRecvStorageSlot(std::shared_ptr<T> data, rank_t source, size_t profileId) {
    Package package = packageMem(data);
    size_t bufferCount = package.bufferCount;
    type_id_t typeId = TM::template idOf<T>();

    assert(data != nullptr);
    assert(0 < bufferCount && bufferCount < CommService::MAX_PACKAGE_BUFFER_COUNT);
    this->recvStorageSize += 1;
    return this->recvStorage(source, typeId).add(StorageSlot<TM>{
        .source = source,
        .package = std::move(package),
        .bufferCount = 0,
        .ttlBufferCount = bufferCount,
        .data = std::move(data),
        .typeId = typeId,
        .useAddResult = false,
        .receivedBuffers = {},
        .profileId = profileId,
    });
  }

  /// @brief Remove a storage slot from the receive storage.
  /// @param source    Source rank of the storage line.
  /// @param typeId    Type id of the storage line.
  /// @param storageId Id of the slot to remove in the storage line.
  void removeRecvStorageSlot(rank_t source, type_id_t typeId, StorageId storageId) {
    recvStorageSize -= 1;
    this->recvStorage(source, typeId).remove(storageId);
  }
};

} // end namespace comm

} // end namespace hh

#endif
