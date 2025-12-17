#ifndef COMMUNICATOR_PACKAGE
#define COMMUNICATOR_PACKAGE
#include "protocol.hpp"
#include "type_map.hpp"
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <vector>

namespace hh {

namespace comm {

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

// Package Wharehouse //////////////////////////////////////////////////////////

struct StorageId {
  std::uint64_t source;
  std::uint64_t typeId;
  std::uint64_t packageId;
  std::uint64_t cid;

  StorageId() = default;

  StorageId(std::uint64_t source, std::uint64_t packageId, std::uint64_t typeId, std::uint64_t cid)
      : source(source),
        typeId(typeId),
        packageId(packageId),
        cid(cid) {}

  StorageId(std::uint64_t source, std::uint64_t packageId, std::uint64_t typeId)
      : StorageId(source, packageId, typeId, counter_++) {}

  bool operator<(StorageId const &other) const {
    if (this->source == other.source) {
      if (this->typeId == other.typeId) {
        if (this->packageId == other.packageId) {
          return this->cid < other.cid;
        }
        return this->packageId < other.packageId;
      }
      return this->typeId < other.typeId;
    }
    return this->source < other.source;
  }

private:
  static inline std::uint64_t counter_ = 0;
};

template <typename TM>
struct PackageWarehouse {
  struct Storage {
    Package            package;
    std::uint64_t      bufferCount;
    std::uint64_t      ttlBufferCount;
    variant_type_t<TM> data;
    bool               returnMemory;
    bool               dbgBufferReceived[4]; // TODO: remove
  };

  std::map<StorageId, Storage> sendStorage;
  std::map<StorageId, Storage> recvStorage;
  std::mutex                   mutex;
};

template <typename TM>
using PackageStorage = typename PackageWarehouse<TM>::Storage;

} // end namespace comm

} // end namespace hh

#endif
