#ifndef COMMUNICATOR_PACKAGE
#define COMMUNICATOR_PACKAGE
#include "protocol.hpp"
#include <cstring>
#include <map>
#include <memory>
#include <set>
#include <variant>
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

// Package Wharehouse //////////////////////////////////////////////////////////

struct StorageId {
  std::uint32_t source;
  std::uint16_t packageId;

  bool operator<(StorageId const &other) const {
    if (this->source == other.source) {
      return this->packageId < other.packageId;
    }
    return this->source < other.source;
  }
};

template <typename TM>
struct PackageWarehouse {
  struct Storage {
    Package            package;
    std::uint64_t      bufferCount;
    std::uint64_t      ttlBufferCount;
    std::uint8_t       typeId;
    variant_type_t<TM> data;
    bool               returnMemory;
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
